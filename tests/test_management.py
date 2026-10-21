from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import multiprocessing

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.recurring import RecurringStore
from app.store import TaskStore
from app.worker import worker_iteration
from app.history_relay import deliver_history
import app.history as history
import app.history_relay as relay


def test_listing_filters_pagination_and_existing_jobs(client, store):
    first = store.create_task('add', args=[1, 2], priority='high')
    store.create_task('fibonacci', args=[5], run_at=datetime.now(timezone.utc)+timedelta(minutes=5))
    assert client.get('/tasks?limit=1').json()['total'] == 2
    assert len(client.get('/tasks?limit=1').json()['items']) == 1
    assert len(client.get('/tasks?offset=1&limit=1').json()['items']) == 1
    assert client.get('/tasks?status=scheduled').json()['items'][0]['task_type'] == 'fibonacci'
    assert client.get('/tasks?task_type=add').json()['total'] == 1
    assert client.get('/tasks?search='+first['task_id'][:8]).json()['items'][0]['task_id'] == first['task_id']
    assert client.get('/tasks?limit=0').status_code == 422


def test_idempotency_concurrent_and_conflicting(store, client):
    def submit(_):
        return store.create_task('add', args=[1, 2], idempotency_key='same')['task_id']
    with ThreadPoolExecutor(max_workers=8) as pool:
        ids = list(pool.map(submit, range(16)))
    assert len(set(ids)) == 1
    assert store.runnable_queue_lengths()['medium'] == 1
    assert store.redis.xlen(store.outbox_key) == 1
    response = client.post('/tasks', headers={'Idempotency-Key':'same'}, json={'task_type':'add','args':[3,4]})
    assert response.status_code == 409
    response = client.post('/tasks', headers={'Idempotency-Key':'api-key'}, json={'task_type':'add','args':[3,4]})
    assert response.status_code == 201
    assert client.post('/tasks', headers={'Idempotency-Key':'api-key'}, json={'task_type':'add','args':[3,4]}).json()['task_id'] == response.json()['task_id']


@pytest.mark.parametrize('scheduled', [False, True])
def test_cancel_removes_queue_entry_and_cannot_be_claimed(client, store, scheduled):
    task = store.create_task('add', run_at=datetime.now(timezone.utc)+timedelta(hours=1) if scheduled else None)
    task_id = task['task_id']
    assert client.post(f'/tasks/{task_id}/cancel').json()['status'] == 'cancelled'
    assert store.claim_task('worker') is None
    assert store.scheduled_count() == 0
    assert store.get_metrics()['status_counts']['cancelled'] == 1
    assert client.post(f'/tasks/{task_id}/cancel').status_code == 409
    assert client.post('/tasks/missing/cancel').status_code == 404


def test_running_job_cannot_be_cancelled_or_retried(client, store):
    task = store.create_task('add')
    store.claim_task('worker')
    assert client.post('/tasks/'+task['task_id']+'/cancel').status_code == 409
    assert client.post('/tasks/'+task['task_id']+'/retry').status_code == 409


def test_retry_resets_budget_preserves_attempts_and_history(client, store):
    task = store.create_task('add', args=[10, 20])
    raw = store.claim_task('worker')
    store.finish_execution(task['task_id'], raw['execution_token'], error_message='broken', failure_type='unknown_task')
    assert store.dead_letter_size() == 1
    response = client.post('/tasks/'+task['task_id']+'/retry')
    assert response.status_code == 200
    assert response.json()['attempt_count'] == 1
    assert response.json()['retry_count'] == 0
    assert store.dead_letter_size() == 0
    assert client.post('/tasks/'+task['task_id']+'/retry').status_code == 409
    worker_iteration(store.redis, store, 'worker', store.runnable_queue_names(), timeout=1)
    assert store.get_task(task['task_id'])['result'] == 30
    assert store.get_task(task['task_id'])['attempt_count'] == 2
    assert any(e['event_type']=='manually_retried' for _,e in store.redis.xrange(store.outbox_key))


def _claim_and_wait(redis_url, queue, dead, scheduled, ready):
    import time
    store = TaskStore(redis_url, queue, dead, scheduled)
    raw = store.claim_task('doomed-worker')
    ready.send(raw)
    time.sleep(60)


def test_killed_worker_recovery_fences_stale_completion(store, test_settings):
    task = store.create_task('add', args=[2, 5])
    ctx = multiprocessing.get_context('spawn')
    parent, child = ctx.Pipe()
    worker = ctx.Process(target=_claim_and_wait, args=(test_settings.redis_url, store.queue_name,
        store.dead_letter_queue_name, store.scheduled_queue_name, child))
    worker.start()
    try:
        assert parent.poll(10), 'Worker failed to claim'
        raw = parent.recv()
        worker.kill()
        worker.join(5)
        assert not worker.is_alive()
        # Advance the lease deadline instead of sleeping through timeout + grace.
        store.redis.zadd(store.processing_key, {task['task_id']:0})
        with ThreadPoolExecutor(max_workers=4) as pool:
            recovered = list(pool.map(lambda _:store.recover_expired_tasks(), range(4)))
        assert sum(recovered) == 1
        assert store.finish_execution(task['task_id'], raw['execution_token'], result='stale') is None
        store.redis.zadd(store.scheduled_queue_name, {task['task_id']:0})
        worker_iteration(store.redis, store, 'replacement', store.runnable_queue_names(), timeout=1)
        final = store.get_task(task['task_id'])
        assert final['status'] == 'completed'
        assert final['result'] == 7
        assert final['attempt_count'] == 2
    finally:
        if worker.is_alive():
            worker.kill()
        worker.join()
        parent.close()
        child.close()


def test_repeated_crashes_exhaust_retry_budget(store):
    task = store.create_task('add')
    for _ in range(4):
        store.promote_due_scheduled_tasks()
        raw = store.claim_task('crashing-worker')
        assert raw
        store.redis.zadd(store.processing_key, {task['task_id']:0})
        assert store.recover_expired_tasks() == 1
        if store.get_task(task['task_id'])['status'] == 'scheduled':
            store.redis.zadd(store.scheduled_queue_name, {task['task_id']:0})
    assert store.get_task(task['task_id'])['status'] == 'dead_lettered'
    assert store.dead_letter_size() == 1


def test_history_outage_retains_events_and_replay_is_idempotent(store, monkeypatch):
    engine = create_engine('sqlite+pysqlite:///:memory:')
    history.Base.metadata.create_all(engine)
    monkeypatch.setattr(history, 'SessionLocal', sessionmaker(engine))
    task = store.create_task('add')
    def fail(*args, **kwargs):
        raise RuntimeError('database down')
    monkeypatch.setattr(relay, 'append_event', fail)
    with pytest.raises(RuntimeError):
        deliver_history(store)
    assert store.redis.xlen(store.outbox_key) == 1
    monkeypatch.setattr(relay, 'append_event', history.append_event)
    event_id, event = store.redis.xrange(store.outbox_key)[0]
    # Simulate a successful SQL commit followed by a relay crash before XDEL.
    history.append_event(event['event_type'], store._deserialize_task(json.loads(event['snapshot'])),
                         event_id=f'{store.queue_name}:{event_id}')
    assert deliver_history(store) == 1
    assert store.redis.xlen(store.outbox_key) == 0
    assert len(history.get_job_history(task['task_id'])) == 1


def test_recurring_concurrent_occurrence_and_pause(store):
    recurring = RecurringStore(store)
    schedule = recurring.create('Every minute', 60, {'task_type':'add','args':[1,2]})
    schedule['next_run'] = datetime.now(timezone.utc).timestamp() - 180
    store.redis.hset(recurring.key, schedule['id'], json.dumps(schedule))
    with ThreadPoolExecutor(max_workers=6) as pool:
        counts = list(pool.map(lambda _: RecurringStore(store).tick(), range(6)))
    assert sum(counts) == 1
    assert store.runnable_queue_lengths()['medium'] == 1
    assert recurring.tick() == 0
    assert recurring.list()[0]['next_run'] > datetime.now(timezone.utc).timestamp()
    recurring.update(schedule['id'], False)
    assert recurring.tick() == 0
    assert not recurring.list()[0]['enabled']
    recurring.update(schedule['id'], True)
    assert recurring.list()[0]['enabled']


def test_schedule_api_validation(client):
    body = {'name':'Demo', 'interval_seconds':60, 'task':{'task_type':'fibonacci','args':[5]}}
    response = client.post('/schedules',json=body)
    assert response.status_code == 201
    schedule_id = response.json()['id']
    assert client.get('/schedules').json()['items'][0]['id'] == schedule_id
    assert client.patch('/schedules/'+schedule_id,json={'enabled':False}).json()['enabled'] is False
    assert client.patch('/schedules/missing',json={'enabled':False}).status_code == 404
    assert client.post('/schedules',json={**body, 'interval_seconds':0}).status_code == 422
    assert client.post('/schedules',json={**body,'task':{'task_type':'missing'}}).status_code == 400

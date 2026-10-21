COMPOSE = docker compose -f docker-compose.yml -f docker-compose.m1.yml

.PHONY: up stop status logs test smoke
up:
	$(COMPOSE) up --build -d

stop:
	$(COMPOSE) stop

status:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100 api worker1 worker2 worker3 history-relay

# The existing suite flushes its Redis database. Refuse to touch a nonempty DB.
test:
	test "$$($(COMPOSE) exec -T redis redis-cli -n 15 DBSIZE)" = "0"
	$(COMPOSE) run --rm --no-deps -e REDIS_DB=15 -e QUEUE_NAME=test_task_queue -e DEAD_LETTER_QUEUE_NAME=test_task_queue_dead_letter -e SCHEDULED_QUEUE_NAME=test_task_queue_scheduled -e DEFAULT_TASK_TIMEOUT=5 -v "$(CURDIR)/tests:/app/tests:ro" api python -m pytest tests -q

smoke:
	$(COMPOSE) run --rm --no-deps -v "$(CURDIR)/scripts:/checks:ro" api python /checks/smoke_local.py --base-url http://api:8000

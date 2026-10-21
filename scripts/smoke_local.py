"""Exercise the running stack; retain demo tasks and a report in shared data."""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import httpx
from PIL import Image


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000")
    args = parser.parse_args()
    output = Path("/data") / ("smoke-" + uuid4().hex[:8])
    output.mkdir(parents=True)
    Image.new("RGB", (32, 24), "navy").save(output / "input.png")
    (output / "words.txt").write_text("hello queue hello", encoding="utf-8")
    results = {}
    with httpx.Client(base_url=args.base_url, timeout=10) as client:
        for endpoint in ("/docs", "/redoc", "/openapi.json", "/dashboard", "/metrics"):
            response = client.get(endpoint)
            response.raise_for_status()
        assert client.get("/tasks/does-not-exist").status_code == 404
        assert client.post("/tasks", json={"task_type": "missing"}).status_code == 400
        assert client.post("/tasks", json={"task_type": "dummy", "timeout_seconds": 0}).status_code == 422

        cases = {
            "add": {"task_type": "add", "args": [12, 30], "priority": "high"},
            "dummy": {"task_type": "dummy", "kwargs": {"sleep_seconds": 0.1}},
            "fibonacci": {"task_type": "fibonacci", "args": [20], "priority": "low"},
            "word_count": {"task_type": "word_count", "kwargs": {"text": "hello queue hello"}},
            "word_file": {"task_type": "word_count", "kwargs": {"input_path": str(output / "words.txt")}},
            "image_resize": {"task_type": "image_resize", "kwargs": {
                "input_path": str(output / "input.png"), "output_path": str(output / "resized.png"),
                "width": 8, "height": 6}},
            "retry": {"task_type": "flaky", "kwargs": {"fail_count": 2}},
            "dead_letter": {"task_type": "flaky", "kwargs": {"fail_count": 99}},
            "timeout": {"task_type": "dummy", "kwargs": {"sleep_seconds": 2}, "timeout_seconds": 1},
            "scheduled": {"task_type": "fibonacci", "args": [8],
                "run_at": (datetime.now(timezone.utc) + timedelta(seconds=8)).isoformat()},
        }
        for label, payload in cases.items():
            response = client.post("/tasks", json=payload)
            assert response.status_code == 201, response.text
            results[label] = response.json()
        assert results["scheduled"]["status"] == "scheduled"
        early = client.get("/tasks/" + results["scheduled"]["task_id"]).json()
        assert early["status"] == "scheduled", early

        pending = set(results)
        deadline = time.monotonic() + 120
        while pending and time.monotonic() < deadline:
            for label in list(pending):
                response = client.get("/tasks/" + results[label]["task_id"])
                response.raise_for_status()
                task = response.json()
                if task["status"] in {"completed", "dead_lettered", "failed"}:
                    results[label] = task
                    pending.remove(label)
                    print(f"{label}: {task['status']} ({task['attempt_count']} attempts)", flush=True)
            if pending:
                time.sleep(0.5)
        assert not pending, f"Tasks did not finish: {pending}"
        for label, task in results.items():
            expected = "dead_lettered" if label in {"dead_letter", "timeout"} else "completed"
            assert task["status"] == expected, (label, task)
        assert results["add"]["result"] == 42
        assert results["fibonacci"]["result"]["value"] == 6765
        assert results["scheduled"]["result"]["value"] == 21
        assert results["word_count"]["result"]["word_count"] == 3
        assert results["word_file"]["result"]["word_count"] == 3
        assert results["retry"]["retry_count"] == 2
        assert results["retry"]["attempt_count"] == 3
        for label in ("dead_letter", "timeout"):
            assert results[label]["attempt_count"] == 4
            assert results[label]["retry_count"] == 3
        assert results["timeout"]["failure_type"] == "timeout"
        with Image.open(output / "resized.png") as image:
            assert image.size == (8, 6)
        # Redis state commits slightly before the PostgreSQL audit append.
        histories = {}
        for label, task in results.items():
            for _ in range(20):
                response = client.get("/history/tasks/" + task["task_id"])
                response.raise_for_status()
                events = response.json()["events"]
                if any(event["event_type"] == task["status"] for event in events):
                    break
                time.sleep(0.1)
            else:
                raise AssertionError(f"Missing terminal history for {label}")
            histories[label] = events
        metrics = client.get("/metrics").json()
        assert metrics["workers"]["active_count"] == 3, metrics
        report = {"verified_at": datetime.now(timezone.utc).isoformat(),
                  "tasks": results, "history": histories, "metrics": metrics}
        (output / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"All live checks passed. Report: {output / 'report.json'}")


if __name__ == "__main__":
    main()

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import worker


class FakeBus:
    def __init__(self, assignments=()):
        self.subscribe_calls = []
        self.assignments = list(assignments)
        self.published = []
        self.next_id = 17

    def publish(self, topic, payload, **kwargs):
        self.published.append((topic, payload, kwargs))
        sent = {"id": self.next_id, "topic": topic, "payload": payload}
        self.next_id += 1
        return sent

    def subscribe(self, **kwargs):
        self.subscribe_calls.append(kwargs)
        return iter(self.assignments)

    def load_offset(self):
        raise AssertionError("worker must not load a durable offset")

    def save_offset(self, event_id):
        raise AssertionError("worker must not save a durable offset")


class WorkerCursorTests(unittest.TestCase):
    def test_worker_starts_from_registration_without_durable_offsets(self):
        fake_bus = FakeBus()
        args = SimpleNamespace(
            name="alice",
            capacity=1,
            capabilities=[],
            block=None,
            fail=None,
            fail_permanently=None,
        )

        with (
            patch("worker.parse_args", return_value=args),
            patch("worker.BusClient", return_value=fake_bus) as client_class,
            patch("worker.heartbeat_loop", return_value=None),
            patch("worker.uuid.uuid4", return_value=SimpleNamespace(hex="instance-1")),
        ):
            worker.main()

        client_class.assert_called_once_with(worker.BUS_URL, actor="alice")
        self.assertEqual(
            [{"topics": ["task.assigned"], "from_id": 17}],
            fake_bus.subscribe_calls,
        )

    def test_worker_can_report_retryable_attempt_failure(self):
        assignment = {
            "id": 21,
            "payload": {
                "task_id": 4,
                "assignment_id": "task:4:attempt:1",
                "attempt": 1,
                "assignee": "alice",
                "worker_instance_id": "instance-1",
                "goal": "demo",
            },
        }
        fake_bus = FakeBus([assignment])
        args = SimpleNamespace(
            name="alice",
            capacity=1,
            capabilities=[],
            block=None,
            fail=4,
            fail_permanently=None,
        )

        with (
            patch("worker.parse_args", return_value=args),
            patch("worker.BusClient", return_value=fake_bus),
            patch("worker.heartbeat_loop", return_value=None),
            patch("worker.time.sleep", return_value=None),
            patch("worker.uuid.uuid4", return_value=SimpleNamespace(hex="instance-1")),
        ):
            worker.main()

        topics = [item[0] for item in fake_bus.published]
        self.assertEqual(
            ["agent.registered", "task.started", "task.attempt_failed"],
            topics,
        )
        failure = fake_bus.published[-1]
        self.assertTrue(failure[1]["retryable"])
        self.assertEqual(
            "attempt-failed:task:4:attempt:1",
            failure[2]["idempotency_key"],
        )


if __name__ == "__main__":
    unittest.main()

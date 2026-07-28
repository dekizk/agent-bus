import unittest
from types import SimpleNamespace
from unittest.mock import patch

import worker


class FakeBus:
    def __init__(self):
        self.subscribe_calls = []

    def publish(self, topic, payload, **kwargs):
        if topic != "agent.registered":
            raise AssertionError(f"unexpected publish: {topic}")
        return {"id": 17}

    def subscribe(self, **kwargs):
        self.subscribe_calls.append(kwargs)
        return iter(())

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


if __name__ == "__main__":
    unittest.main()

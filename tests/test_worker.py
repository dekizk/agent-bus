import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import worker
from executors import (
    AssignmentContext,
    Blocked,
    Completed,
    PermanentFailure,
    RetryableFailure,
)


def assignment(task_id):
    return AssignmentContext(
        correlation_id="workflow-one",
        task_id=task_id,
        assignment_id=f"task:{task_id}:attempt:1",
        assignment_event_id=10 + task_id,
        attempt=1,
        goal=f"demo {task_id}",
        assignee="alice",
        worker_instance_id="alice-1",
    )


class DemoExecutorTests(unittest.TestCase):
    def test_demo_executor_returns_typed_outcomes_once(self):
        executor = worker.DemoExecutor(
            "alice",
            block_once=1,
            fail_once=2,
            fail_permanently_once=3,
        )
        with patch("worker.time.sleep", return_value=None):
            self.assertIsInstance(executor.execute(assignment(1)), Blocked)
            self.assertIsInstance(executor.execute(assignment(1)), Completed)
            self.assertIsInstance(executor.execute(assignment(2)), RetryableFailure)
            self.assertIsInstance(executor.execute(assignment(2)), Completed)
            self.assertIsInstance(executor.execute(assignment(3)), PermanentFailure)
            self.assertIsInstance(executor.execute(assignment(3)), Completed)


class WorkerMainTests(unittest.TestCase):
    def test_main_wires_demo_executor_into_worker_runtime(self):
        args = SimpleNamespace(
            name="alice",
            capacity=2,
            capabilities=["python"],
            block=None,
            fail=4,
            fail_permanently=None,
        )
        fake_bus = Mock()
        fake_runtime = Mock()

        with (
            patch("worker.parse_args", return_value=args),
            patch("worker.BusClient", return_value=fake_bus) as client_class,
            patch("worker.WorkerRuntime", return_value=fake_runtime) as runtime_class,
        ):
            worker.main()

        client_class.assert_called_once_with(worker.BUS_URL, actor="alice")
        runtime_class.assert_called_once()
        kwargs = runtime_class.call_args.kwargs
        self.assertEqual("alice", kwargs["name"])
        self.assertEqual(2, kwargs["capacity"])
        self.assertEqual(["python"], kwargs["capabilities"])
        self.assertIsInstance(kwargs["executor"], worker.DemoExecutor)
        fake_runtime.run.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()

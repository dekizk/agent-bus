import json
import sys
import threading
import time
import unittest
from unittest.mock import patch

from executors import (
    AssignmentContext,
    Blocked,
    Completed,
    InProcessExecutor,
    PermanentFailure,
    RetryableFailure,
    SubprocessExecutor,
)


def assignment(**overrides):
    values = {
        "correlation_id": "workflow-one",
        "task_id": 4,
        "assignment_id": "task:4:attempt:2",
        "assignment_event_id": 19,
        "attempt": 2,
        "goal": "Run the integration suite",
        "context": {"repository": "agent-bus", "flags": ["fast"]},
        "required_capabilities": ("python", "testing"),
        "max_retries": 3,
        "retryable_failures": 1,
        "assignee": "alice",
        "worker_instance_id": "alice-1",
        "ownership_mode": "canary",
        "ownership_owner": "agent-bus",
        "external_origin": {"system": "legacy", "task_ref": "work-4"},
    }
    values.update(overrides)
    return AssignmentContext(**values)


OUTCOMES = (
    Completed("done", {"commit": "abc"}),
    Blocked("approval required"),
    RetryableFailure("rate_limited", "try later"),
    PermanentFailure("invalid_goal", "cannot execute"),
)


def outcome_payload(outcome):
    if isinstance(outcome, Completed):
        return {"status": "completed", "summary": outcome.summary, "result": dict(outcome.result)}
    if isinstance(outcome, Blocked):
        return {"status": "blocked", "reason": outcome.reason}
    status = (
        "retryable_failure"
        if isinstance(outcome, RetryableFailure)
        else "permanent_failure"
    )
    return {"status": status, "code": outcome.code, "reason": outcome.reason}


class ExecutorConformanceMixin:
    def make_executor(self, outcome):
        raise NotImplementedError

    def test_all_standard_outcomes_conform(self):
        for expected in OUTCOMES:
            with self.subTest(outcome=expected.__class__.__name__):
                executor = self.make_executor(expected)
                actual = executor.execute(assignment())
                self.assertEqual(expected, actual)
                close = getattr(executor, "close", None)
                if callable(close):
                    close()


class InProcessConformanceTests(ExecutorConformanceMixin, unittest.TestCase):
    def make_executor(self, outcome):
        return InProcessExecutor(lambda received: outcome)

    def test_object_run_method_is_supported(self):
        class Agent:
            def run(self, received):
                return Completed(f"finished {received.task_id}")

        result = InProcessExecutor(Agent()).execute(assignment())
        self.assertEqual("finished 4", result.summary)

    def test_untyped_return_is_rejected(self):
        with self.assertRaisesRegex(TypeError, "executor must return"):
            InProcessExecutor(lambda received: {"status": "completed"}).execute(
                assignment()
            )


class SubprocessConformanceTests(ExecutorConformanceMixin, unittest.TestCase):
    def make_executor(self, outcome):
        encoded = json.dumps(outcome_payload(outcome))
        code = (
            "import json,sys; assignment=json.load(sys.stdin); "
            f"print({encoded!r})"
        )
        return SubprocessExecutor([sys.executable, "-c", code])

    def test_exit_75_is_retryable(self):
        executor = SubprocessExecutor(
            [sys.executable, "-c", "import sys; sys.exit(75)"]
        )
        self.assertIsInstance(executor.execute(assignment()), RetryableFailure)

    def test_invalid_or_oversized_output_is_permanent(self):
        invalid = SubprocessExecutor(
            [sys.executable, "-c", "print('not-json')"]
        )
        self.assertEqual(
            "invalid_executor_output",
            invalid.execute(assignment()).code,
        )

        oversized = SubprocessExecutor(
            [sys.executable, "-c", "print('x' * 1000)"],
            max_protocol_bytes=800,
        )
        self.assertEqual(
            "protocol_output_too_large",
            oversized.execute(assignment(context={})).code,
        )

    def test_timeout_is_retryable(self):
        executor = SubprocessExecutor(
            [sys.executable, "-c", "import time; time.sleep(1)"],
            timeout=0.02,
        )
        self.assertEqual("subprocess_timeout", executor.execute(assignment()).code)

    def test_cancel_terminates_a_running_subprocess(self):
        executor = SubprocessExecutor(
            [sys.executable, "-c", "import time; time.sleep(10)"],
            timeout=20,
        )
        results = []
        thread = threading.Thread(
            target=lambda: results.append(executor.execute(assignment()))
        )
        thread.start()
        deadline = time.time() + 1
        while time.time() < deadline:
            with executor._lock:
                if executor._processes:
                    break
            time.sleep(0.005)
        executor.cancel("task:4:attempt:2")
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual("subprocess_cancelled", results[0].code)

    def test_process_creation_failure_is_reported_as_start_failure(self):
        executor = SubprocessExecutor(["missing-agent"])
        with patch("executors.subprocess.Popen", side_effect=OSError("missing")):
            result = executor.execute(assignment())
        self.assertIsInstance(result, PermanentFailure)
        self.assertEqual("subprocess_start_failed", result.code)

    def test_post_start_os_error_is_retryable_io_failure(self):
        class EmptyPipe:
            def read(self, size):
                return b""

            def close(self):
                pass

        class FailingStdin:
            def write(self, value):
                raise OSError("write failed")

            def close(self):
                pass

        class StartedProcess:
            def __init__(self):
                self.stdin = FailingStdin()
                self.stdout = EmptyPipe()
                self.stderr = EmptyPipe()
                self.returncode = None

            def poll(self):
                return self.returncode

            def terminate(self):
                self.returncode = -15

            def wait(self, timeout=None):
                return self.returncode

            def kill(self):
                self.returncode = -9

        executor = SubprocessExecutor(["agent"])
        with patch("executors.subprocess.Popen", return_value=StartedProcess()):
            result = executor.execute(assignment())
        self.assertIsInstance(result, RetryableFailure)
        self.assertEqual("subprocess_io_failed", result.code)
        self.assertIn("write failed", result.reason)
        self.assertEqual({}, executor._processes)
        self.assertEqual(set(), executor._cancelled)


class AssignmentContextTests(unittest.TestCase):
    def test_context_is_immutable_and_serializes_for_adapters(self):
        value = assignment()
        with self.assertRaises(TypeError):
            value.context["repository"] = "other"
        with self.assertRaises(TypeError):
            value.external_origin["task_ref"] = "other"

        serialized = value.to_dict()
        self.assertEqual(["fast"], serialized["context"]["flags"])
        self.assertEqual("canary", serialized["ownership"]["mode"])

    def test_assignment_event_contract_is_parsed(self):
        source = assignment()
        event = {
            "id": source.assignment_event_id,
            "correlation_id": source.correlation_id,
            "payload": {
                "task_id": source.task_id,
                "assignment_id": source.assignment_id,
                "attempt": source.attempt,
                "goal": source.goal,
                "context": source.to_dict()["context"],
                "required_capabilities": list(source.required_capabilities),
                "retry_policy": {"max_retries": source.max_retries},
                "retryable_failures": source.retryable_failures,
                "assignee": source.assignee,
                "worker_instance_id": source.worker_instance_id,
                "ownership": source.to_dict()["ownership"],
                "external_origin": source.to_dict()["external_origin"],
            },
        }
        self.assertEqual(source, AssignmentContext.from_event(event))


if __name__ == "__main__":
    unittest.main()

import threading
import time
import unittest

import httpx

from executors import Blocked, Completed, PermanentFailure, RetryableFailure
from runtime import RUNTIME_TOPICS, WorkerRuntime


def assigned_event(task_id=1, attempt=1, *, event_id=10, instance="alice-1"):
    return {
        "id": event_id,
        "ts": 100.0,
        "topic": "task.assigned",
        "actor": "pm",
        "correlation_id": "workflow-one",
        "payload": {
            "task_id": task_id,
            "assignment_id": f"task:{task_id}:attempt:{attempt}",
            "attempt": attempt,
            "assignee": "alice",
            "worker_instance_id": instance,
            "goal": f"task {task_id}",
            "context": {"source": "test"},
            "required_capabilities": ["python"],
            "retry_policy": {"max_retries": 2},
            "retryable_failures": attempt - 1,
            "ownership": {"mode": "controlled", "owner": "agent-bus"},
        },
    }


class FakeBus:
    actor = "alice"

    def __init__(self, events=()):
        self.events = list(events)
        self.published = []
        self.subscribe_calls = []
        self._next_id = 100
        self._lock = threading.Lock()

    def publish(self, topic, payload, **kwargs):
        with self._lock:
            event = {
                "id": self._next_id,
                "ts": time.time(),
                "topic": topic,
                "actor": self.actor,
                "correlation_id": "workflow-one",
                "payload": payload,
                **kwargs,
            }
            self._next_id += 1
            self.published.append(event)
            return event

    def subscribe(self, **kwargs):
        self.subscribe_calls.append(kwargs)
        return iter(self.events)


class StaticExecutor:
    def __init__(self, outcome):
        self.outcome = outcome

    def execute(self, assignment):
        return self.outcome


class RuntimeOutcomeTests(unittest.TestCase):
    def run_outcome(self, outcome, **runtime_options):
        fake_bus = FakeBus([assigned_event()])
        runtime = WorkerRuntime(
            fake_bus,
            name="alice",
            instance_id="alice-1",
            executor=StaticExecutor(outcome),
            capabilities=["python"],
            heartbeat_seconds=100,
            log=lambda message: None,
            **runtime_options,
        )
        runtime.run()
        return fake_bus

    def test_outcomes_translate_to_lifecycle_events(self):
        cases = (
            (Completed("done", {"value": 1}), "task.completed"),
            (Blocked("approval"), "task.blocked"),
            (RetryableFailure("temporary", "retry"), "task.attempt_failed"),
            (PermanentFailure("invalid", "stop"), "task.attempt_failed"),
        )
        for outcome, final_topic in cases:
            with self.subTest(outcome=outcome.__class__.__name__):
                bus = self.run_outcome(outcome)
                topics = [event["topic"] for event in bus.published]
                self.assertEqual(
                    ["agent.registered", "task.started", final_topic],
                    topics,
                )
                final = bus.published[-1]
                self.assertEqual(
                    f"task:1:attempt:1",
                    final["payload"]["assignment_id"],
                )
                if isinstance(outcome, RetryableFailure):
                    self.assertTrue(final["payload"]["retryable"])
                if isinstance(outcome, PermanentFailure):
                    self.assertFalse(final["payload"]["retryable"])

    def test_unexpected_exception_policy_is_explicit(self):
        class Broken:
            def execute(self, assignment):
                raise RuntimeError("boom")

        for retryable in (False, True):
            with self.subTest(retryable=retryable):
                bus = FakeBus([assigned_event()])
                runtime = WorkerRuntime(
                    bus,
                    name="alice",
                    instance_id="alice-1",
                    executor=Broken(),
                    heartbeat_seconds=100,
                    unexpected_exceptions_retryable=retryable,
                    log=lambda message: None,
                )
                runtime.run()
                self.assertEqual(
                    retryable,
                    bus.published[-1]["payload"]["retryable"],
                )
                self.assertEqual(
                    "executor_exception",
                    bus.published[-1]["payload"]["failure_code"],
                )

    def test_subscription_starts_at_registration_and_uses_runtime_topics(self):
        bus = self.run_outcome(Completed("done"))
        call = bus.subscribe_calls[0]
        self.assertEqual(list(RUNTIME_TOPICS), call["topics"])
        self.assertEqual(100, call["from_id"])
        self.assertIsInstance(call["stop_event"], threading.Event)

    def test_nonretryable_bus_rejection_stops_the_worker(self):
        class RejectingBus(FakeBus):
            def publish(self, topic, payload, **kwargs):
                if topic == "task.started":
                    request = httpx.Request("POST", "http://bus/events")
                    response = httpx.Response(422, request=request)
                    raise httpx.HTTPStatusError(
                        "invalid lifecycle event",
                        request=request,
                        response=response,
                    )
                return super().publish(topic, payload, **kwargs)

        class CountingExecutor:
            calls = 0

            def execute(self, assignment):
                self.calls += 1
                return Completed("done")

        executor = CountingExecutor()
        runtime = WorkerRuntime(
            RejectingBus([assigned_event()]),
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        )
        runtime.run()
        self.assertTrue(runtime.stop_event.is_set())
        self.assertEqual(0, executor.calls)


class RuntimeOwnershipTests(unittest.TestCase):
    def test_capacity_bounds_concurrent_execution(self):
        release = threading.Event()
        two_running = threading.Event()

        class MeasuringExecutor:
            def __init__(self):
                self.lock = threading.Lock()
                self.current = 0
                self.maximum = 0

            def execute(self, assignment):
                with self.lock:
                    self.current += 1
                    self.maximum = max(self.maximum, self.current)
                    if self.current == 2:
                        two_running.set()
                release.wait(2)
                with self.lock:
                    self.current -= 1
                return Completed("done")

        executor = MeasuringExecutor()
        bus = FakeBus(
            [
                assigned_event(1, event_id=10),
                assigned_event(2, event_id=11),
                assigned_event(3, event_id=12),
            ]
        )
        runtime = WorkerRuntime(
            bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            capacity=2,
            heartbeat_seconds=100,
            log=lambda message: None,
        )
        thread = threading.Thread(target=runtime.run)
        thread.start()
        self.assertTrue(two_running.wait(1))
        self.assertEqual(2, executor.maximum)
        release.set()
        thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(3, len([e for e in bus.published if e["topic"] == "task.completed"]))

    def test_expiry_cancels_adapter_and_suppresses_late_result(self):
        began = threading.Event()
        release = threading.Event()

        class CancellableExecutor:
            def __init__(self):
                self.cancelled = []

            def execute(self, assignment):
                began.set()
                release.wait(2)
                return Completed("too late")

            def cancel(self, assignment_id):
                self.cancelled.append(assignment_id)

        assignment = assigned_event()
        expiry = {
            "id": 11,
            "topic": "task.assignment_expired",
            "payload": {
                "task_id": 1,
                "assignment_id": "task:1:attempt:1",
                "worker_instance_id": "alice-1",
            },
        }

        def events():
            yield assignment
            self.assertTrue(began.wait(1))
            yield expiry
            release.set()

        executor = CancellableExecutor()
        bus = FakeBus()
        runtime = WorkerRuntime(
            bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        )
        runtime.run(events())
        self.assertEqual(["task:1:attempt:1"], executor.cancelled)
        self.assertNotIn("task.completed", [event["topic"] for event in bus.published])

    def test_expiry_of_queued_work_does_not_cancel_adapter(self):
        began = threading.Event()
        release = threading.Event()

        class CancellableExecutor:
            def __init__(self):
                self.cancelled = []

            def execute(self, assignment):
                began.set()
                release.wait(2)
                return Completed("done")

            def cancel(self, assignment_id):
                self.cancelled.append(assignment_id)

        expiry = {
            "id": 12,
            "topic": "task.assignment_expired",
            "payload": {
                "task_id": 2,
                "assignment_id": "task:2:attempt:1",
                "worker_instance_id": "alice-1",
            },
        }

        def events():
            yield assigned_event(1, event_id=10)
            self.assertTrue(began.wait(1))
            yield assigned_event(2, event_id=11)
            yield expiry
            release.set()

        executor = CancellableExecutor()
        bus = FakeBus()
        runtime = WorkerRuntime(
            bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            capacity=1,
            heartbeat_seconds=100,
            log=lambda message: None,
        )
        runtime.run(events())
        self.assertEqual([], executor.cancelled)
        started_task_ids = [
            event["payload"]["task_id"]
            for event in bus.published
            if event["topic"] == "task.started"
        ]
        self.assertEqual([1], started_task_ids)

    def test_replacement_instance_stops_old_runtime(self):
        bus = FakeBus()
        runtime = WorkerRuntime(
            bus,
            name="alice",
            instance_id="alice-1",
            executor=StaticExecutor(Completed("done")),
            heartbeat_seconds=100,
            log=lambda message: None,
        )
        runtime.process_event(
            {
                "topic": "agent.registered",
                "payload": {"name": "alice", "instance_id": "alice-2"},
            }
        )
        self.assertTrue(runtime.stop_event.is_set())


if __name__ == "__main__":
    unittest.main()

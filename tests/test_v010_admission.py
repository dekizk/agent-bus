"""PM/runtime agreement against real persisted coordination history."""

import contextlib
import io
import threading
import time
import unittest
from unittest.mock import patch

import httpx

import bus
from executors import AssignmentContext, Completed
from pm_agent import PM_TOPICS, plan_next_emission, reconcile
from projection import replay_events
from runtime import WorkerRuntime
from telemetry import BusTelemetrySink, ProducerIdentity
from tests import test_v010_phase3_accounting as accounting
from tests.test_v010_phase3_accounting import (
    DirectPMBus,
    DirectWorkerBus,
)


class PollingWorkerBus(DirectWorkerBus):
    def __init__(self):
        self.subscribed = threading.Event()
        self.seen_id = 0
        self.query_offsets = []

    def query_all(self, *, after_id=0, topics=None):
        self.query_offsets.append(after_id)
        return bus.fetch_after(after_id, topics)

    def get_event(self, event_id):
        return bus.fetch_event(event_id)

    def subscribe(self, *, topics, from_id, stop_event):
        self.subscribed.set()
        cursor = from_id
        while not stop_event.is_set():
            for event in bus.fetch_after(cursor, topics):
                yield event
                cursor = event["id"]
                self.seen_id = cursor
            stop_event.wait(0.005)


class RecordingExecutor:
    def __init__(self):
        self.assignments = []

    def execute(self, assignment):
        self.assignments.append(assignment.assignment_id)
        return Completed("admission regression completed", {})


class AdmissionAgreementTests(unittest.TestCase):
    def setUp(self):
        self.fixture = accounting.Phase3CAccountingTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

    def start_worker(self, worker_bus=None):
        worker_bus = worker_bus or PollingWorkerBus()
        executor = RecordingExecutor()
        runtime = WorkerRuntime(
            worker_bus, name="alice", instance_id="alice-1", executor=executor,
            heartbeat_seconds=0.02, publish_retry_seconds=0.001,
            log=lambda _: None,
        )
        errors = []

        def run():
            try:
                runtime.run()
            except Exception as exc:
                errors.append(exc)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()

        def cleanup():
            runtime.stop()
            thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertEqual([], errors)

        self.addCleanup(cleanup)
        self.assertTrue(worker_bus.subscribed.wait(2))
        return worker_bus, executor, runtime

    def wait_for(self, condition):
        deadline = time.monotonic() + 2
        while not condition() and time.monotonic() < deadline:
            threading.Event().wait(0.005)
        self.assertTrue(condition())

    def reconcile(self, state, cursor, publisher=None, **options):
        with contextlib.redirect_stdout(io.StringIO()):
            return reconcile(state, publisher or DirectPMBus(), cursor=cursor, **options)

    def report(self, assignment, invocation, tokens):
        sink = BusTelemetrySink(
            DirectWorkerBus(), producer=ProducerIdentity("admission-test", "alice-1", "1")
        )
        return sink.model_completed(
            AssignmentContext.from_event(assignment), invocation_id=invocation,
            provider="test", model="test", duration_ms=1,
            usage={"total_tokens": tokens, "cost_usd": 0},
        )

    def test_late_usage_fences_execution_then_policy_extension_recovers(self):
        f = self.fixture
        f._register(capacity=1)
        f._create("budget-race", "first")
        second = f._create("budget-race", "second")
        f._budget_policy("budget-race", tokens=100, reserve_tokens=60)
        state, cursor = f._projection()
        first = self.reconcile(state, cursor)[-1]
        self.report(first, "first-call", 40)
        f._complete(first)
        worker_bus, executor, runtime = self.start_worker()
        cursor.catch_up(DirectPMBus())
        injected = []

        def race(topic, payload, kwargs):
            if topic == "task.assigned" and not injected:
                injected.append(self.report(first, "late-second-call", 80))

        stale = self.reconcile(state, cursor, DirectPMBus(race))[-1]
        self.wait_for(lambda: worker_bus.seen_id >= stale["id"])
        self.wait_for(lambda: len(bus.fetch_after(0, ["agent.heartbeat"])) >= 2)
        self.assertEqual([], executor.assignments)
        self.assertEqual([], bus.fetch_after(stale["id"], ["task.started"]))
        self.assertFalse(runtime.stop_event.is_set())
        self.assertEqual("open", state.tasks[second["payload"]["task_id"]].status)
        self.assertEqual(120, state.workflow_budget_usage("budget-race")["tokens"])

        f._budget_policy("budget-race", tokens=200, reserve_tokens=60, reason="extend")
        cursor.catch_up(DirectPMBus())
        accepted = self.reconcile(state, cursor)[-1]
        self.wait_for(lambda: bool(bus.fetch_after(accepted["id"], ["task.completed"])))
        self.assertEqual([accepted["payload"]["assignment_id"]], executor.assignments)
        self.assertEqual(2, accepted["payload"]["attempt"])
        self.assertEqual([0, stale["id"]], worker_bus.query_offsets)
        restarted = replay_events(bus.fetch_after(0, list(PM_TOPICS)))
        self.assertEqual("completed", restarted.tasks[second["payload"]["task_id"]].status)

    def test_agent_default_crossing_operator_policy_does_not_stall_worker(self):
        self.check_default_policy_race("agent.policy_set")

    def test_workflow_default_crossing_operator_policy_does_not_stall_worker(self):
        self.check_default_policy_race("workflow.policy_set")

    def check_default_policy_race(self, topic_to_race):
        task = self.fixture._create("default-race", "task")
        worker_bus, executor, runtime = self.start_worker()
        state, cursor = self.fixture._projection()
        injected = []

        def race(topic, payload, kwargs):
            if topic == topic_to_race and not injected:
                policy = {"source": "operator", "reason": "operator wins", "max_active_assignments": 1}
                options = {}
                if topic == "agent.policy_set":
                    policy["agent_name"] = "alice"
                else:
                    options["correlation_id"] = "default-race"
                injected.append(bus.append_event(topic, "human", policy, **options))

        emitted = self.reconcile(
            state, cursor, DirectPMBus(race),
            default_workflow_max_active_assignments=2,
            default_agent_max_active_assignments=2,
        )
        assigned = next(event for event in emitted if event["topic"] == "task.assigned")
        self.wait_for(lambda: bool(bus.fetch_after(assigned["id"], ["task.completed"])))
        self.assertEqual([assigned["payload"]["assignment_id"]], executor.assignments)
        self.assertFalse(runtime.stop_event.is_set())
        key = "agent_policy_event_id" if topic_to_race == "agent.policy_set" else "workflow_policy_event_id"
        self.assertEqual(injected[0]["id"], assigned["payload"][key])
        replayed = replay_events(bus.fetch_after(0, list(PM_TOPICS)))
        self.assertEqual("completed", replayed.tasks[task["payload"]["task_id"]].status)

    def test_wall_clock_expiring_during_publication_prevents_execution(self):
        task = self.fixture._create("wall-race", "task")
        worker_bus, executor, _ = self.start_worker()
        bus.append_event("workflow.policy_set", "human", {
            "source": "operator", "reason": "one second", "max_wall_clock_seconds": 1,
        }, correlation_id="wall-race")
        state, cursor = self.fixture._projection()
        planned = plan_next_emission(state, task["ts"] + 0.5)
        self.assertEqual("task.assigned", planned["topic"])
        with patch("bus.time.time", return_value=task["ts"] + 2):
            stale = bus.append_event(planned["topic"], "pm", planned["payload"],
                                     caused_by=planned["caused_by"], idempotency_key=planned["idempotency_key"])
        # A separate admission reader sees exactly the same timestamp boundary.
        runtime = WorkerRuntime(PollingWorkerBus(), name="alice", instance_id="alice-1",
                                executor=RecordingExecutor())
        self.assertFalse(runtime._assignment_is_accepted(stale))
        self.wait_for(lambda: worker_bus.seen_id >= stale["id"])
        self.assertEqual([], executor.assignments)
        replayed = replay_events(bus.fetch_after(0, list(PM_TOPICS)))
        self.assertEqual("open", replayed.tasks[task["payload"]["task_id"]].status)

    def test_later_policy_decrease_does_not_retroactively_reject_assignment(self):
        self.fixture._register(capacity=1)
        self.fixture._create("prefix", "task")
        state, cursor = self.fixture._projection()
        accepted = self.reconcile(state, cursor)[-1]
        policy = bus.append_event("agent.policy_set", "human", {
            "agent_name": "alice", "source": "operator", "reason": "later", "max_active_assignments": 1,
        })
        runtime = WorkerRuntime(PollingWorkerBus(), name="alice", instance_id="alice-1",
                                executor=RecordingExecutor())
        self.assertTrue(runtime._assignment_is_accepted(accepted))
        self.assertLess(runtime._admission_event_id, policy["id"])
        self.assertFalse(runtime._assignment_is_accepted(accepted))

    def test_incomplete_or_unavailable_history_stops_worker(self):
        self.fixture._register(capacity=1)
        self.fixture._create("history", "task")
        state, cursor = self.fixture._projection()
        assignment = self.reconcile(state, cursor)[-1]
        for failure in ("missing", "offline"):
            with self.subTest(failure=failure):
                worker_bus = PollingWorkerBus()
                executor = RecordingExecutor()
                runtime = WorkerRuntime(worker_bus, name="alice", instance_id="alice-1",
                                        executor=executor, log=lambda _: None)
                options = {"return_value": []} if failure == "missing" else {
                    "side_effect": httpx.ConnectError("offline")
                }
                with patch.object(worker_bus, "query_all", **options):
                    runtime.run([assignment])
                self.assertTrue(runtime.stop_event.is_set())
                self.assertEqual([], executor.assignments)

    def test_admission_error_does_not_wait_for_another_stream_event(self):
        self.fixture._register(capacity=1)
        self.fixture._create("stop-stream", "task")
        state, cursor = self.fixture._projection()
        assignment = self.reconcile(state, cursor)[-1]
        worker_bus = PollingWorkerBus()
        executor = RecordingExecutor()
        runtime = WorkerRuntime(worker_bus, name="alice", instance_id="alice-1",
                                executor=executor, log=lambda _: None)

        def stream():
            yield assignment
            self.fail("runtime requested another stream event after stopping")

        with patch.object(worker_bus, "query_all", side_effect=httpx.ConnectError("offline")):
            runtime.run(stream())
        self.assertTrue(runtime._closed)
        self.assertTrue(runtime._heartbeat_stop.is_set())
        self.assertEqual([], executor.assignments)


if __name__ == "__main__":
    unittest.main()

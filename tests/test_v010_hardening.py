import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import bus
from executors import Completed
from operations import explain_task
from pm_agent import (
    PM_TOPICS,
    OrderedProjectionCursor,
    PMState,
    plan_next_emission,
    reconcile,
)
from projection import apply_event, replay_events
from runtime import WorkerRuntime
from tests.test_pm_agent import assigned, created, event, registered
from tests.test_runtime import FakeBus as RuntimeFakeBus, assigned_event


class DirectPMBus:
    def __init__(self, before_publish=None):
        self.before_publish = before_publish

    def publish(self, topic, payload, **kwargs):
        if self.before_publish is not None:
            self.before_publish(topic)
        return bus.append_event(topic, "pm", payload, **kwargs)

    def query_all(self, *, after_id=0, topics=None):
        return bus.fetch_after(after_id, topics)


class HardeningDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = bus.DB_PATH
        bus.DB_PATH = Path(self.temp_dir.name) / "events.db"
        bus.init_db()

    def tearDown(self):
        bus.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_ordered_pm_queues_resume_that_races_pause_acknowledgement(self):
        root = bus.append_event("task.created", "human", {"title": "demo"})
        task_id = root["payload"]["task_id"]
        bus.append_event(
            "task.pause_requested",
            "human",
            {"task_id": task_id, "reason": "hold"},
        )
        state = PMState()
        cursor = OrderedProjectionCursor(state)
        cursor.consume(bus.fetch_after(0, list(PM_TOPICS)))
        injected = False

        def race(topic):
            nonlocal injected
            if topic == "task.paused" and not injected:
                injected = True
                bus.append_event(
                    "task.resume_requested",
                    "human",
                    {"task_id": task_id, "reason": "continue"},
                )

        reconcile(state, DirectPMBus(race), now=root["ts"], cursor=cursor)
        replayed = replay_events(bus.fetch_after(0, list(PM_TOPICS)))

        self.assertEqual("open", state.tasks[task_id].status)
        self.assertEqual("open", replayed.tasks[task_id].status)
        self.assertEqual(
            state.tasks[task_id].resumed_event_id,
            replayed.tasks[task_id].resumed_event_id,
        )

    def test_stale_assignment_is_fenced_and_post_resume_identity_advances(self):
        worker = bus.append_event(
            "agent.registered",
            "alice",
            {
                "name": "alice",
                "instance_id": "alice-1",
                "capacity": 1,
                "capabilities": [],
            },
        )
        root = bus.append_event(
            "task.created",
            "human",
            {"title": "demo"},
            correlation_id="flow",
        )
        task_id = root["payload"]["task_id"]
        state = PMState()
        cursor = OrderedProjectionCursor(state)
        cursor.consume(bus.fetch_after(0, list(PM_TOPICS)))
        injected = False

        def race(topic):
            nonlocal injected
            if topic == "task.assigned" and not injected:
                injected = True
                bus.append_event(
                    "workflow.pause_requested",
                    "human",
                    {"reason": "hold"},
                    correlation_id="flow",
                )

        pm_bus = DirectPMBus(race)
        reconcile(state, pm_bus, now=worker["ts"], cursor=cursor)
        self.assertEqual("open", state.tasks[task_id].status)
        self.assertEqual(1, state.tasks[task_id].last_assignment_attempt)
        self.assertEqual("paused", state.workflows["flow"].status)

        bus.append_event(
            "workflow.resume_requested",
            "human",
            {"reason": "continue"},
            correlation_id="flow",
        )
        cursor.catch_up(pm_bus)
        reconcile(state, pm_bus, now=worker["ts"], cursor=cursor)

        self.assertEqual("task:1:attempt:2", state.tasks[task_id].assignment_id)
        replayed = replay_events(bus.fetch_after(0, list(PM_TOPICS)))
        self.assertEqual(state.tasks[task_id].assignment_id, replayed.tasks[task_id].assignment_id)
        self.assertEqual(state.tasks[task_id].status, replayed.tasks[task_id].status)

    def test_workflow_pause_ack_snapshot_survives_concurrent_cancellation_and_restart(self):
        state, cursor, pm_bus, root, task_id = self._active_workflow("cancel-race")
        pause_request = bus.append_event(
            "workflow.pause_requested",
            "human",
            {"reason": "hold"},
            correlation_id="cancel-race",
        )
        cursor.catch_up(pm_bus)
        injected = False

        def race(topic):
            nonlocal injected
            if topic == "workflow.paused" and not injected:
                injected = True
                bus.append_event(
                    "task.cancel_requested",
                    "human",
                    {"task_id": task_id, "reason": "cancel during pause"},
                )

        reconcile(
            state,
            DirectPMBus(race),
            now=root["ts"],
            cursor=cursor,
        )

        pause_acks = [
            item
            for item in bus.fetch_after(0, None)
            if item["topic"] == "workflow.paused"
        ]
        self.assertEqual(1, len(pause_acks))
        self.assertEqual(
            [
                {
                    "task_id": task_id,
                    "assignment_id": f"task:{task_id}:attempt:1",
                }
            ],
            pause_acks[0]["payload"]["interrupted_assignments"],
        )
        self.assertEqual(pause_request["id"], pause_acks[0]["caused_by"])
        self.assertEqual("cancelled", state.tasks[task_id].status)
        self.assertEqual("paused", state.workflows["cancel-race"].status)
        self.assertEqual((), state.workflows["cancel-race"].interrupted_task_ids)

        restarted = PMState()
        restart_cursor = OrderedProjectionCursor(restarted)
        restart_cursor.consume(bus.fetch_after(0, list(PM_TOPICS)))
        emitted = reconcile(
            restarted,
            DirectPMBus(),
            now=root["ts"],
            cursor=restart_cursor,
        )
        self.assertEqual([], emitted)
        self.assertEqual("cancelled", restarted.tasks[task_id].status)
        self.assertEqual("paused", restarted.workflows["cancel-race"].status)

    def test_workflow_pause_ack_snapshot_tolerates_stronger_task_transitions(self):
        cases = {
            "task-pause": "paused",
            "supersession": "superseded",
            "deadline": "deadline_exceeded",
        }
        for case, expected_status in cases.items():
            with self.subTest(case=case):
                bus.DB_PATH = Path(self.temp_dir.name) / f"{case}.db"
                bus.init_db()
                deadline_at = 9_999_999_999.0 if case == "deadline" else None
                state, cursor, pm_bus, root, task_id = self._active_workflow(
                    case,
                    deadline_at=deadline_at,
                )
                bus.append_event(
                    "workflow.pause_requested",
                    "human",
                    {"reason": "hold"},
                    correlation_id=case,
                )
                cursor.catch_up(pm_bus)
                injected = False

                def race(topic):
                    nonlocal injected
                    if topic != "workflow.paused" or injected:
                        return
                    injected = True
                    if case == "task-pause":
                        bus.append_event(
                            "task.pause_requested",
                            "human",
                            {"task_id": task_id, "reason": "individual hold"},
                        )
                    elif case == "supersession":
                        bus.append_event(
                            "task.created",
                            "human",
                            {
                                "title": "replacement",
                                "supersedes_task_id": task_id,
                                "supersession_reason": "new intent",
                            },
                        )
                    else:
                        with patch.object(bus.time, "time", return_value=deadline_at + 1):
                            bus.append_event(
                                "task.deadline_exceeded",
                                "pm",
                                {
                                    "task_id": task_id,
                                    "deadline_at": deadline_at,
                                    "last_assignment_id": f"task:{task_id}:attempt:1",
                                    "attempts": 1,
                                },
                                caused_by=root["id"],
                                idempotency_key=(
                                    f"deadline-exceeded:task:{task_id}:created:{root['id']}"
                                ),
                            )

                reconcile(
                    state,
                    DirectPMBus(race),
                    now=root["ts"],
                    cursor=cursor,
                )
                self.assertEqual(expected_status, state.tasks[task_id].status)
                self.assertEqual("paused", state.workflows[case].status)
                self.assertEqual((), state.workflows[case].interrupted_task_ids)

                restarted = replay_events(bus.fetch_after(0, list(PM_TOPICS)))
                restart_cursor = OrderedProjectionCursor(
                    restarted,
                    last_event_id=max(
                        item["id"]
                        for item in bus.fetch_after(0, list(PM_TOPICS))
                    ),
                )
                self.assertEqual(
                    [],
                    reconcile(
                        restarted,
                        DirectPMBus(),
                        now=root["ts"],
                        cursor=restart_cursor,
                    ),
                )
                self.assertEqual(expected_status, restarted.tasks[task_id].status)
                self.assertEqual("paused", restarted.workflows[case].status)

    def test_concurrent_identical_supersession_returns_one_event(self):
        bus.append_event("task.created", "human", {"title": "old"})
        payload = {
            "title": "new",
            "supersedes_task_id": 1,
            "supersession_reason": "changed",
        }
        results = self._race_identical_appends(
            "task.created", payload, idempotency_key="replace:1"
        )
        self.assertEqual(results[0]["id"], results[1]["id"])
        self.assertEqual(2, len(bus.fetch_after(0, None)))

    def test_concurrent_identical_external_origin_returns_one_event(self):
        payload = {
            "title": "imported",
            "external_origin": {"system": "legacy", "task_ref": "work-1"},
            "ownership": {"mode": "controlled", "owner": "agent-bus"},
        }
        results = self._race_identical_appends(
            "task.created", payload, idempotency_key="adopt:work-1"
        )
        self.assertEqual(results[0]["id"], results[1]["id"])
        self.assertEqual(1, len(bus.fetch_after(0, None)))

    def test_superseded_failed_task_cannot_be_retried(self):
        old = bus.append_event("task.created", "human", {"title": "old"})
        bus.append_event(
            "task.created",
            "human",
            {
                "title": "replacement",
                "supersedes_task_id": old["payload"]["task_id"],
                "supersession_reason": "new intent",
            },
        )
        with self.assertRaisesRegex(bus.EventValidationError, "cannot be retried"):
            bus.append_event(
                "task.retry_requested",
                "human",
                {
                    "task_id": old["payload"]["task_id"],
                    "additional_retries": 1,
                    "reason": "revive old intent",
                },
                caused_by=old["id"],
            )

    def _race_identical_appends(self, topic, payload, *, idempotency_key):
        original = bus._resolve_correlation_id
        barrier = threading.Barrier(2)

        def resolve(*args, **kwargs):
            result = original(*args, **kwargs)
            barrier.wait(timeout=5)
            return result

        with patch.object(bus, "_resolve_correlation_id", side_effect=resolve):
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [
                    pool.submit(
                        bus.append_event,
                        topic,
                        "human",
                        payload,
                        None,
                        idempotency_key,
                    )
                    for _ in range(2)
                ]
                return [future.result(timeout=10) for future in futures]

    def _active_workflow(self, correlation_id, *, deadline_at=None):
        worker = bus.append_event(
            "agent.registered",
            "alice",
            {
                "name": "alice",
                "instance_id": "alice-1",
                "capacity": 1,
                "capabilities": [],
            },
        )
        payload = {"title": "demo"}
        if deadline_at is not None:
            payload["deadline_at"] = deadline_at
        root = bus.append_event(
            "task.created",
            "human",
            payload,
            correlation_id=correlation_id,
        )
        task_id = root["payload"]["task_id"]
        state = PMState()
        cursor = OrderedProjectionCursor(state)
        cursor.consume(bus.fetch_after(0, list(PM_TOPICS)))
        pm_bus = DirectPMBus()
        reconcile(state, pm_bus, now=worker["ts"], cursor=cursor)
        self.assertEqual("assigned", state.tasks[task_id].status)
        return state, cursor, pm_bus, root, task_id


class HardeningProjectionTests(unittest.TestCase):
    def test_expiry_during_workflow_pause_does_not_consume_retry_budget(self):
        state = PMState()
        for item in (
            created(event_id=1, correlation_id="flow", max_retries=0),
            assigned(2, 1),
            event(
                3,
                "workflow.pause_requested",
                "human",
                {"reason": "hold"},
                correlation_id="flow",
            ),
        ):
            self.assertTrue(apply_event(state, item))
        self.assertFalse(
            apply_event(
                state,
                event(
                    4,
                    "task.assignment_expired",
                    "pm",
                    {
                        "task_id": 1,
                        "assignment_id": "task:1:attempt:1",
                        "assignee": "alice",
                        "worker_instance_id": "alice-1",
                        "reason": "lease expired",
                    },
                ),
            )
        )
        self.assertEqual(0, state.tasks[1].retryable_failures)

    def test_task_and_workflow_early_resumes_are_queued(self):
        task_state = PMState()
        for item in (
            created(event_id=1, correlation_id="flow"),
            event(2, "task.pause_requested", "human", {"task_id": 1, "reason": "hold"}),
            event(3, "task.resume_requested", "human", {"task_id": 1, "reason": "go"}),
        ):
            self.assertTrue(apply_event(task_state, item))
        self.assertEqual(
            "resume_queued",
            explain_task(task_state, 1, now=101.0, lease_seconds=20)["code"],
        )
        task_pause = plan_next_emission(task_state, 101.0)
        self.assertEqual("task.paused", task_pause["topic"])
        self.assertTrue(apply_event(task_state, materialize(task_pause, 4)))
        self.assertEqual("resume_requested", task_state.tasks[1].status)
        self.assertEqual("task.resumed", plan_next_emission(task_state, 101.0)["topic"])

        workflow_state = PMState()
        for item in (
            created(event_id=1, correlation_id="flow"),
            event(2, "workflow.pause_requested", "human", {"reason": "hold"}, correlation_id="flow"),
            event(3, "workflow.resume_requested", "human", {"reason": "go"}, correlation_id="flow"),
        ):
            self.assertTrue(apply_event(workflow_state, item))
        self.assertEqual(
            "workflow_resume_queued",
            explain_task(workflow_state, 1, now=101.0, lease_seconds=20)["code"],
        )
        workflow_pause = plan_next_emission(workflow_state, 101.0)
        self.assertEqual("workflow.paused", workflow_pause["topic"])
        self.assertTrue(apply_event(workflow_state, materialize(workflow_pause, 4)))
        self.assertEqual("resume_requested", workflow_state.workflows["flow"].status)
        self.assertEqual("workflow.resumed", plan_next_emission(workflow_state, 101.0)["topic"])

    def test_stale_assignment_during_task_pause_uses_fresh_post_resume_identity(self):
        state = PMState()
        self.assertTrue(apply_event(state, created(event_id=1, correlation_id="flow")))
        self.assertTrue(
            apply_event(
                state,
                event(2, "task.pause_requested", "human", {"task_id": 1, "reason": "hold"}),
            )
        )
        self.assertTrue(apply_event(state, assigned(3, 1)))
        self.assertIsNone(state.tasks[1].assignment_id)
        self.assertEqual(1, state.tasks[1].last_assignment_attempt)
        pause = plan_next_emission(state, 101.0)
        self.assertTrue(apply_event(state, materialize(pause, 4)))
        self.assertTrue(
            apply_event(
                state,
                event(5, "task.resume_requested", "human", {"task_id": 1, "reason": "go"}),
            )
        )
        self.assertTrue(
            apply_event(state, materialize(plan_next_emission(state, 101.0), 6))
        )
        self.assertTrue(apply_event(state, registered(event_id=7)))
        next_assignment = plan_next_emission(state, 101.0)
        self.assertEqual("task:1:attempt:2", next_assignment["payload"]["assignment_id"])

    def test_retry_event_cannot_revive_failed_superseded_intent(self):
        state = PMState()
        events = (
            registered(),
            created(correlation_id="flow", max_retries=0),
            assigned(3, 1),
            event(
                4,
                "task.attempt_failed",
                "alice",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "worker_instance_id": "alice-1",
                    "failure_code": "permanent",
                    "reason": "stop",
                    "retryable": False,
                },
            ),
            event(
                5,
                "task.failed",
                "pm",
                {
                    "task_id": 1,
                    "reason_code": "permanent",
                    "reason": "stop",
                    "last_assignment_id": "task:1:attempt:1",
                    "attempts": 1,
                    "retryable_failures": 0,
                    "max_retries": 0,
                },
                caused_by=4,
            ),
            event(
                6,
                "task.created",
                "human",
                {
                    "task_id": 2,
                    "title": "replacement",
                    "supersedes_task_id": 1,
                    "supersession_reason": "new intent",
                },
                correlation_id="flow",
            ),
        )
        for item in events:
            self.assertTrue(apply_event(state, item))
        self.assertFalse(
            apply_event(
                state,
                event(
                    7,
                    "task.retry_requested",
                    "human",
                    {"task_id": 1, "additional_retries": 1, "reason": "revive"},
                    caused_by=5,
                ),
            )
        )
        self.assertEqual("failed", state.tasks[1].status)

    def test_deadline_explanation_precedes_pause(self):
        state = PMState()
        for item in (
            created(event_id=1, correlation_id="flow", deadline_at=105.0),
            event(2, "task.pause_requested", "human", {"task_id": 1, "reason": "hold"}),
        ):
            self.assertTrue(apply_event(state, item))
        pause = plan_next_emission(state, 101.0)
        self.assertTrue(apply_event(state, materialize(pause, 3)))
        explanation = explain_task(state, 1, now=106.0, lease_seconds=20)
        self.assertEqual("deadline_reconciliation_pending", explanation["code"])
        self.assertEqual("paused", explanation["details"]["control_status"])


class HardeningRuntimeTests(unittest.TestCase):
    def test_worker_ignores_assignment_inside_workflow_pause(self):
        class RecordingExecutor:
            def __init__(self):
                self.assignments = []

            def execute(self, assignment):
                self.assignments.append(assignment.assignment_id)
                return Completed("done")

        stale = assigned_event(attempt=1, event_id=11)
        current = assigned_event(attempt=2, event_id=13)
        events = (
            {
                "id": 10,
                "topic": "workflow.pause_requested",
                "correlation_id": "workflow-one",
                "payload": {"reason": "hold"},
            },
            stale,
            {
                "id": 12,
                "topic": "workflow.resumed",
                "correlation_id": "workflow-one",
                "payload": {"reason": "continue"},
            },
            current,
        )
        executor = RecordingExecutor()
        runtime_bus = RuntimeFakeBus(events)
        WorkerRuntime(
            runtime_bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        ).run()
        self.assertEqual(["task:1:attempt:2"], executor.assignments)


def materialize(planned, event_id):
    return event(
        event_id,
        planned["topic"],
        "pm",
        planned["payload"],
        caused_by=planned.get("caused_by"),
        correlation_id=planned.get("correlation_id"),
    )


if __name__ == "__main__":
    unittest.main()

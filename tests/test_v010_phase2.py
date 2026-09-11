import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import agent_bus_cli
import bus
from client import BusClient
from executors import Completed
from operations import explain_task, workflow_view
from pm_agent import PMState, plan_next_emission
from projection import apply_event
from runtime import WorkerRuntime
from tests.test_pm_agent import assigned, created, event, registered
from tests.test_runtime import FakeBus as RuntimeFakeBus, RuntimeTestCase, assigned_event


def materialize(planned, event_id, *, ts=101.0):
    return event(
        event_id,
        planned["topic"],
        "pm",
        planned["payload"],
        ts=ts,
        caused_by=planned.get("caused_by"),
        correlation_id=planned.get("correlation_id"),
    )


class PhaseTwoProjectionTests(unittest.TestCase):
    def test_active_task_pause_fences_late_result_and_resumes_next_attempt(self):
        state = PMState()
        for item in (
            registered(),
            created(correlation_id="flow"),
            assigned(3, 1),
            event(
                4,
                "task.pause_requested",
                "human",
                {"task_id": 1, "reason": "inspect intermediate state"},
                correlation_id="flow",
            ),
        ):
            self.assertTrue(apply_event(state, item))

        paused_plan = plan_next_emission(state, now=101.0)
        self.assertEqual("task.paused", paused_plan["topic"])
        paused = materialize(paused_plan, 5)
        self.assertTrue(apply_event(state, paused))
        self.assertFalse(apply_event(state, paused))
        self.assertEqual("paused", state.tasks[1].status)
        self.assertEqual(1, state.tasks[1].attempt)
        self.assertFalse(
            apply_event(
                state,
                event(
                    6,
                    "task.completed",
                    "alice",
                    {
                        "task_id": 1,
                        "assignment_id": "task:1:attempt:1",
                        "worker_instance_id": "alice-1",
                        "summary": "late",
                    },
                ),
            )
        )

        self.assertTrue(
            apply_event(
                state,
                event(
                    7,
                    "task.resume_requested",
                    "human",
                    {"task_id": 1, "reason": "continue"},
                    correlation_id="flow",
                ),
            )
        )
        resumed_plan = plan_next_emission(state, now=101.0)
        self.assertEqual("task.resumed", resumed_plan["topic"])
        self.assertTrue(apply_event(state, materialize(resumed_plan, 8)))
        self.assertEqual(4, state.tasks[1].pause_request_event_id)
        self.assertEqual(5, state.tasks[1].paused_event_id)
        self.assertEqual(7, state.tasks[1].resume_request_event_id)
        self.assertEqual(8, state.tasks[1].resumed_event_id)
        assignment = plan_next_emission(state, now=101.0)
        self.assertEqual("task.assigned", assignment["topic"])
        self.assertEqual(2, assignment["payload"]["attempt"])

    def test_blocked_pause_preserves_outstanding_human_decision(self):
        state = PMState()
        for item in (
            registered(),
            created(correlation_id="flow"),
            assigned(3, 1),
            event(
                4,
                "task.blocked",
                "alice",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "worker_instance_id": "alice-1",
                    "reason": "approval needed",
                },
            ),
            event(
                5,
                "decision.needed",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "decision_id": "decision:task:1:attempt:1",
                    "reason": "approval needed",
                },
            ),
            event(
                6,
                "task.pause_requested",
                "human",
                {"task_id": 1, "reason": "hold"},
                correlation_id="flow",
            ),
        ):
            self.assertTrue(apply_event(state, item))
        self.assertTrue(
            apply_event(state, materialize(plan_next_emission(state, 101.0), 7))
        )
        self.assertTrue(
            apply_event(
                state,
                event(
                    8,
                    "task.resume_requested",
                    "human",
                    {"task_id": 1, "reason": "continue waiting"},
                    correlation_id="flow",
                ),
            )
        )
        self.assertTrue(
            apply_event(state, materialize(plan_next_emission(state, 101.0), 9))
        )
        task = state.tasks[1]
        self.assertEqual("blocked", task.status)
        self.assertTrue(task.decision_needed)
        self.assertEqual("task:1:attempt:1", task.assignment_id)
        self.assertIsNone(plan_next_emission(state, 101.0))

    def test_workflow_pause_interrupts_active_work_and_gates_new_tasks(self):
        state = PMState()
        for item in (
            registered(),
            created(correlation_id="flow"),
            assigned(3, 1),
            event(
                4,
                "workflow.pause_requested",
                "human",
                {"reason": "maintenance"},
                correlation_id="flow",
            ),
        ):
            self.assertTrue(apply_event(state, item))
        self.assertFalse(
            apply_event(
                state,
                event(
                    5,
                    "task.completed",
                    "alice",
                    {
                        "task_id": 1,
                        "assignment_id": "task:1:attempt:1",
                        "worker_instance_id": "alice-1",
                    },
                ),
            )
        )
        paused_plan = plan_next_emission(state, 101.0)
        self.assertEqual(
            [{"task_id": 1, "assignment_id": "task:1:attempt:1"}],
            paused_plan["payload"]["interrupted_assignments"],
        )
        self.assertTrue(apply_event(state, materialize(paused_plan, 6)))
        self.assertTrue(apply_event(state, created(7, 2, correlation_id="flow")))
        self.assertIsNone(plan_next_emission(state, 101.0))
        view = workflow_view(state, "flow", now=101.0, lease_seconds=20)
        self.assertEqual("paused", view["status"])
        self.assertEqual("workflow_paused", view["tasks"][0]["effective_status"])

        self.assertTrue(
            apply_event(
                state,
                event(
                    8,
                    "workflow.resume_requested",
                    "human",
                    {"reason": "maintenance complete"},
                    correlation_id="flow",
                ),
            )
        )
        resumed = plan_next_emission(state, 101.0)
        self.assertEqual("workflow.resumed", resumed["topic"])
        self.assertTrue(apply_event(state, materialize(resumed, 9)))
        next_assignment = plan_next_emission(state, 101.0)
        self.assertEqual(1, next_assignment["payload"]["task_id"])
        self.assertEqual(2, next_assignment["payload"]["attempt"])

    def test_deadline_remains_authoritative_while_task_is_paused(self):
        state = PMState()
        for item in (
            created(correlation_id="flow", deadline_at=105.0),
            event(
                3,
                "task.pause_requested",
                "human",
                {"task_id": 1, "reason": "hold"},
                correlation_id="flow",
            ),
        ):
            self.assertTrue(apply_event(state, item))
        self.assertTrue(
            apply_event(state, materialize(plan_next_emission(state, 101.0), 4))
        )
        deadline = plan_next_emission(state, 106.0)
        self.assertEqual("task.deadline_exceeded", deadline["topic"])
        self.assertTrue(apply_event(state, materialize(deadline, 5, ts=106.0)))
        self.assertEqual("deadline_exceeded", state.tasks[1].status)

    def test_cancellation_remains_authoritative_while_task_is_paused(self):
        state = PMState()
        for item in (
            created(correlation_id="flow"),
            event(
                3,
                "task.pause_requested",
                "human",
                {"task_id": 1, "reason": "hold"},
                correlation_id="flow",
            ),
        ):
            self.assertTrue(apply_event(state, item))
        self.assertTrue(
            apply_event(state, materialize(plan_next_emission(state, 101.0), 4))
        )
        self.assertTrue(
            apply_event(
                state,
                event(
                    5,
                    "task.cancel_requested",
                    "human",
                    {"task_id": 1, "reason": "stop"},
                    correlation_id="flow",
                ),
            )
        )
        planned = plan_next_emission(state, 101.0)
        self.assertEqual("task.cancelled", planned["topic"])

    def test_supersession_is_immutable_fences_old_attempt_and_cascades(self):
        state = PMState()
        for item in (
            registered(),
            created(correlation_id="flow"),
            created(3, 3, correlation_id="flow", depends_on=(1,)),
            assigned(4, 1),
            event(
                5,
                "task.created",
                "human",
                {
                    "task_id": 2,
                    "title": "replacement",
                    "supersedes_task_id": 1,
                    "supersession_reason": "requirements changed",
                },
                correlation_id="flow",
            ),
        ):
            self.assertTrue(apply_event(state, item))
        self.assertEqual("supersession_requested", state.tasks[1].status)
        self.assertFalse(
            apply_event(
                state,
                event(
                    6,
                    "task.completed",
                    "alice",
                    {
                        "task_id": 1,
                        "assignment_id": "task:1:attempt:1",
                        "worker_instance_id": "alice-1",
                    },
                ),
            )
        )
        superseded = plan_next_emission(state, 101.0)
        self.assertEqual("task.superseded", superseded["topic"])
        self.assertTrue(apply_event(state, materialize(superseded, 7)))
        cascade = plan_next_emission(state, 101.0)
        self.assertEqual("task.dependency_failed", cascade["topic"])
        self.assertEqual(3, cascade["payload"]["task_id"])
        explanation = explain_task(state, 1, now=101.0, lease_seconds=20)
        self.assertEqual("superseded", explanation["code"])
        view = workflow_view(state, "flow", now=101.0, lease_seconds=20)
        self.assertEqual(
            [{
                "from_task_id": 1,
                "to_task_id": 2,
                "reason": "requirements changed",
            }],
            view["supersessions"],
        )

    def test_persisted_deadline_wins_when_replacement_arrives_after_cutoff(self):
        state = PMState()
        self.assertTrue(
            apply_event(
                state,
                created(
                    event_id=1,
                    task_id=1,
                    ts=100.0,
                    correlation_id="flow",
                    deadline_at=105.0,
                ),
            )
        )
        self.assertTrue(
            apply_event(
                state,
                event(
                    2,
                    "task.created",
                    "human",
                    {
                        "task_id": 2,
                        "title": "replacement",
                        "supersedes_task_id": 1,
                        "supersession_reason": "late revision",
                    },
                    ts=106.0,
                    correlation_id="flow",
                ),
            )
        )
        self.assertEqual("open", state.tasks[1].status)
        self.assertEqual(2, state.tasks[1].superseded_by_task_id)
        planned = plan_next_emission(state, now=106.0)
        self.assertEqual("task.deadline_exceeded", planned["topic"])


class PhaseTwoBusTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = bus.DB_PATH
        bus.DB_PATH = Path(self.temp_dir.name) / "events.db"
        bus.init_db()

    def tearDown(self):
        bus.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_controls_inherit_workflow_and_only_one_replacement_is_allowed(self):
        original = bus.append_event("task.created", "human", {"title": "old"})
        task_id = original["payload"]["task_id"]
        paused = bus.append_event(
            "task.pause_requested",
            "human",
            {"task_id": task_id, "reason": "hold"},
        )
        self.assertEqual(original["correlation_id"], paused["correlation_id"])
        retried = bus.append_event(
            "task.pause_requested",
            "human",
            {"task_id": task_id, "reason": "hold"},
            idempotency_key="pause-command",
        )
        retried_again = bus.append_event(
            "task.pause_requested",
            "human",
            {"task_id": task_id, "reason": "hold"},
            idempotency_key="pause-command",
        )
        self.assertEqual(retried["id"], retried_again["id"])
        workflow_pause = bus.append_event(
            "workflow.pause_requested",
            "human",
            {"reason": "hold all"},
            correlation_id=original["correlation_id"],
        )
        self.assertEqual(original["correlation_id"], workflow_pause["correlation_id"])
        replacement = bus.append_event(
            "task.created",
            "human",
            {
                "title": "new",
                "supersedes_task_id": task_id,
                "supersession_reason": "new intent",
            },
        )
        self.assertEqual(original["correlation_id"], replacement["correlation_id"])
        with self.assertRaisesRegex(bus.EventValidationError, "already has a replacement"):
            bus.append_event(
                "task.created",
                "human",
                {
                    "title": "another",
                    "supersedes_task_id": task_id,
                    "supersession_reason": "again",
                },
            )


class PhaseTwoRuntimeTests(RuntimeTestCase):
    def test_workflow_pause_revokes_executor_and_suppresses_late_result(self):
        began = threading.Event()
        release = threading.Event()

        class Executor:
            def __init__(self):
                self.cancelled = []

            def execute(self, assignment):
                began.set()
                release.wait(2)
                return Completed("too late")

            def cancel(self, assignment_id):
                self.cancelled.append(assignment_id)

        assignment = assigned_event()
        assignment["correlation_id"] = "flow"
        pause = {
            "id": 11,
            "topic": "workflow.pause_requested",
            "correlation_id": "flow",
            "payload": {"reason": "hold"},
        }

        def events():
            yield assignment
            self.assertTrue(began.wait(1))
            yield pause
            release.set()

        executor = Executor()
        fake_bus = RuntimeFakeBus()
        WorkerRuntime(
            fake_bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        ).run(events())
        self.assertEqual(["task:1:attempt:1"], executor.cancelled)
        self.assertNotIn("task.completed", [item["topic"] for item in fake_bus.published])

    def test_replacement_creation_revokes_superseded_attempt(self):
        began = threading.Event()
        release = threading.Event()

        class Executor:
            def __init__(self):
                self.cancelled = []

            def execute(self, assignment):
                began.set()
                release.wait(2)
                return Completed("too late")

            def cancel(self, assignment_id):
                self.cancelled.append(assignment_id)

        replacement = {
            "id": 11,
            "topic": "task.created",
            "correlation_id": "flow",
            "payload": {
                "task_id": 2,
                "title": "replacement",
                "supersedes_task_id": 1,
                "supersession_reason": "new intent",
            },
        }

        def events():
            assignment = assigned_event()
            assignment["correlation_id"] = "flow"
            yield assignment
            self.assertTrue(began.wait(1))
            yield replacement
            release.set()

        executor = Executor()
        fake_bus = RuntimeFakeBus()
        WorkerRuntime(
            fake_bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        ).run(events())
        self.assertEqual(["task:1:attempt:1"], executor.cancelled)
        self.assertNotIn("task.completed", [item["topic"] for item in fake_bus.published])


class PhaseTwoClientAndCliTests(unittest.TestCase):
    @patch("client.httpx.post")
    def test_client_helpers_publish_versioned_control_shapes(self, post):
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"id": 1}
        post.return_value = response
        with tempfile.TemporaryDirectory() as directory:
            client = BusClient(
                "http://bus",
                actor="human",
                offset_dir=Path(directory),
            )
            client.pause_task(3, reason="hold", idempotency_key="pause-3")
            body = post.call_args.kwargs["json"]
            self.assertEqual("task.pause_requested", body["topic"])
            self.assertEqual("pause-3", body["idempotency_key"])
            client.pause_workflow(
                "flow", reason="hold all", idempotency_key="pause-flow"
            )
            body = post.call_args.kwargs["json"]
            self.assertEqual("workflow.pause_requested", body["topic"])
            self.assertEqual("flow", body["correlation_id"])
            client.supersede_task(
                3,
                {"title": "new"},
                reason="changed",
                idempotency_key="replace-3",
            )
            body = post.call_args.kwargs["json"]
            self.assertEqual("task.created", body["topic"])
            self.assertEqual(3, body["payload"]["supersedes_task_id"])

    def test_cli_exposes_task_workflow_and_supersession_controls(self):
        parser = agent_bus_cli.build_parser()
        task_pause = parser.parse_args(
            ["pause", "task", "3", "--reason", "hold"]
        )
        self.assertEqual(("pause", "task", "3"), (
            task_pause.command, task_pause.scope, task_pause.target
        ))
        workflow_resume = parser.parse_args(
            ["resume", "workflow", "flow", "--reason", "continue"]
        )
        self.assertEqual("workflow", workflow_resume.scope)
        replacement = parser.parse_args(
            ["supersede", "3", "new intent", "--reason", "changed"]
        )
        self.assertEqual(3, replacement.task_id)


if __name__ == "__main__":
    unittest.main()

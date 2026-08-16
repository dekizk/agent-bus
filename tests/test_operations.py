import json
import unittest

import pm_agent
import projection
from operations import (
    ProjectionLookupError,
    build_projection,
    explain_task,
    summarize_telemetry,
    task_view,
    worker_views,
    workflow_view,
)
from projection import CoordinationProjection, TaskRecord, WorkerRecord


def event(event_id, topic, actor, payload, *, ts=100.0, caused_by=None, correlation_id="flow-1"):
    return {
        "id": event_id,
        "ts": ts,
        "topic": topic,
        "actor": actor,
        "payload": payload,
        "caused_by": caused_by,
        "correlation_id": correlation_id,
        "schema_version": 2,
        "idempotency_key": None,
    }


def completed_workflow_events():
    return [
        event(
            1,
            "agent.registered",
            "alice",
            {"name": "alice", "instance_id": "alice-1", "capacity": 1, "capabilities": ["python"]},
            correlation_id=None,
        ),
        event(
            2,
            "task.created",
            "human",
            {
                "task_id": 1,
                "title": "Produce",
                "retry_policy": {"max_retries": 2},
                "required_capabilities": ["python"],
            },
        ),
        event(
            3,
            "task.created",
            "human",
            {"task_id": 2, "title": "Consume", "depends_on": [1]},
        ),
        event(
            4,
            "task.assigned",
            "pm",
            {
                "task_id": 1,
                "assignment_id": "task:1:attempt:1",
                "attempt": 1,
                "assignee": "alice",
                "worker_instance_id": "alice-1",
                "dependency_refs": [],
            },
        ),
        event(
            5,
            "task.started",
            "alice",
            {"task_id": 1, "assignment_id": "task:1:attempt:1", "worker_instance_id": "alice-1"},
        ),
        event(
            6,
            "task.completed",
            "alice",
            {
                "task_id": 1,
                "assignment_id": "task:1:attempt:1",
                "worker_instance_id": "alice-1",
                "summary": "produced",
                "result": {"value": 42},
            },
        ),
        event(
            7,
            "task.assigned",
            "pm",
            {
                "task_id": 2,
                "assignment_id": "task:2:attempt:1",
                "attempt": 1,
                "assignee": "alice",
                "worker_instance_id": "alice-1",
                "dependency_refs": [{"task_id": 1, "completion_event_id": 6}],
            },
        ),
        event(
            8,
            "task.completed",
            "alice",
            {
                "task_id": 2,
                "assignment_id": "task:2:attempt:1",
                "worker_instance_id": "alice-1",
                "summary": "consumed",
                "result": {"answer": 84},
            },
        ),
    ]


def terminal_model(event_id, assignment_id, invocation_id, *, topic="telemetry.model.completed", tokens=10, cost=0.01):
    return event(
        event_id,
        topic,
        "alice",
        {
            "task_id": int(assignment_id.split(":")[1]),
            "assignment_id": assignment_id,
            "worker_instance_id": "alice-1",
            "invocation_id": invocation_id,
            "provider": "provider",
            "model": "model",
            "duration_ms": 125,
            "usage": {
                "input_tokens": tokens - 2,
                "output_tokens": 2,
                "total_tokens": tokens,
                "estimated_cost_usd": cost,
            },
            **({"error_code": "timeout", "retryable": True} if topic.endswith(".failed") else {}),
        },
    )


class SharedProjectionTests(unittest.TestCase):
    def test_pm_reexports_the_single_shared_projection(self):
        self.assertIs(pm_agent.PMState, projection.PMState)
        self.assertIs(pm_agent.apply_event, projection.apply_event)
        self.assertEqual(pm_agent.PM_TOPICS, projection.PROJECTION_TOPICS)

    def test_replay_retains_exact_current_state_event_and_summary(self):
        state = build_projection(completed_workflow_events())
        self.assertEqual("completed", state.tasks[1].status)
        self.assertEqual(6, state.tasks[1].status_event_id)
        self.assertEqual("produced", state.tasks[1].completion_summary)
        self.assertEqual(8, state.tasks[2].status_event_id)
        self.assertFalse(
            task_view(state, 1, now=101, lease_seconds=20)["assignment_active"]
        )


class ExplanationTests(unittest.TestCase):
    def state_with(self, status, **fields):
        state = CoordinationProjection()
        values = {
            "task_id": 1,
            "title": "demo",
            "correlation_id": "flow-1",
            "status": status,
            "created_event_id": 1,
            "status_event_id": 10,
            **fields,
        }
        state.tasks[1] = TaskRecord(**values)
        return state

    def test_every_current_lifecycle_state_has_a_specific_explanation(self):
        cases = {
            "open": (self.state_with("open"), "no_active_workers"),
            "assigned": (
                self.state_with(
                    "assigned",
                    assignee="alice",
                    worker_instance_id="alice-1",
                    assignment_id="task:1:attempt:1",
                    assignment_event_id=9,
                ),
                "active_lease_healthy",
            ),
            "started": (
                self.state_with(
                    "started",
                    assignee="alice",
                    worker_instance_id="alice-1",
                    assignment_id="task:1:attempt:1",
                    assignment_event_id=9,
                ),
                "active_lease_healthy",
            ),
            "blocked": (
                self.state_with(
                    "blocked",
                    block_event_id=10,
                    block_reason="approval required",
                    decision_id="decision:1",
                    decision_needed=True,
                    decision_event_id=11,
                ),
                "human_decision_required",
            ),
            "cancellation_requested": (
                self.state_with(
                    "cancellation_requested",
                    cancel_request_event_id=10,
                    cancel_reason="operator request",
                ),
                "cancellation_pending",
            ),
            "completed": (
                self.state_with("completed", completion_event_id=10),
                "completed",
            ),
            "failed": (
                self.state_with(
                    "failed",
                    failed_event_id=10,
                    last_failure_event_id=9,
                    last_failure_code="invalid_input",
                    last_failure_reason="bad input",
                ),
                "failed",
            ),
            "dependency_failed": (
                self.state_with(
                    "dependency_failed",
                    dependency_failed_event_id=10,
                    dependency_failure_task_id=2,
                    dependency_failure_event_id=8,
                    dependency_failure_reason="dependency task 2 failed",
                ),
                "dependency_failed",
            ),
            "cancelled": (
                self.state_with(
                    "cancelled",
                    cancelled_event_id=10,
                    cancel_request_event_id=9,
                    cancel_reason="operator request",
                ),
                "cancelled",
            ),
            "deadline_exceeded": (
                self.state_with(
                    "deadline_exceeded",
                    deadline_exceeded_event_id=10,
                    deadline_at=99,
                ),
                "deadline_exceeded",
            ),
        }
        for status, (state, expected) in cases.items():
            if status in {"assigned", "started"}:
                state.workers["alice"] = WorkerRecord(
                    "alice", "alice-1", 100, last_event_id=12
                )
            with self.subTest(status=status):
                value = explain_task(state, 1, now=101, lease_seconds=20)
                self.assertEqual(expected, value["code"])
                self.assertTrue(value["summary"])
                self.assertTrue(value["event_ids"])

    def test_open_reasons_distinguish_dependencies_capabilities_and_capacity(self):
        state = CoordinationProjection()
        state.tasks[1] = TaskRecord(1, "upstream", status="started", status_event_id=4)
        state.tasks[2] = TaskRecord(
            2,
            "dependent",
            status="open",
            status_event_id=2,
            depends_on=(1,),
        )
        self.assertEqual(
            "dependencies_incomplete",
            explain_task(state, 2, now=101, lease_seconds=20)["code"],
        )

        state.tasks[2].depends_on = ()
        state.tasks[2].required_capabilities = frozenset({"gpu"})
        state.workers["alice"] = WorkerRecord(
            "alice", "alice-1", 100, capabilities=frozenset({"python"}), last_event_id=3
        )
        self.assertEqual(
            "capabilities_unavailable",
            explain_task(state, 2, now=101, lease_seconds=20)["code"],
        )

        state.tasks[2].required_capabilities = frozenset({"python"})
        state.tasks[1].assignee = "alice"
        state.tasks[1].worker_instance_id = "alice-1"
        self.assertEqual(
            "workers_at_capacity",
            explain_task(state, 2, now=101, lease_seconds=20)["code"],
        )

    def test_lookup_errors_are_actionable(self):
        with self.assertRaisesRegex(ProjectionLookupError, "task 9 was not found"):
            task_view(CoordinationProjection(), 9, now=1, lease_seconds=20)


class WorkflowViewTests(unittest.TestCase):
    def test_two_task_dag_and_usage_are_understandable_without_raw_rows(self):
        state = build_projection(completed_workflow_events())
        telemetry = [
            event(
                20,
                "telemetry.model.started",
                "alice",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "worker_instance_id": "alice-1",
                    "invocation_id": "model-1",
                    "provider": "provider",
                    "model": "model",
                },
            ),
            terminal_model(21, "task:1:attempt:1", "model-1", tokens=10, cost=0.01),
            terminal_model(22, "task:2:attempt:1", "model-2", topic="telemetry.model.failed", tokens=15, cost=0.02),
        ]
        value = workflow_view(
            state,
            "flow-1",
            now=101,
            lease_seconds=20,
            telemetry_events=telemetry,
        )
        self.assertEqual("completed", value["status"])
        self.assertEqual([{"from_task_id": 1, "to_task_id": 2}], value["edges"])
        self.assertEqual(25, value["telemetry"]["usage"]["total_tokens"])
        self.assertEqual(0.03, value["telemetry"]["usage"]["cost_usd"])
        self.assertEqual(1, value["telemetry"]["model"]["failed"])
        json.dumps(value)

    def test_duplicate_terminal_identity_is_counted_once(self):
        events = [
            terminal_model(4, "task:1:attempt:1", "same", tokens=10),
            terminal_model(5, "task:1:attempt:1", "same", tokens=20),
        ]
        value = summarize_telemetry(events)
        self.assertEqual(1, value["model"]["completed"])
        self.assertEqual(20, value["usage"]["total_tokens"])

    def test_worker_view_traces_lease_health_to_last_event(self):
        state = CoordinationProjection()
        state.workers["alice"] = WorkerRecord("alice", "a-1", 50, last_event_id=7)
        value = worker_views(state, now=100, lease_seconds=20)[0]
        self.assertEqual("stale", value["status"])
        self.assertEqual(7, value["last_event_id"])


if __name__ == "__main__":
    unittest.main()

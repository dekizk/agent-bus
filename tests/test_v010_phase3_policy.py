import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import agent_bus_cli
import bus
from local_config import LocalConfig, initialize_local_config
from operations import explain_task, workflow_view
from pm_agent import OrderedProjectionCursor, PM_TOPICS, PMState, reconcile
from projection import replay_events


class DirectPMBus:
    def __init__(self, before_publish=None):
        self.before_publish = before_publish

    def publish(self, topic, payload, **kwargs):
        if self.before_publish is not None:
            self.before_publish(topic, payload)
        return bus.append_event(topic, "pm", payload, **kwargs)

    def query_all(self, *, after_id=0, topics=None):
        return bus.fetch_after(after_id, topics)


class WorkflowPolicyDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = bus.DB_PATH
        bus.DB_PATH = Path(self.temp_dir.name) / "events.db"
        bus.init_db()

    def tearDown(self):
        bus.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_default_is_materialized_before_assignment_and_survives_restart(self):
        worker = self._register(capacity=2)
        root = bus.append_event(
            "task.created",
            "human",
            {"title": "first"},
            correlation_id="default-flow",
        )
        state, cursor = self._projection()

        emitted = reconcile(
            state,
            DirectPMBus(),
            now=worker["ts"],
            cursor=cursor,
            default_workflow_max_active_assignments=1,
        )

        self.assertEqual(
            ["workflow.policy_set", "task.assigned"],
            [item["topic"] for item in emitted],
        )
        policy = emitted[0]
        assignment = emitted[1]
        self.assertEqual(root["id"], policy["caused_by"])
        self.assertEqual(1, policy["payload"]["max_active_assignments"])
        self.assertEqual(policy["id"], assignment["payload"]["workflow_policy_event_id"])
        self.assertEqual(policy["id"], state.tasks[1].assignment_policy_event_id)

        restarted = replay_events(bus.fetch_after(0, list(PM_TOPICS)))
        restart_cursor = OrderedProjectionCursor(
            restarted,
            last_event_id=max(item["id"] for item in bus.fetch_after(0, list(PM_TOPICS))),
        )
        self.assertEqual(
            [],
            reconcile(
                restarted,
                DirectPMBus(),
                now=worker["ts"],
                cursor=restart_cursor,
                default_workflow_max_active_assignments=9,
            ),
        )
        self.assertEqual(1, restarted.workflows["default-flow"].max_active_assignments)

    def test_operator_policy_wins_when_it_crosses_default_publication(self):
        worker = self._register(capacity=2)
        bus.append_event(
            "task.created",
            "human",
            {"title": "race"},
            correlation_id="policy-race",
        )
        state, cursor = self._projection()
        injected = False

        def race(topic, payload):
            nonlocal injected
            if topic == "workflow.policy_set" and not injected:
                injected = True
                bus.append_event(
                    "workflow.policy_set",
                    "human",
                    {
                        "source": "operator",
                        "reason": "operator chose a stricter limit",
                        "max_active_assignments": 1,
                    },
                    correlation_id="policy-race",
                    idempotency_key="policy-race:operator",
                )

        emitted = reconcile(
            state,
            DirectPMBus(race),
            now=worker["ts"],
            cursor=cursor,
            default_workflow_max_active_assignments=4,
        )
        policies = [
            item for item in bus.fetch_after(0, None)
            if item["topic"] == "workflow.policy_set"
        ]

        self.assertEqual(2, len(policies))
        operator_policy, stale_default = policies
        self.assertEqual("operator", operator_policy["payload"]["source"])
        self.assertEqual("default", stale_default["payload"]["source"])
        self.assertEqual(operator_policy["id"], state.workflows["policy-race"].policy_event_id)
        assignment = next(item for item in emitted if item["topic"] == "task.assigned")
        self.assertEqual(
            operator_policy["id"],
            assignment["payload"]["workflow_policy_event_id"],
        )

        replayed = replay_events(bus.fetch_after(0, list(PM_TOPICS)))
        self.assertEqual(
            operator_policy["id"], replayed.workflows["policy-race"].policy_event_id
        )

    def test_late_default_for_reopened_historical_workflow_uses_original_root(self):
        worker = self._register(capacity=2)
        original = bus.append_event(
            "task.created",
            "human",
            {"title": "historical root"},
            correlation_id="upgraded-flow",
        )
        state, cursor = self._projection()
        reconcile(state, DirectPMBus(), now=worker["ts"], cursor=cursor)
        self._complete(state, cursor, original["payload"]["task_id"])
        replacement = bus.append_event(
            "task.created",
            "human",
            {"title": "new work after upgrade"},
            correlation_id="upgraded-flow",
        )
        cursor.catch_up(DirectPMBus())

        emitted = reconcile(
            state,
            DirectPMBus(),
            now=worker["ts"],
            cursor=cursor,
            default_workflow_max_active_assignments=1,
        )
        self.assertEqual(
            ["workflow.policy_set", "task.assigned"],
            [item["topic"] for item in emitted],
        )
        self.assertEqual(original["id"], emitted[0]["caused_by"])
        self.assertEqual(replacement["payload"]["task_id"], emitted[1]["payload"]["task_id"])

    def test_lower_limit_does_not_revoke_work_and_blocks_until_below_limit(self):
        worker = self._register(capacity=3)
        task_ids = []
        for title in ("one", "two", "three"):
            created = bus.append_event(
                "task.created",
                "human",
                {"title": title},
                correlation_id="limit-flow",
            )
            task_ids.append(created["payload"]["task_id"])
        state, cursor = self._projection()
        pm_bus = DirectPMBus()
        reconcile(
            state,
            pm_bus,
            now=worker["ts"],
            cursor=cursor,
            default_workflow_max_active_assignments=2,
        )
        self.assertEqual(2, len(state.workflow_active_tasks("limit-flow")))

        lowered = bus.append_event(
            "workflow.policy_set",
            "human",
            {
                "source": "operator",
                "reason": "reduce resource use",
                "max_active_assignments": 1,
            },
            correlation_id="limit-flow",
        )
        cursor.catch_up(pm_bus)
        self.assertEqual([], reconcile(state, pm_bus, now=worker["ts"], cursor=cursor))
        self.assertEqual("assigned", state.tasks[task_ids[0]].status)
        self.assertEqual("assigned", state.tasks[task_ids[1]].status)
        self.assertEqual("open", state.tasks[task_ids[2]].status)

        explanation = explain_task(
            state, task_ids[2], now=worker["ts"], lease_seconds=20
        )
        self.assertEqual("workflow_concurrency_limit", explanation["code"])
        self.assertEqual(lowered["id"], explanation["details"]["policy_event_id"])
        view = workflow_view(
            state, "limit-flow", now=worker["ts"], lease_seconds=20
        )
        self.assertEqual(lowered["id"], view["policy"]["event_id"])
        self.assertEqual(1, view["policy"]["max_active_assignments"])

        self._complete(state, cursor, task_ids[0])
        self.assertEqual([], reconcile(state, pm_bus, now=worker["ts"], cursor=cursor))
        self.assertEqual("open", state.tasks[task_ids[2]].status)

        self._complete(state, cursor, task_ids[1])
        emitted = reconcile(state, pm_bus, now=worker["ts"], cursor=cursor)
        self.assertEqual(["task.assigned"], [item["topic"] for item in emitted])
        self.assertEqual(task_ids[2], emitted[0]["payload"]["task_id"])
        self.assertEqual(lowered["id"], emitted[0]["payload"]["workflow_policy_event_id"])

    def test_assignment_planned_under_replaced_policy_is_fenced_and_reissued(self):
        worker = self._register(capacity=2)
        bus.append_event(
            "task.created",
            "human",
            {"title": "stale policy plan"},
            correlation_id="stale-policy",
        )
        original = bus.append_event(
            "workflow.policy_set",
            "human",
            {
                "source": "operator",
                "reason": "initial",
                "max_active_assignments": 2,
            },
            correlation_id="stale-policy",
        )
        state, cursor = self._projection()
        injected = False

        def race(topic, payload):
            nonlocal injected
            if topic == "task.assigned" and not injected:
                injected = True
                bus.append_event(
                    "workflow.policy_set",
                    "human",
                    {
                        "source": "operator",
                        "reason": "changed before assignment publication",
                        "max_active_assignments": 1,
                    },
                    correlation_id="stale-policy",
                )

        emitted = reconcile(
            state,
            DirectPMBus(race),
            now=worker["ts"],
            cursor=cursor,
        )
        assignments = [item for item in emitted if item["topic"] == "task.assigned"]
        self.assertEqual(2, len(assignments))
        self.assertEqual(original["id"], assignments[0]["payload"]["workflow_policy_event_id"])
        self.assertEqual("task:1:attempt:1", assignments[0]["payload"]["assignment_id"])
        self.assertEqual("task:1:attempt:2", assignments[1]["payload"]["assignment_id"])
        self.assertEqual(
            state.workflows["stale-policy"].policy_event_id,
            assignments[1]["payload"]["workflow_policy_event_id"],
        )
        self.assertEqual("task:1:attempt:2", state.tasks[1].assignment_id)
        self.assertEqual(2, state.tasks[1].attempt)
        self.assertEqual(2, state.tasks[1].last_assignment_attempt)

        replayed = replay_events(bus.fetch_after(0, list(PM_TOPICS)))
        self.assertEqual(state.tasks[1].assignment_id, replayed.tasks[1].assignment_id)
        self.assertEqual(
            state.tasks[1].assignment_policy_event_id,
            replayed.tasks[1].assignment_policy_event_id,
        )

    def test_policy_event_validation_and_unknown_workflow_rejection(self):
        bus.append_event(
            "task.created",
            "human",
            {"title": "known"},
            correlation_id="known-flow",
        )
        valid = {
            "source": "operator",
            "reason": "bound fan-out",
            "max_active_assignments": 2,
        }
        first = bus.append_event(
            "workflow.policy_set",
            "human",
            valid,
            correlation_id="known-flow",
            idempotency_key="policy:known-flow:2",
        )
        duplicate = bus.append_event(
            "workflow.policy_set",
            "human",
            valid,
            correlation_id="known-flow",
            idempotency_key="policy:known-flow:2",
        )
        self.assertEqual(first["id"], duplicate["id"])
        bus.append_event(
            "workflow.policy_set",
            "human",
            {**valid, "max_active_assignments": None, "reason": "unbounded"},
            correlation_id="known-flow",
        )
        invalid = (
            ("human", {**valid, "source": "default"}),
            ("pm", valid),
            ("human", {**valid, "max_active_assignments": 0}),
            ("human", {**valid, "reason": ""}),
        )
        for actor, payload in invalid:
            with self.subTest(actor=actor, payload=payload):
                with self.assertRaises(bus.EventValidationError):
                    bus.append_event(
                        "workflow.policy_set",
                        actor,
                        payload,
                        correlation_id="known-flow",
                    )
        with self.assertRaisesRegex(bus.EventValidationError, "does not exist"):
            bus.append_event(
                "workflow.policy_set",
                "human",
                valid,
                correlation_id="missing-flow",
            )

    def _register(self, *, capacity):
        return bus.append_event(
            "agent.registered",
            "alice",
            {
                "name": "alice",
                "instance_id": "alice-1",
                "capacity": capacity,
                "capabilities": [],
            },
        )

    def _projection(self):
        state = PMState()
        cursor = OrderedProjectionCursor(state)
        cursor.consume(bus.fetch_after(0, list(PM_TOPICS)))
        return state, cursor

    def _complete(self, state, cursor, task_id):
        task = state.tasks[task_id]
        bus.append_event(
            "task.completed",
            "alice",
            {
                "task_id": task_id,
                "assignment_id": task.assignment_id,
                "worker_instance_id": task.worker_instance_id,
                "summary": "done",
                "result": {},
            },
        )
        cursor.catch_up(DirectPMBus())


class WorkflowPolicyLocalSurfaceTests(unittest.TestCase):
    def test_new_and_legacy_local_configs_have_stable_defaults(self):
        with tempfile.TemporaryDirectory() as directory:
            path = initialize_local_config(directory)
            configured = LocalConfig.from_file(path)
            self.assertEqual(
                4,
                configured.default_workflow_max_active_assignments,
            )
            with patch.dict(os.environ, {}, clear=True):
                configured.apply_environment()
                self.assertEqual(
                    "4",
                    os.environ[
                        "AGENT_BUS_DEFAULT_WORKFLOW_MAX_ACTIVE_ASSIGNMENTS"
                    ],
                )
            value = json.loads(path.read_text(encoding="utf-8"))
            value.pop("default_workflow_max_active_assignments")
            path.write_text(json.dumps(value), encoding="utf-8")
            legacy = LocalConfig.from_file(path)
            self.assertIsNone(legacy.default_workflow_max_active_assignments)
            with patch.dict(os.environ, {}, clear=True):
                legacy.apply_environment()
                self.assertEqual(
                    "unbounded",
                    os.environ[
                        "AGENT_BUS_DEFAULT_WORKFLOW_MAX_ACTIVE_ASSIGNMENTS"
                    ],
                )

    def test_cli_policy_command_calls_typed_client_helper(self):
        with tempfile.TemporaryDirectory() as directory:
            config = initialize_local_config(directory)
            event = {
                "id": 9,
                "topic": "workflow.policy_set",
                "correlation_id": "flow",
                "payload": {"max_active_assignments": 2},
            }
            with patch("client.BusClient") as client_type:
                client_type.return_value.set_workflow_policy.return_value = event
                stdout = io.StringIO()
                stderr = io.StringIO()
                code = agent_bus_cli.main(
                    [
                        "policy",
                        "flow",
                        "--max-active-assignments",
                        "2",
                        "--reason",
                        "bound fan-out",
                        "--config",
                        str(config),
                    ],
                    stdout=stdout,
                    stderr=stderr,
                )
            self.assertEqual(0, code, stderr.getvalue())
            client_type.return_value.set_workflow_policy.assert_called_once_with(
                "flow",
                max_active_assignments=2,
                reason="bound fan-out",
                idempotency_key=None,
            )
            self.assertIn("event #9", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()

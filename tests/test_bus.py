import sqlite3
import tempfile
import unittest
from pathlib import Path

import bus
from fastapi.testclient import TestClient


class BusStorageTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = bus.DB_PATH
        bus.DB_PATH = Path(self.temp_dir.name) / "events.db"
        bus.init_db()

    def tearDown(self):
        bus.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_server_task_id_is_stable_across_idempotent_retry(self):
        first = bus.append_event(
            "task.created",
            "human",
            {"title": "crash-safe task"},
            idempotency_key="create:one",
        )
        retried = bus.append_event(
            "task.created",
            "human",
            {"title": "crash-safe task"},
            idempotency_key="create:one",
        )

        self.assertEqual(first["id"], retried["id"])
        self.assertEqual(first["payload"]["task_id"], retried["payload"]["task_id"])
        self.assertEqual(first["correlation_id"], retried["correlation_id"])
        self.assertIsNotNone(first["correlation_id"])
        self.assertEqual(
            {"max_retries": bus.DEFAULT_MAX_RETRIES},
            first["payload"]["retry_policy"],
        )
        self.assertEqual(
            {"mode": "controlled", "owner": "agent-bus"},
            first["payload"]["ownership"],
        )
        self.assertEqual(1, len(bus.fetch_after(0, None)))

        with self.assertRaises(bus.IdempotencyConflict):
            bus.append_event(
                "task.created",
                "human",
                {"title": "crash-safe task"},
                idempotency_key="create:one",
                correlation_id="different-workflow",
            )

    def test_task_retry_policy_is_persisted_and_validated(self):
        explicit = bus.append_event(
            "task.created",
            "human",
            {
                "title": "no automatic retries",
                "retry_policy": {"max_retries": 0},
            },
        )
        self.assertEqual(
            {"max_retries": 0},
            explicit["payload"]["retry_policy"],
        )

        for invalid_policy in (
            {"max_retries": -1},
            {"max_retries": True},
            {"max_retries": 1, "other": 2},
            {},
            None,
        ):
            with self.subTest(retry_policy=invalid_policy):
                with self.assertRaises(bus.EventValidationError):
                    bus.append_event(
                        "task.created",
                        "human",
                        {
                            "title": "invalid",
                            "retry_policy": invalid_policy,
                        },
                    )

    def test_dependencies_are_existing_acyclic_same_workflow_edges(self):
        root = bus.append_event(
            "task.created",
            "human",
            {"title": "root"},
            correlation_id="workflow-one",
        )
        dependent = bus.append_event(
            "task.created",
            "human",
            {
                "title": "dependent",
                "depends_on": [root["payload"]["task_id"]],
            },
        )

        self.assertEqual("workflow-one", dependent["correlation_id"])
        self.assertEqual([root["payload"]["task_id"]], dependent["payload"]["depends_on"])

        with self.assertRaisesRegex(bus.EventValidationError, "does not exist"):
            bus.append_event(
                "task.created",
                "human",
                {"title": "forward edge", "depends_on": [999]},
            )
        with self.assertRaisesRegex(bus.EventValidationError, "duplicates"):
            bus.append_event(
                "task.created",
                "human",
                {"title": "duplicate edge", "depends_on": [1, 1]},
            )
        with self.assertRaisesRegex(bus.EventValidationError, "at most"):
            bus.append_event(
                "task.created",
                "human",
                {
                    "title": "too much fan-in",
                    "depends_on": list(range(1, bus.MAX_TASK_DEPENDENCIES + 2)),
                },
            )

        other = bus.append_event(
            "task.created",
            "human",
            {"title": "other"},
            correlation_id="workflow-two",
        )
        with self.assertRaisesRegex(bus.EventValidationError, "same correlation_id"):
            bus.append_event(
                "task.created",
                "human",
                {
                    "title": "cross-workflow fan-in",
                    "depends_on": [
                        root["payload"]["task_id"],
                        other["payload"]["task_id"],
                    ],
                },
            )

    def test_dependency_reference_and_failure_contracts(self):
        assignment = {
            "task_id": 2,
            "assignment_id": "task:2:attempt:1",
            "attempt": 1,
            "assignee": "alice",
            "worker_instance_id": "alice-1",
            "dependency_refs": [
                {"task_id": 1, "completion_event_id": 10},
            ],
        }
        bus.validate_event("task.assigned", "pm", assignment, 10, None, 2)
        with self.assertRaises(bus.EventValidationError):
            bus.validate_event(
                "task.assigned",
                "pm",
                {**assignment, "dependency_refs": [{"task_id": 1}]},
                10,
                None,
                2,
            )

        failure = {
            "task_id": 2,
            "dependency_task_id": 1,
            "dependency_event_id": 11,
            "reason": "dependency task 1 failed",
        }
        bus.validate_event("task.dependency_failed", "pm", failure, 11, None, 2)
        with self.assertRaises(bus.EventValidationError):
            bus.validate_event(
                "task.dependency_failed", "worker", failure, 11, None, 2
            )

    def test_integration_context_and_ownership_contracts(self):
        controlled = bus.append_event(
            "task.created",
            "bridge",
            {
                "title": "Imported task",
                "context": {"repository": "agent-bus"},
                "external_origin": {
                    "system": "legacy",
                    "task_ref": "work-1",
                },
                "ownership": {
                    "mode": "canary",
                    "owner": "agent-bus",
                },
            },
        )
        self.assertEqual("work-1", controlled["payload"]["external_origin"]["task_ref"])

        observed = bus.append_event(
            "integration.task_observed",
            "bridge",
            {
                "title": "External task",
                "context": {},
                "external_origin": {
                    "system": "legacy",
                    "task_ref": "work-2",
                },
                "ownership": {
                    "mode": "shadow",
                    "owner": "external",
                },
            },
            correlation_id="external-work-2",
        )
        self.assertEqual("external", observed["payload"]["ownership"]["owner"])
        self.assertNotIn("task_id", observed["payload"])

        invalid_payloads = (
            {
                "title": "bad context",
                "context": ["not", "an", "object"],
            },
            {
                "title": "too large",
                "context": {"value": "x" * bus.MAX_INLINE_CONTEXT_BYTES},
            },
            {
                "title": "wrong owner",
                "ownership": {"mode": "shadow", "owner": "external"},
            },
            {
                "title": "missing ownership",
                "ownership": None,
            },
        )
        for payload in invalid_payloads:
            with self.subTest(payload=payload["title"]):
                with self.assertRaises(bus.EventValidationError):
                    bus.append_event("task.created", "bridge", payload)

    def test_failure_and_retry_event_contracts(self):
        valid_attempt_failure = {
            "task_id": 1,
            "assignment_id": "task:1:attempt:1",
            "worker_instance_id": "alice-1",
            "failure_code": "tool_timeout",
            "reason": "tool did not return",
            "retryable": True,
        }
        bus.validate_event(
            "task.attempt_failed",
            "alice",
            valid_attempt_failure,
            None,
            None,
            2,
        )

        with self.assertRaises(bus.EventValidationError):
            bus.validate_event(
                "task.attempt_failed",
                "alice",
                {**valid_attempt_failure, "retryable": "yes"},
                None,
                None,
                2,
            )
        with self.assertRaises(bus.EventValidationError):
            bus.validate_event(
                "task.retry_requested",
                "human",
                {"task_id": 1, "additional_retries": 0, "reason": "try again"},
                None,
                None,
                2,
            )

    def test_assignment_decision_history_contract(self):
        payload = {
            "task_id": 1,
            "assignment_id": "task:1:attempt:2",
            "attempt": 2,
            "assignee": "alice",
            "worker_instance_id": "alice-1",
            "decisions": [
                {
                    "event_id": 8,
                    "actor": "human",
                    "assignment_id": "task:1:attempt:1",
                    "decision_id": "decision:task:1:attempt:1",
                    "decision": {"release_target": "staging"},
                }
            ],
        }
        bus.validate_event("task.assigned", "pm", payload, None, None, 2)

        for invalid in (
            {},
            [{"event_id": 8}],
            [
                {
                    "event_id": 0,
                    "actor": "human",
                    "assignment_id": "task:1:attempt:1",
                    "decision_id": "decision:task:1:attempt:1",
                    "decision": "staging",
                }
            ],
        ):
            with self.subTest(decisions=invalid):
                with self.assertRaises(bus.EventValidationError):
                    bus.validate_event(
                        "task.assigned",
                        "pm",
                        {**payload, "decisions": invalid},
                        None,
                        None,
                        2,
                    )

    def test_reusing_idempotency_key_for_different_request_is_conflict(self):
        bus.append_event(
            "task.created",
            "human",
            {"title": "first"},
            idempotency_key="create:one",
        )
        with self.assertRaises(bus.IdempotencyConflict):
            bus.append_event(
                "task.created",
                "human",
                {"title": "different"},
                idempotency_key="create:one",
            )

    def test_v2_known_event_contract_rejects_missing_attempt_identity(self):
        with self.assertRaises(bus.EventValidationError):
            bus.append_event(
                "task.completed",
                "alice",
                {"task_id": 1},
            )

    def test_worker_completion_retry_appends_one_event(self):
        parent = bus.append_event(
            "custom.assignment",
            "pm",
            {},
            correlation_id="workflow-one",
        )
        payload = {
            "task_id": 1,
            "assignment_id": "task:1:attempt:1",
            "worker_instance_id": "alice-1",
            "summary": "done",
        }
        first = bus.append_event(
            "task.completed",
            "alice",
            payload,
            caused_by=parent["id"],
            idempotency_key="completed:task:1:attempt:1",
        )
        second = bus.append_event(
            "task.completed",
            "alice",
            payload,
            caused_by=parent["id"],
            idempotency_key="completed:task:1:attempt:1",
        )
        self.assertEqual(first["id"], second["id"])
        self.assertEqual("workflow-one", first["correlation_id"])
        completed = bus.fetch_after(0, ["task.completed"])
        self.assertEqual(1, len(completed))

    def test_correlation_is_generated_inherited_and_cannot_conflict(self):
        root = bus.append_event("task.created", "human", {"title": "workflow"})
        child = bus.append_event(
            "custom.child",
            "agent",
            {},
            caused_by=root["id"],
        )
        explicit_child = bus.append_event(
            "custom.explicit-child",
            "agent",
            {},
            caused_by=child["id"],
            correlation_id=root["correlation_id"],
        )
        sibling_task = bus.append_event(
            "task.created",
            "human",
            {"title": "another task in the workflow"},
            correlation_id=root["correlation_id"],
        )

        self.assertEqual(root["correlation_id"], child["correlation_id"])
        self.assertEqual(root["correlation_id"], explicit_child["correlation_id"])
        self.assertEqual(root["correlation_id"], sibling_task["correlation_id"])
        self.assertNotEqual(
            root["payload"]["task_id"],
            sibling_task["payload"]["task_id"],
        )

        with self.assertRaisesRegex(bus.EventValidationError, "conflicts"):
            bus.append_event(
                "custom.bad-child",
                "agent",
                {},
                caused_by=root["id"],
                correlation_id="different-workflow",
            )

    def test_new_event_rejects_nonexistent_causal_parent(self):
        with self.assertRaisesRegex(bus.EventValidationError, "does not exist"):
            bus.append_event(
                "custom.orphan",
                "agent",
                {},
                caused_by=999,
            )

    def test_correlation_filters_history_and_stream_windows(self):
        first = bus.append_event(
            "custom.tick",
            "agent",
            {"n": 1},
            correlation_id="workflow-a",
        )
        bus.append_event(
            "custom.tick",
            "agent",
            {"n": 2},
            correlation_id="workflow-b",
        )
        third = bus.append_event(
            "custom.child",
            "agent",
            {"n": 3},
            caused_by=first["id"],
        )

        history = bus.fetch_after(
            0,
            None,
            correlation_id="workflow-a",
        )
        self.assertEqual([first["id"], third["id"]], [item["id"] for item in history])

        events, scanned_to, full = bus.fetch_stream_window(
            0,
            None,
            limit=2,
            correlation_id="workflow-a",
        )
        self.assertEqual(
            [first["id"], third["id"]],
            [item["id"] for item in events],
        )
        self.assertEqual(third["id"], scanned_to)
        self.assertTrue(full)

        events, scanned_to, full = bus.fetch_stream_window(
            scanned_to,
            None,
            limit=2,
            correlation_id="workflow-a",
        )
        self.assertEqual([], events)
        self.assertEqual(third["id"], scanned_to)
        self.assertFalse(full)

    def test_stream_window_filters_in_sql(self):
        bus.append_event("custom.one", "agent", {})
        matching = bus.append_event("custom.match", "agent", {})
        bus.append_event("custom.two", "agent", {})

        events, scanned_to, full = bus.fetch_stream_window(
            0,
            ["custom.match"],
            limit=2,
        )
        self.assertEqual([matching["id"]], [item["id"] for item in events])
        self.assertEqual(matching["id"], scanned_to)
        self.assertFalse(full)

        events, scanned_to, full = bus.fetch_stream_window(
            scanned_to,
            ["custom.match"],
            limit=2,
        )
        self.assertEqual([], events)
        self.assertEqual(matching["id"], scanned_to)
        self.assertFalse(full)

    def test_filtered_stream_does_not_decode_unrelated_payloads(self):
        with bus.db() as connection:
            connection.execute(
                """
                INSERT INTO events
                    (ts, topic, actor, schema_version, payload)
                VALUES (1, 'telemetry.corrupt', 'agent', 1, 'not-json')
                """
            )
        matching = bus.append_event("task.created", "human", {"title": "valid"})

        events, scanned_to, full = bus.fetch_stream_window(
            0,
            ["task.created"],
        )
        self.assertEqual([matching["id"]], [item["id"] for item in events])
        self.assertEqual(matching["id"], scanned_to)
        self.assertFalse(full)

    def test_legacy_rows_remain_readable_but_new_v1_events_are_rejected(self):
        with bus.db() as connection:
            connection.execute(
                """
                INSERT INTO events
                    (ts, topic, actor, schema_version, idempotency_key, caused_by, payload)
                VALUES (1, 'task.completed', 'alice', 1, NULL, 1, '{"task_id":1}')
                """
            )
        self.assertEqual(1, bus.fetch_after(0, None)[0]["schema_version"])

        with self.assertRaises(bus.EventValidationError):
            bus.append_event(
                "task.completed",
                "alice",
                {"task_id": 1},
                caused_by=1,
                schema_version=1,
            )

        child = bus.append_event(
            "custom.after-legacy",
            "agent",
            {},
            caused_by=1,
        )
        self.assertIsNone(child["correlation_id"])

    def test_init_db_migrates_pre_correlation_and_producer_database(self):
        bus.DB_PATH.unlink()
        connection = sqlite3.connect(bus.DB_PATH)
        try:
            connection.execute(
                """
                CREATE TABLE events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    topic TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    schema_version INTEGER NOT NULL DEFAULT 1,
                    idempotency_key TEXT,
                    caused_by INTEGER,
                    payload TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO events
                    (ts, topic, actor, schema_version, idempotency_key, caused_by, payload)
                VALUES (1, 'custom.legacy', 'legacy', 1, NULL, NULL, '{}')
                """
            )
            connection.commit()
        finally:
            connection.close()

        bus.init_db()

        with bus.db() as migrated:
            columns = {
                row["name"] for row in migrated.execute("PRAGMA table_info(events)")
            }
        self.assertIn("correlation_id", columns)
        self.assertIn("producer", columns)
        legacy = bus.fetch_after(0, None)[0]
        self.assertIsNone(legacy["correlation_id"])
        self.assertIsNone(legacy["producer"])


class BusApiTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = bus.DB_PATH
        self.original_api_token = bus.API_TOKEN
        bus.DB_PATH = Path(self.temp_dir.name) / "events.db"
        # API tests must not inherit authentication policy from the shell that
        # launched the suite. The dedicated auth test enables it explicitly.
        bus.API_TOKEN = None
        self.client_context = TestClient(bus.app)
        self.client = self.client_context.__enter__()

    def tearDown(self):
        self.client_context.__exit__(None, None, None)
        bus.DB_PATH = self.original_db_path
        bus.API_TOKEN = self.original_api_token
        self.temp_dir.cleanup()

    def test_publish_query_and_contract_errors(self):
        self.assertEqual("0.6.0", bus.app.version)
        created = self.client.post(
            "/events",
            json={
                "topic": "task.created",
                "actor": "human",
                "payload": {"title": "API task"},
                "idempotency_key": "api:create:one",
            },
        )
        self.assertEqual(200, created.status_code)
        self.assertEqual(2, created.json()["schema_version"])
        self.assertIsNotNone(created.json()["correlation_id"])

        invalid = self.client.post(
            "/events",
            json={
                "topic": "task.completed",
                "actor": "alice",
                "payload": {"task_id": 1},
            },
        )
        self.assertEqual(422, invalid.status_code)

        conflict = self.client.post(
            "/events",
            json={
                "topic": "task.created",
                "actor": "human",
                "payload": {"title": "different"},
                "idempotency_key": "api:create:one",
            },
        )
        self.assertEqual(409, conflict.status_code)

        queried = self.client.get("/events", params={"limit": 10})
        self.assertEqual(200, queried.status_code)
        self.assertEqual(1, len(queried.json()))
        fetched = self.client.get(f"/events/{created.json()['id']}")
        self.assertEqual(created.json(), fetched.json())
        self.assertEqual(404, self.client.get("/events/999").status_code)

    def test_correlation_propagation_validation_and_query(self):
        root = self.client.post(
            "/events",
            json={
                "topic": "task.created",
                "actor": "human",
                "correlation_id": "workflow-api",
                "payload": {"title": "API workflow"},
            },
        )
        self.assertEqual(200, root.status_code)

        child = self.client.post(
            "/events",
            json={
                "topic": "custom.child",
                "actor": "agent",
                "caused_by": root.json()["id"],
                "payload": {},
            },
        )
        self.assertEqual(200, child.status_code)
        self.assertEqual("workflow-api", child.json()["correlation_id"])

        conflict = self.client.post(
            "/events",
            json={
                "topic": "custom.conflict",
                "actor": "agent",
                "caused_by": root.json()["id"],
                "correlation_id": "workflow-other",
                "payload": {},
            },
        )
        self.assertEqual(422, conflict.status_code)

        orphan = self.client.post(
            "/events",
            json={
                "topic": "custom.orphan",
                "actor": "agent",
                "caused_by": 999,
                "payload": {},
            },
        )
        self.assertEqual(422, orphan.status_code)

        queried = self.client.get(
            "/events",
            params={"correlation_id": "workflow-api"},
        )
        self.assertEqual(200, queried.status_code)
        self.assertEqual(
            [root.json()["id"], child.json()["id"]],
            [item["id"] for item in queried.json()],
        )

        invalid_filter = self.client.get(
            "/events",
            params={"correlation_id": " workflow-api "},
        )
        self.assertEqual(422, invalid_filter.status_code)

    def test_full_page_is_signaled_in_response_header(self):
        for index in range(3):
            self.client.post(
                "/events",
                json={"topic": "custom.tick", "actor": "agent", "payload": {"n": index}},
            )

        full = self.client.get("/events", params={"limit": 2})
        self.assertEqual("1", full.headers["X-Page-Full"])

        partial = self.client.get("/events", params={"limit": 10})
        self.assertEqual("0", partial.headers["X-Page-Full"])

    def test_bearer_token_guards_data_routes_when_configured(self):
        bus.API_TOKEN = "secret"
        try:
            body = {"topic": "custom.tick", "actor": "agent", "payload": {}}

            self.assertEqual(401, self.client.post("/events", json=body).status_code)
            self.assertEqual(401, self.client.get("/events").status_code)
            self.assertEqual(200, self.client.get("/health").status_code)

            headers = {"Authorization": "Bearer secret"}
            self.assertEqual(
                200, self.client.post("/events", json=body, headers=headers).status_code
            )
            self.assertEqual(200, self.client.get("/events", headers=headers).status_code)
        finally:
            bus.API_TOKEN = None


if __name__ == "__main__":
    unittest.main()

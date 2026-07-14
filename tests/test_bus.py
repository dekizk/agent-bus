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
        self.assertEqual(1, len(bus.fetch_after(0, None)))

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
            caused_by=1,
            idempotency_key="completed:task:1:attempt:1",
        )
        second = bus.append_event(
            "task.completed",
            "alice",
            payload,
            caused_by=1,
            idempotency_key="completed:task:1:attempt:1",
        )
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(1, len(bus.fetch_after(0, None)))

    def test_filtered_stream_window_advances_over_unrelated_events(self):
        bus.append_event("custom.one", "agent", {})
        matching = bus.append_event("custom.match", "agent", {})
        bus.append_event("custom.two", "agent", {})

        events, scanned_to, full = bus.fetch_stream_window(
            0,
            ["custom.match"],
            limit=2,
        )
        self.assertEqual([matching["id"]], [item["id"] for item in events])
        self.assertEqual(2, scanned_to)
        self.assertTrue(full)

        events, scanned_to, full = bus.fetch_stream_window(
            scanned_to,
            ["custom.match"],
            limit=2,
        )
        self.assertEqual([], events)
        self.assertEqual(3, scanned_to)
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

class BusApiTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = bus.DB_PATH
        bus.DB_PATH = Path(self.temp_dir.name) / "events.db"
        self.client_context = TestClient(bus.app)
        self.client = self.client_context.__enter__()

    def tearDown(self):
        self.client_context.__exit__(None, None, None)
        bus.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_publish_query_and_contract_errors(self):
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
        original_token = bus.API_TOKEN
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
            bus.API_TOKEN = original_token


if __name__ == "__main__":
    unittest.main()

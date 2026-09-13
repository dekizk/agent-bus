import io
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import bus
from agent_bus_cli import main
from local_config import initialize_local_config, LocalConfig
from projection import replay_events


class TaskIndexTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.original = bus.DB_PATH
        bus.DB_PATH = Path(self.temp.name) / "events.db"
        bus.init_db()

    def tearDown(self):
        bus.DB_PATH = self.original
        self.temp.cleanup()

    def create(self, **payload):
        return bus.append_event("task.created", "test", {"title": "Task", **payload})

    def rows(self, table):
        with bus.db() as conn:
            return [tuple(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY 1")]

    def test_rebuild_and_missing_table_preserve_history_and_projection(self):
        first = self.create(task_id=50)
        self.create(depends_on=[50])
        before = self.rows("events")
        state = replay_events(bus.fetch_after(0, None))
        with bus.db() as conn:
            conn.execute("DROP TABLE task_identity")
        bus.init_db()
        self.assertEqual(2, bus.rebuild_task_index())
        self.assertEqual(before, self.rows("events"))
        self.assertEqual(vars(state), vars(replay_events(bus.fetch_after(0, None))))
        with bus.db() as conn:
            self.assertEqual(first["id"], bus._find_task_created(conn, 50)["id"])
        self.assertEqual(52, self.create()["payload"]["task_id"])

    def test_backfill_first_valid_identity_and_counter_recovery(self):
        with bus.db() as conn:
            conn.execute("DROP TABLE task_identity")
            for payload in ['[]', 'null', 'bad json', '{"task_id":true}',
                            '{"task_id":-1}', '{"task_id":12}', '{"task_id":12}']:
                conn.execute("INSERT INTO events(ts,topic,actor,payload) "
                             "VALUES (1,'task.created','legacy',?)", (payload,))
        bus.init_db()
        self.assertEqual([(12, 6)], self.rows("task_identity"))
        self.assertEqual(13, self.create()["payload"]["task_id"])
        with bus.db() as conn:
            conn.execute("UPDATE counters SET value=100 WHERE name='task_id'")
        bus.rebuild_task_index()
        self.assertEqual(101, self.create()["payload"]["task_id"])

    def test_failed_rebuild_rolls_back_and_retry_succeeds(self):
        self.create()
        previous = self.rows("task_identity")
        events = self.rows("events")
        with patch.object(bus, "_backfill_task_identity", side_effect=RuntimeError("crash")):
            with self.assertRaises(RuntimeError):
                bus.rebuild_task_index()
        self.assertEqual(previous, self.rows("task_identity"))
        self.assertEqual(events, self.rows("events"))
        self.assertEqual(1, bus.rebuild_task_index())

    def test_interrupted_migration_does_not_leave_a_partial_index(self):
        self.create()
        events = self.rows("events")
        with bus.db() as conn:
            conn.execute("DROP TABLE task_identity")
        with patch.object(bus, "_backfill_task_identity", side_effect=RuntimeError("migration interrupted")):
            with self.assertRaises(RuntimeError):
                bus.init_db()
        with bus.db() as conn:
            self.assertIsNone(conn.execute("SELECT 1 FROM sqlite_master WHERE name='task_identity'").fetchone())
        self.assertEqual(events, self.rows("events"))
        bus.init_db()
        self.assertEqual(1, len(self.rows("task_identity")))

    def test_index_insert_failure_rolls_back_event_and_counter(self):
        with bus.db() as conn:
            conn.execute("CREATE TRIGGER fail_index BEFORE INSERT ON task_identity "
                         "BEGIN SELECT RAISE(ABORT,'injected failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.create()
        self.assertEqual([], self.rows("events"))
        self.assertEqual([], self.rows("task_identity"))
        self.assertEqual([("task_id", 0)], self.rows("counters"))
        with bus.db() as conn:
            conn.execute("DROP TRIGGER fail_index")
        self.assertEqual(1, self.create()["payload"]["task_id"])

    def test_process_loss_during_rebuild_preserves_committed_index(self):
        self.create()
        before = self.rows("task_identity")
        result = subprocess.run([
            sys.executable, "-c",
            "import os, sys, bus; from pathlib import Path; "
            "bus.DB_PATH=Path(sys.argv[1]); "
            "bus._backfill_task_identity=lambda conn: os._exit(77); "
            "bus.rebuild_task_index()", str(bus.DB_PATH),
        ], cwd=Path(bus.__file__).parent, capture_output=True, timeout=10)
        self.assertEqual(77, result.returncode, result.stderr)
        bus.init_db()
        self.assertEqual(before, self.rows("task_identity"))
        self.assertEqual(1, bus.rebuild_task_index())

    def test_rebuild_and_publish_concurrently_keep_all_identities(self):
        self.create()
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(bus.rebuild_task_index) if i % 3 == 0
                       else pool.submit(self.create) for i in range(18)]
            for future in futures:
                future.result(timeout=10)
        self.assertEqual(13, len(self.rows("task_identity")))
        before = self.rows("task_identity")
        bus.rebuild_task_index()
        self.assertEqual(before, self.rows("task_identity"))

    def test_idempotency_and_concurrent_explicit_duplicates(self):
        def publish(_):
            try:
                return self.create(task_id=99)
            except bus.EventValidationError:
                return None
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(publish, range(8)))
        self.assertEqual(1, sum(result is not None for result in results))
        one = bus.append_event("task.created", "test", {"title": "Stable"},
                               idempotency_key="stable")
        again = bus.append_event("task.created", "test", {"title": "Stable"},
                                 idempotency_key="stable")
        self.assertEqual(one, again)
        self.assertEqual(2, len(self.rows("task_identity")))

    def test_lookup_plan_uses_primary_keys_and_startup_does_not_backfill(self):
        self.create()
        with patch.object(bus, "_backfill_task_identity", side_effect=AssertionError):
            bus.init_db()
        with bus.db() as conn:
            plan = list(conn.execute(
                "EXPLAIN QUERY PLAN SELECT events.* FROM task_identity AS identity "
                "JOIN events ON events.id=identity.created_event_id WHERE identity.task_id=?", (1,)))
        self.assertEqual(2, len(plan))
        self.assertTrue(all("SEARCH" in row["detail"] and "PRIMARY KEY" in row["detail"]
                            for row in plan), plan)

    def test_cli_rebuild_uses_config_database(self):
        config = initialize_local_config(Path(self.temp.name) / "local")
        local = LocalConfig.from_file(config)
        local.database_path.parent.mkdir(parents=True, exist_ok=True)
        bus.DB_PATH = local.database_path
        bus.init_db()
        self.create()
        with bus.db() as conn:
            conn.execute("DELETE FROM task_identity")
        output = io.StringIO()
        with patch.dict("os.environ"):
            self.assertEqual(0, main(["rebuild-task-index", "--config", str(config), "--json"],
                                     stdout=output))
        self.assertEqual(1, json.loads(output.getvalue())["tasks_indexed"])


if __name__ == "__main__":
    unittest.main()

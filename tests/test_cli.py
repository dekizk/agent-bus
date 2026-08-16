import io
import json
import os
import tempfile
import unittest
from pathlib import Path

import httpx

import agent_bus_cli
from observer import ObserverClient
from tests.test_operations import completed_workflow_events, event, terminal_model


class FakeObserver:
    def __init__(self, base_url, *, token=None):
        self.base_url = base_url.rstrip("/")
        self.token = token

    def health(self):
        return {"ok": True, "schema_version": 2}

    def query_all(self, *, topics=None, correlation_id=None, **kwargs):
        if topics and any(topic.startswith("telemetry.") for topic in topics):
            return [terminal_model(20, "task:1:attempt:1", "model-1")]
        return completed_workflow_events()

    def subscribe(self, **kwargs):
        yield event(30, "task.completed", "alice", {"task_id": 1})


class ObserverClientTests(unittest.TestCase):
    def test_client_exposes_get_only_operations_and_pages_without_files(self):
        requests = []

        def handler(request):
            requests.append(request)
            if request.url.path == "/health":
                return httpx.Response(200, json={"ok": True, "schema_version": 2})
            after_id = int(request.url.params.get("after_id", "0"))
            if after_id == 0:
                return httpx.Response(200, json=[{"id": 1}, {"id": 2}])
            return httpx.Response(200, json=[])

        with tempfile.TemporaryDirectory() as directory:
            original = os.getcwd()
            try:
                os.chdir(directory)
                transport = httpx.MockTransport(handler)
                http_client = httpx.Client(transport=transport)
                client = ObserverClient("http://bus", client=http_client)
                self.assertTrue(client.health()["ok"])
                self.assertEqual([1, 2], [item["id"] for item in client.query_all(page_size=2)])
                self.assertEqual([], list(Path(directory).iterdir()))
            finally:
                os.chdir(original)
        self.assertEqual({"GET"}, {request.method for request in requests})
        self.assertFalse(hasattr(client, "publish"))


class CliTests(unittest.TestCase):
    def run_cli(self, argv):
        stdout = io.StringIO()
        stderr = io.StringIO()
        code = agent_bus_cli.main(
            argv,
            stdout=stdout,
            stderr=stderr,
            clock=lambda: 101,
            client_factory=FakeObserver,
        )
        return code, stdout.getvalue(), stderr.getvalue()

    def test_task_defaults_to_human_output_with_trace(self):
        code, stdout, stderr = self.run_cli(["task", "1"])
        self.assertEqual(0, code)
        self.assertIn("Task 1 · Produce", stdout)
        self.assertIn("State: completed · event #6", stdout)
        self.assertIn("Last attempt: alice", stdout)
        self.assertIn("Trace: #6", stdout)
        self.assertEqual("", stderr)

    def test_json_flag_works_before_or_after_the_command(self):
        for argv in (["--json", "task", "1"], ["task", "1", "--json"]):
            with self.subTest(argv=argv):
                code, stdout, _ = self.run_cli(argv)
                self.assertEqual(0, code)
                value = json.loads(stdout)
                self.assertEqual(1, value["task_id"])
                self.assertFalse(value["assignment_active"])

    def test_workflow_shows_dag_and_usage(self):
        code, stdout, _ = self.run_cli(["workflow", "flow-1"])
        self.assertEqual(0, code)
        self.assertIn("Workflow flow-1 · completed · 2 tasks", stdout)
        self.assertIn("depends on [1]", stdout)
        self.assertIn("10 tokens", stdout)

    def test_explain_and_workers_are_concise(self):
        code, explanation, _ = self.run_cli(["explain", "2"])
        self.assertEqual(0, code)
        self.assertIn("Task 2: Task completed successfully", explanation)
        code, workers, _ = self.run_cli(["workers"])
        self.assertEqual(0, code)
        self.assertIn("alice · healthy", workers)
        self.assertIn("event #1", workers)

    def test_doctor_reports_replay_and_health(self):
        code, stdout, _ = self.run_cli(["doctor"])
        self.assertEqual(0, code)
        self.assertIn("Bus: healthy", stdout)
        self.assertIn("2 tasks", stdout)

    def test_tail_is_filtered_and_machine_readable(self):
        code, stdout, _ = self.run_cli(["tail", "flow-1", "--json"])
        self.assertEqual(0, code)
        self.assertEqual("task.completed", json.loads(stdout)["topic"])

    def test_missing_task_returns_distinct_actionable_exit(self):
        code, _, stderr = self.run_cli(["task", "999"])
        self.assertEqual(3, code)
        self.assertIn("task 999 was not found", stderr)

    def test_read_only_command_creates_no_local_projection_or_offset(self):
        with tempfile.TemporaryDirectory() as directory:
            original = os.getcwd()
            try:
                os.chdir(directory)
                code, _, _ = self.run_cli(["workflow", "flow-1"])
                self.assertEqual(0, code)
                self.assertEqual([], list(Path(directory).iterdir()))
            finally:
                os.chdir(original)


if __name__ == "__main__":
    unittest.main()

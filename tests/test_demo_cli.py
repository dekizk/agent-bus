"""CLI flags that make the demo worker and task submission scriptable."""

import io
import tempfile
import unittest
from unittest.mock import patch

from logplane import cli as logplane_cli
from logplane import worker
from logplane.local_config import initialize_local_config


def run(argv):
    stdout, stderr = io.StringIO(), io.StringIO()
    code = logplane_cli.main(argv, stdout=stdout, stderr=stderr)
    return code, stdout.getvalue(), stderr.getvalue()


class DemoWorkerFlagTests(unittest.TestCase):
    def test_outcome_flags_are_mutually_exclusive(self):
        parser = logplane_cli.build_parser()
        with patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(["demo-worker", "alice", "--block", "1", "--fail", "2"])

    def test_negative_delay_is_rejected(self):
        parser = logplane_cli.build_parser()
        with patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(["demo-worker", "alice", "--delay", "-1"])

    def test_flags_reach_the_demo_executor(self):
        cases = (
            (["--block", "3"], {"block_once": 3}),
            (["--fail", "4"], {"fail_once": 4}),
            (["--fail-permanently", "5"], {"fail_permanently_once": 5}),
            ([], {}),
        )
        for extra, expected in cases:
            with self.subTest(extra=extra), tempfile.TemporaryDirectory() as directory:
                config = initialize_local_config(directory)
                with (
                    patch("logplane.client.BusClient"),
                    patch("logplane.runtime.WorkerRuntime") as runtime_class,
                ):
                    code, _, stderr = run(
                        ["demo-worker", "alice", "--config", str(config), "--delay", "2.5", *extra]
                    )
                self.assertEqual(0, code, stderr)
                executor = runtime_class.call_args.kwargs["executor"]
                self.assertIsInstance(executor, worker.DemoExecutor)
                self.assertEqual(2.5, executor.delay_seconds)
                for name in ("block_once", "fail_once", "fail_permanently_once"):
                    self.assertEqual(expected.get(name), getattr(executor, name))
                runtime_class.return_value.run.assert_called_once_with()

    def test_default_delay_matches_previous_behaviour(self):
        self.assertEqual(0.5, worker.DemoExecutor("alice").delay_seconds)
        with self.assertRaises(ValueError):
            worker.DemoExecutor("alice", delay_seconds=-1)


class SubmitDependsOnTests(unittest.TestCase):
    def submit(self, extra):
        with tempfile.TemporaryDirectory() as directory:
            config = initialize_local_config(directory)
            with patch("logplane.client.BusClient") as client_class:
                client_class.return_value.publish.return_value = {
                    "id": 9,
                    "correlation_id": "flow-1",
                    "payload": {"task_id": 3},
                }
                code, stdout, stderr = run(
                    ["submit", "Write the report", "--config", str(config), *extra]
                )
            self.assertEqual(0, code, stderr)
            topic, payload = client_class.return_value.publish.call_args.args
            self.assertEqual("task.created", topic)
            return payload, client_class.return_value.publish.call_args.kwargs, stdout

    def test_repeated_depends_on_is_recorded_in_order(self):
        payload, kwargs, stdout = self.submit(["--depends-on", "1", "--depends-on", "2"])
        self.assertEqual([1, 2], payload["depends_on"])
        self.assertIsNone(kwargs["correlation_id"])
        self.assertIn("Submitted task 3", stdout)

    def test_without_depends_on_the_payload_is_unchanged(self):
        payload, _, _ = self.submit([])
        self.assertNotIn("depends_on", payload)

    def test_depends_on_requires_positive_task_ids(self):
        parser = logplane_cli.build_parser()
        with patch("sys.stderr", io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(["submit", "x", "--depends-on", "0"])


if __name__ == "__main__":
    unittest.main()

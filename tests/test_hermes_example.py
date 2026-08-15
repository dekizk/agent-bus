import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import ANY, Mock, patch

from examples.hermes.hermes_executor import HermesExecutor
from examples.hermes import live_smoke, run_worker
from artifacts import ArtifactStore
from executors import (
    AssignmentContext,
    Blocked,
    Completed,
    PermanentFailure,
    RetryableFailure,
)


def assignment(**overrides):
    values = {
        "correlation_id": "hermes-trial",
        "task_id": 7,
        "assignment_id": "task:7:attempt:1",
        "assignment_event_id": 22,
        "attempt": 1,
        "goal": "Summarize the supplied integration note",
        "context": {"text": "Hermes is an interchangeable executor."},
        "required_capabilities": ("hermes",),
        "max_retries": 1,
        "retryable_failures": 0,
        "assignee": "hermes",
        "worker_instance_id": "hermes-1",
    }
    values.update(overrides)
    return AssignmentContext(**values)


FAKE_HERMES = r'''import json
import os
import sys
import time
from pathlib import Path

capture = os.environ.get("FAKE_HERMES_CAPTURE")
if capture:
    Path(capture).write_text(
        json.dumps({
            "argv": sys.argv[1:],
            "cwd": os.getcwd(),
            "source": os.environ.get("HERMES_SESSION_SOURCE"),
            "safe": os.environ.get("HERMES_SAFE_MODE"),
            "ignore_rules": os.environ.get("HERMES_IGNORE_RULES"),
        }),
        encoding="utf-8",
    )
started = os.environ.get("FAKE_HERMES_STARTED")
if started:
    Path(started).write_text("started", encoding="utf-8")
sleep_seconds = float(os.environ.get("FAKE_HERMES_SLEEP", "0"))
if sleep_seconds:
    time.sleep(sleep_seconds)
if "--usage-file" in sys.argv:
    usage_path = Path(sys.argv[sys.argv.index("--usage-file") + 1])
    usage_path.write_text(
        os.environ.get(
            "FAKE_HERMES_USAGE",
            json.dumps({"total_tokens": 12, "model": "fake", "unknown": "drop"}),
        ),
        encoding="utf-8",
    )
sys.stdout.write(os.environ.get("FAKE_HERMES_STDOUT", ""))
sys.stderr.write(os.environ.get("FAKE_HERMES_STDERR", ""))
raise SystemExit(int(os.environ.get("FAKE_HERMES_EXIT", "0")))
'''


class HermesExecutorTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.fake = self.root / "fake_hermes.py"
        self.fake.write_text(FAKE_HERMES, encoding="utf-8")

    def tearDown(self):
        self.temp_dir.cleanup()

    def make_executor(self, output, **overrides):
        environment = {
            "FAKE_HERMES_STDOUT": json.dumps(output),
        }
        environment.update(overrides.pop("environment", {}))
        timeout = overrides.pop("timeout", 2)
        return HermesExecutor(
            working_directory=self.root,
            model="fake-model",
            provider="fake-provider",
            toolsets=("clarify",),
            command=(sys.executable, str(self.fake)),
            timeout=timeout,
            environment=environment,
            **overrides,
        )

    def test_all_agent_bus_outcomes_are_supported(self):
        cases = (
            (
                {"status": "completed", "summary": "done", "result": {"value": 1}},
                Completed,
            ),
            ({"status": "blocked", "reason": "approval"}, Blocked),
            (
                {
                    "status": "retryable_failure",
                    "code": "temporary",
                    "reason": "try again",
                },
                RetryableFailure,
            ),
            (
                {
                    "status": "permanent_failure",
                    "code": "invalid",
                    "reason": "cannot run",
                },
                PermanentFailure,
            ),
        )
        for payload, expected_type in cases:
            with self.subTest(status=payload["status"]):
                result = self.make_executor(payload).execute(assignment())
                self.assertIsInstance(result, expected_type)

    def test_invocation_is_isolated_bounded_and_reports_filtered_usage(self):
        capture = self.root / "capture.json"
        usages = []
        executor = self.make_executor(
            {"status": "completed", "summary": "done", "result": {}},
            environment={"FAKE_HERMES_CAPTURE": str(capture)},
            usage_callback=lambda assignment_id, usage: usages.append(
                (assignment_id, dict(usage))
            ),
        )
        result = executor.execute(assignment())
        self.assertIsInstance(result, Completed)

        recorded = json.loads(capture.read_text(encoding="utf-8"))
        args = recorded["argv"]
        prompt = args[args.index("-z") + 1]
        self.assertIn('"assignment_id": "task:7:attempt:1"', prompt)
        self.assertIn("agent-bus owns assignment", prompt)
        self.assertIn("decisions array contains authoritative human responses", prompt)
        self.assertEqual("clarify", args[args.index("--toolsets") + 1])
        self.assertIn("--safe-mode", args)
        self.assertEqual(self.root.resolve(), Path(recorded["cwd"]).resolve())
        self.assertEqual("tool", recorded["source"])
        self.assertEqual("1", recorded["safe"])
        self.assertEqual("1", recorded["ignore_rules"])
        self.assertEqual("task:7:attempt:1", usages[0][0])
        self.assertEqual(12, usages[0][1]["total_tokens"])
        self.assertNotIn("unknown", usages[0][1])

    def test_model_telemetry_is_emitted_without_controlling_the_outcome(self):
        telemetry = Mock()
        telemetry.model_started.return_value = {"id": 88}
        executor = self.make_executor(
            {"status": "completed", "summary": "done", "result": {}},
            telemetry_sink=telemetry,
        )

        result = executor.execute(assignment())

        self.assertIsInstance(result, Completed)
        started = telemetry.model_started.call_args.kwargs
        self.assertEqual("task:7:attempt:1:model:1", started["invocation_id"])
        self.assertEqual("fake-provider", started["provider"])
        self.assertIn("ASSIGNMENT_JSON", started["input_content"])
        completed = telemetry.model_completed.call_args.kwargs
        self.assertEqual(88, completed["caused_by"])
        self.assertEqual(12, completed["usage"]["total_tokens"])

        telemetry.model_started.side_effect = RuntimeError("telemetry offline")
        telemetry.model_completed.side_effect = RuntimeError("telemetry offline")
        unaffected = executor.execute(assignment())
        self.assertIsInstance(unaffected, Completed)

    def test_prompt_marks_human_decisions_as_authoritative(self):
        prompt = HermesExecutor.build_prompt(
            assignment(
                attempt=2,
                assignment_id="task:7:attempt:2",
                context={"release_target": None},
                decisions=(
                    {
                        "event_id": 31,
                        "actor": "human",
                        "assignment_id": "task:7:attempt:1",
                        "decision_id": "decision:task:7:attempt:1",
                        "decision": {"release_target": "staging"},
                    },
                ),
            )
        )
        self.assertIn('"release_target": null', prompt)
        self.assertIn('"release_target": "staging"', prompt)
        self.assertIn("A later decision supersedes conflicting", prompt)
        self.assertIn("Do not block again for information already supplied", prompt)
        self.assertIn("dependencies array", prompt)
        self.assertIn("declared depends_on order", prompt)

    def test_process_errors_timeout_and_invalid_output_are_typed(self):
        failed = self.make_executor(
            {"status": "completed", "summary": "unused"},
            environment={"FAKE_HERMES_EXIT": "1", "FAKE_HERMES_STDERR": "offline"},
        ).execute(assignment())
        self.assertIsInstance(failed, RetryableFailure)
        self.assertEqual("hermes_process_failed", failed.code)

        timed_out = self.make_executor(
            {"status": "completed", "summary": "late"},
            timeout=0.03,
            environment={"FAKE_HERMES_SLEEP": "1"},
        ).execute(assignment())
        self.assertIsInstance(timed_out, RetryableFailure)
        self.assertEqual("hermes_timeout", timed_out.code)

        invalid = self.make_executor(
            {},
            environment={"FAKE_HERMES_STDOUT": "not-json"},
        ).execute(assignment())
        self.assertIsInstance(invalid, PermanentFailure)
        self.assertEqual("invalid_hermes_output", invalid.code)

        oversized = self.make_executor(
            {},
            max_output_bytes=64,
            environment={"FAKE_HERMES_STDOUT": "x" * 1000},
        ).execute(assignment())
        self.assertIsInstance(oversized, PermanentFailure)
        self.assertEqual("hermes_output_too_large", oversized.code)

        missing = HermesExecutor(
            working_directory=self.root,
            model="fake-model",
            provider="fake-provider",
            command=(str(self.root / "missing-hermes"),),
        ).execute(assignment())
        self.assertIsInstance(missing, PermanentFailure)
        self.assertEqual("hermes_start_failed", missing.code)

    def test_cancellation_terminates_the_active_process(self):
        started = self.root / "started"
        executor = self.make_executor(
            {"status": "completed", "summary": "late"},
            timeout=5,
            environment={
                "FAKE_HERMES_SLEEP": "5",
                "FAKE_HERMES_STARTED": str(started),
            },
        )
        results = []
        thread = threading.Thread(
            target=lambda: results.append(executor.execute(assignment()))
        )
        thread.start()
        deadline = time.time() + 2
        while time.time() < deadline and not started.exists():
            time.sleep(0.005)
        self.assertTrue(started.exists())
        executor.cancel("task:7:attempt:1")
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertIsInstance(results[0], RetryableFailure)
        self.assertEqual("hermes_cancelled", results[0].code)

    def test_configuration_requires_explicit_bounded_authority(self):
        with self.assertRaisesRegex(ValueError, "safe_mode requires"):
            HermesExecutor(
                working_directory=self.root,
                model=None,
                provider=None,
            )
        for toolset in ("all", "*", "todo", "Todo", "delegation", "cronjob"):
            with self.subTest(toolset=toolset):
                with self.assertRaisesRegex(ValueError, "not allowed"):
                    HermesExecutor(
                        working_directory=self.root,
                        model="fake-model",
                        provider="fake-provider",
                        toolsets=(toolset,),
                    )


class HermesWorkerWiringTests(unittest.TestCase):
    @patch("examples.hermes.run_worker.WorkerRuntime")
    @patch("examples.hermes.run_worker.BusClient")
    @patch("examples.hermes.run_worker.HermesExecutor")
    def test_worker_uses_public_runtime_contract(
        self,
        executor_class,
        client_class,
        runtime_class,
    ):
        executor = executor_class.return_value
        bus_client = client_class.return_value
        runtime = runtime_class.return_value
        with tempfile.TemporaryDirectory() as temp_dir:
            run_worker.main(
                [
                    "--working-directory",
                    temp_dir,
                    "--model",
                    "fake-model",
                    "--provider",
                    "fake-provider",
                    "--capability",
                    "research",
                ]
            )
        executor_class.assert_called_once()
        client_class.assert_called_once_with(
            "http://127.0.0.1:8765",
            actor="hermes",
        )
        runtime_class.assert_called_once_with(
            bus_client,
            name="hermes",
            executor=executor,
            instance_id=ANY,
            capacity=1,
            capabilities=["research"],
            heartbeat_seconds=5.0,
        )
        runtime.run.assert_called_once_with()


class HermesLiveSmokeTests(unittest.TestCase):
    def test_artifact_capture_verification_requires_explicit_complete_refs(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArtifactStore(Path(directory))
            input_ref = store.put_text("prompt", kind="model_input")
            output_ref = store.put_text("result", kind="model_output")
            events = [
                {"payload": {"artifacts": [input_ref]}},
                {"payload": {"artifacts": [output_ref]}},
            ]

            self.assertEqual(
                [input_ref, output_ref],
                live_smoke.verify_artifact_capture(events, store, required=True),
            )
            self.assertEqual(
                [],
                live_smoke.verify_artifact_capture(
                    [{"payload": {"artifacts": []}}],
                    None,
                    required=False,
                ),
            )
            with self.assertRaisesRegex(RuntimeError, "without explicit opt-in"):
                live_smoke.verify_artifact_capture(events, store, required=False)
            with self.assertRaisesRegex(RuntimeError, "input and output"):
                live_smoke.verify_artifact_capture(events[:1], store, required=True)


if __name__ == "__main__":
    unittest.main()

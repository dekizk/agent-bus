import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

import agent_bus
import agent_bus_cli
from conformance import check_executor
from executor_protocol import (
    CURRENT_PROTOCOL_VERSION,
    PROTOCOL_NAME,
    assignment_message,
    outcome_message,
    parse_assignment_message,
    parse_outcome_message,
)
from executors import AssignmentContext, Completed, PermanentFailure, RetryableFailure
from integration import (
    CliAgentConfig,
    HttpAgentConfig,
    IntegrationConfig,
    PythonAgentAdapter,
)
from local_config import LocalConfig, initialize_local_config
from operations import workflow_mermaid


def assignment():
    return AssignmentContext(
        correlation_id="workflow-one",
        task_id=1,
        assignment_id="task:1:attempt:1",
        assignment_event_id=2,
        attempt=1,
        goal="Perform bounded work",
        context={},
        required_capabilities=(),
        max_retries=0,
        retryable_failures=0,
        assignee="example",
        worker_instance_id="example-1",
    )


class ProtocolTests(unittest.TestCase):
    def test_v1_envelopes_are_strict_and_round_trip(self):
        request = assignment_message(assignment().to_dict(), CURRENT_PROTOCOL_VERSION)
        self.assertEqual(PROTOCOL_NAME, request["protocol"]["name"])
        self.assertEqual(1, request["protocol"]["version"])
        self.assertEqual(1, parse_assignment_message(request, 1)["task_id"])

        response = outcome_message(
            {"status": "completed", "summary": "done", "result": {}},
            CURRENT_PROTOCOL_VERSION,
        )
        self.assertEqual("completed", parse_outcome_message(response, 1)["status"])
        response["protocol"]["version"] = 2
        with self.assertRaisesRegex(ValueError, "expected 1"):
            parse_outcome_message(response, 1)

    def test_public_package_exposes_the_integration_contract(self):
        self.assertIs(agent_bus.AssignmentContext, AssignmentContext)
        self.assertIs(agent_bus.PythonAgentAdapter, PythonAgentAdapter)
        self.assertIs(agent_bus.BusProtocolError, __import__("client").BusProtocolError)
        self.assertEqual(1, agent_bus.CURRENT_PROTOCOL_VERSION)

    def test_supported_versions_allows_future_capabilities_with_v1_selected(self):
        request = assignment_message(assignment().to_dict(), 1)
        request["protocol"]["supported_versions"] = [1, 2]
        self.assertEqual(1, parse_assignment_message(request, 1)["task_id"])
        for invalid in ([2], [1, 1], [], [True, 1]):
            with self.subTest(invalid=invalid):
                request["protocol"]["supported_versions"] = invalid
                with self.assertRaisesRegex(ValueError, "negotiation"):
                    parse_assignment_message(request, 1)


class AdapterTests(unittest.TestCase):
    def test_python_target_loader_imports_from_console_working_directory(self):
        from integration import load_python_target

        with tempfile.TemporaryDirectory() as directory:
            module_name = "local_adapter_for_agent_bus_test"
            Path(directory, f"{module_name}.py").write_text(
                "class ExistingAgent:\n    pass\n",
                encoding="utf-8",
            )
            original_directory = os.getcwd()
            original_path = list(sys.path)
            try:
                os.chdir(directory)
                sys.path[:] = [
                    entry
                    for entry in sys.path
                    if entry and Path(entry).resolve() != Path(directory).resolve()
                ]
                target = load_python_target(f"{module_name}:ExistingAgent")
            finally:
                os.chdir(original_directory)
                sys.path[:] = original_path
                sys.modules.pop(module_name, None)

        self.assertEqual("ExistingAgent", target.__class__.__name__)

    def test_effect_identity_is_stable_across_retry_attempts(self):
        first = assignment()
        second = AssignmentContext(
            correlation_id=first.correlation_id,
            task_id=first.task_id,
            assignment_id="task:1:attempt:2",
            assignment_event_id=3,
            attempt=2,
            goal=first.goal,
            assignee=first.assignee,
            worker_instance_id=first.worker_instance_id,
        )
        self.assertEqual(first.effect_scope, second.effect_scope)
        self.assertEqual(first.effect_id("send-email"), second.effect_id("send-email"))
        self.assertNotEqual(first.effect_id("send-email"), first.effect_id("charge-card"))
        self.assertEqual(first.effect_scope, first.to_dict()["effect_scope"])
        with self.assertRaisesRegex(ValueError, "operation_name"):
            first.effect_id(" ")

    def test_direct_config_construction_enforces_public_invariants(self):
        with self.assertRaisesRegex(ValueError, "command"):
            CliAgentConfig(())
        with self.assertRaisesRegex(ValueError, "protocol_version"):
            CliAgentConfig(("agent",), protocol_version=0)
        with self.assertRaisesRegex(ValueError, "timeout"):
            HttpAgentConfig("http://127.0.0.1/run", timeout_seconds=0)
        with self.assertRaisesRegex(ValueError, "allow_remote"):
            HttpAgentConfig("https://agent.example/run")
        with self.assertRaisesRegex(ValueError, "token_env"):
            HttpAgentConfig(
                "http://127.0.0.1/run",
                cancellation_endpoint="https://agent.example/cancel",
                allow_remote=True,
            )

    def test_conformance_reports_cleanup_failure_instead_of_crashing(self):
        class BrokenCleanup:
            def execute(self, received):
                return Completed("done")

            def close(self):
                raise RuntimeError("cleanup failed")

        report = check_executor(BrokenCleanup())
        self.assertFalse(report.ok)
        cleanup = next(check for check in report.checks if check.name == "cleanup hook")
        self.assertIn("RuntimeError", cleanup.detail)

    def test_python_agent_conforms_without_bus_runtime(self):
        class ExistingAgent:
            def run(self, received):
                return Completed("checked", {"task_id": received.task_id})

        report = check_executor(PythonAgentAdapter(ExistingAgent()))
        self.assertTrue(report.ok, report.to_dict())
        self.assertEqual("Completed", report.outcome_type)

    def test_configured_cli_uses_protocol_v1(self):
        code = (
            "import json,sys; request=json.load(sys.stdin); "
            "assert request['protocol']['version']==1; "
            "print(json.dumps({'protocol':{'name':'agent-bus.executor','version':1},"
            "'outcome':{'status':'completed','summary':'configured','result':{}}}))"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "adapter.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "worker": {"name": "configured", "capabilities": ["json"]},
                        "adapter": {
                            "type": "cli",
                            "command": [sys.executable, "-c", code],
                            "protocol_version": 1,
                            "working_directory": ".",
                        },
                    }
                ),
                encoding="utf-8",
            )
            config = IntegrationConfig.from_file(path)
            executor = config.build_executor()
            self.assertEqual("configured", executor.execute(assignment()).summary)
            executor.close()
            self.assertEqual(Path(directory).resolve(), config.adapter.working_directory)

    def test_http_bridge_is_loopback_first_versioned_and_status_aware(self):
        requests = []

        def handler(request):
            requests.append(request)
            value = json.loads(request.content)
            self.assertEqual(1, value["protocol"]["version"])
            return httpx.Response(
                200,
                json=outcome_message(
                    {"status": "completed", "summary": "remote done", "result": {}},
                    1,
                ),
            )

        client = httpx.Client(transport=httpx.MockTransport(handler))
        adapter = HttpAgentConfig(
            "http://127.0.0.1:9000/execute"
        ).build(client=client)
        self.assertEqual("remote done", adapter.execute(assignment()).summary)
        self.assertEqual("task:1:attempt:1", requests[0].headers["idempotency-key"])
        self.assertEqual(
            assignment().effect_scope,
            requests[0].headers["x-agent-bus-effect-scope"],
        )

        unavailable = HttpAgentConfig(
            "http://127.0.0.1:9000/execute"
        ).build(
            client=httpx.Client(
                transport=httpx.MockTransport(lambda request: httpx.Response(503))
            )
        )
        self.assertIsInstance(unavailable.execute(assignment()), RetryableFailure)
        rejected = HttpAgentConfig(
            "http://127.0.0.1:9000/execute"
        ).build(
            client=httpx.Client(
                transport=httpx.MockTransport(lambda request: httpx.Response(400))
            )
        )
        self.assertIsInstance(rejected.execute(assignment()), PermanentFailure)

        oversized = HttpAgentConfig(
            "http://127.0.0.1:9000/execute", max_protocol_bytes=1000
        ).build(
            client=httpx.Client(
                transport=httpx.MockTransport(
                    lambda request: httpx.Response(200, content=b"x" * 1001)
                )
            )
        )
        result = oversized.execute(assignment())
        self.assertEqual("protocol_output_too_large", result.code)

    def test_remote_http_requires_explicit_https_and_secret_reference(self):
        with self.assertRaisesRegex(ValueError, "allow_remote"):
            HttpAgentConfig("https://agent.example/execute").build()
        with self.assertRaisesRegex(ValueError, "HTTPS"):
            HttpAgentConfig(
                "http://agent.example/execute", allow_remote=True, token_env="TOKEN"
            ).build()
        with self.assertRaisesRegex(ValueError, "populated"):
            HttpAgentConfig(
                "https://agent.example/execute",
                allow_remote=True,
                token_env="AGENT_BUS_TEST_MISSING_TOKEN",
            ).build()


class OnboardingTests(unittest.TestCase):
    def test_local_config_url_conflict_is_explicit_and_overridable(self):
        with tempfile.TemporaryDirectory() as directory:
            config = initialize_local_config(directory)
            stdout = io.StringIO()
            stderr = io.StringIO()
            with patch.dict(
                "os.environ",
                {"AGENT_BUS_URL": "http://127.0.0.1:8875"},
            ):
                code = agent_bus_cli.main(
                    ["doctor"],
                    stdout=stdout,
                    stderr=stderr,
                    client_factory=lambda *args, **kwargs: None,
                    local_config_path=config,
                )
            self.assertEqual(2, code)
            self.assertIn("conflicts with local config", stderr.getvalue())
            self.assertIn("pass --url explicitly", stderr.getvalue())

            class HealthyObserver:
                base_url = "http://127.0.0.1:8875"

                def health(self):
                    return {"ok": True, "schema_version": 2}

                def query_all(self, **kwargs):
                    return []

            with patch.dict(
                "os.environ",
                {"AGENT_BUS_URL": "http://127.0.0.1:8875"},
            ):
                code = agent_bus_cli.main(
                    ["doctor", "--url", "http://127.0.0.1:8875"],
                    stdout=io.StringIO(),
                    stderr=io.StringIO(),
                    client_factory=lambda *args, **kwargs: HealthyObserver(),
                    local_config_path=config,
                )
            self.assertEqual(0, code)

    def test_init_is_non_destructive_and_local_config_is_loopback_only(self):
        with tempfile.TemporaryDirectory() as directory:
            path = initialize_local_config(directory)
            config = LocalConfig.from_file(path)
            self.assertEqual("http://127.0.0.1:8765", config.bus_url)
            self.assertEqual(
                (Path(directory) / ".agent-bus" / "events.db").resolve(),
                config.database_path,
            )
            with self.assertRaisesRegex(ValueError, "refusing to overwrite"):
                initialize_local_config(directory)

            value = json.loads(path.read_text(encoding="utf-8"))
            value["bus"]["host"] = "0.0.0.0"
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "loopback"):
                LocalConfig.from_file(path)

    def test_cli_init_and_python_adapter_check_are_standalone(self):
        with tempfile.TemporaryDirectory() as directory:
            stdout = io.StringIO()
            stderr = io.StringIO()
            code = agent_bus_cli.main(
                ["init", directory, "--json"], stdout=stdout, stderr=stderr
            )
            self.assertEqual(0, code, stderr.getvalue())
            self.assertTrue(Path(json.loads(stdout.getvalue())["config"]).exists())

        stdout = io.StringIO()
        stderr = io.StringIO()
        code = agent_bus_cli.main(
            [
                "adapter",
                "check",
                "--python-target",
                "examples.python_agent.agent:ExistingAgent",
            ],
            stdout=stdout,
            stderr=stderr,
        )
        self.assertEqual(0, code, stderr.getvalue())
        self.assertIn("Adapter: PASS", stdout.getvalue())

        stdout = io.StringIO()
        stderr = io.StringIO()
        code = agent_bus_cli.main(
            ["adapter", "check", "--python-target", "missing_package:Agent"],
            stdout=stdout,
            stderr=stderr,
        )
        self.assertEqual(2, code)
        self.assertIn("not importable", stderr.getvalue())

    def test_submit_reports_an_unreachable_local_bus_without_a_traceback(self):
        with tempfile.TemporaryDirectory() as directory:
            config = initialize_local_config(directory)
            stdout = io.StringIO()
            stderr = io.StringIO()
            with patch("client.httpx.post", side_effect=httpx.ConnectError("down")):
                code = agent_bus_cli.main(
                    ["submit", "demo", "--config", str(config)],
                    stdout=stdout,
                    stderr=stderr,
                )
        self.assertEqual(2, code)
        self.assertIn("bus is not reachable", stderr.getvalue())

    def test_mermaid_export_is_read_only_and_escapes_labels(self):
        output = workflow_mermaid(
            {
                "tasks": [
                    {"task_id": 1, "title": 'Prepare <input>', "status": "completed"},
                    {"task_id": 2, "title": 'Use "input"', "status": "open"},
                ],
                "edges": [{"from_task_id": 1, "to_task_id": 2}],
            }
        )
        self.assertIn("flowchart LR", output)
        self.assertIn("Prepare &lt;input&gt;", output)
        self.assertIn("T1 --> T2", output)


if __name__ == "__main__":
    unittest.main()

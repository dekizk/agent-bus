import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from fastapi.testclient import TestClient

from executor_protocol import (
    CURRENT_PROTOCOL_VERSION,
    assignment_message,
    cancellation_message,
    parse_outcome_message,
)
from executors import AssignmentContext
from integration import IntegrationConfig
from examples.http_agent.agent import app, reset_state, snapshot_state


def assignment(*, attempt: int = 1, mode: str = "normal") -> AssignmentContext:
    return AssignmentContext(
        correlation_id="http-example-workflow",
        task_id=7,
        assignment_id=f"task:7:attempt:{attempt}",
        assignment_event_id=100 + attempt,
        attempt=attempt,
        goal="Exercise the HTTP adapter example",
        context={"trial_mode": mode},
        required_capabilities=("http-example",),
        max_retries=1,
        retryable_failures=attempt - 1,
        assignee="minimal-http-agent",
        worker_instance_id="http-example-1",
    )


def request_parts(value: AssignmentContext) -> tuple[dict, dict[str, str]]:
    return assignment_message(value.to_dict(), CURRENT_PROTOCOL_VERSION), {
        "Idempotency-Key": value.assignment_id,
        "X-Agent-Bus-Assignment-Id": value.assignment_id,
        "X-Agent-Bus-Effect-Scope": value.effect_scope,
        "X-Agent-Bus-Protocol-Version": str(CURRENT_PROTOCOL_VERSION),
    }


class HttpExampleTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_state()
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self.client.close()
        reset_state()

    def test_checked_in_config_and_health(self):
        path = Path(__file__).parents[1] / "examples" / "http_agent" / "adapter.json"
        config = IntegrationConfig.from_file(path)

        self.assertEqual("minimal-http-agent", config.worker_name)
        self.assertEqual(("http-example",), config.capabilities)
        self.assertEqual("http://127.0.0.1:9000/execute", config.adapter.endpoint)
        self.assertEqual(
            {"ok": True, "protocol_version": 1},
            self.client.get("/health").json(),
        )

    def test_conformance_and_normal_delivery(self):
        probe = assignment()
        probe_context = probe.to_dict()
        probe_context["context"] = {"agent_bus_conformance_probe": True}
        body = assignment_message(probe_context, CURRENT_PROTOCOL_VERSION)
        _, headers = request_parts(probe)

        response = self.client.post("/execute", json=body, headers=headers)

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            "completed",
            parse_outcome_message(response.json(), 1)["status"],
        )
        self.assertEqual({}, snapshot_state()["effect_executions"])

        normal = assignment()
        body, headers = request_parts(normal)
        response = self.client.post("/execute", json=body, headers=headers)
        outcome = parse_outcome_message(response.json(), 1)
        self.assertEqual("completed", outcome["status"])
        self.assertEqual(1, outcome["result"]["logical_effect_executions"])

    def test_retry_changes_attempt_identity_but_not_effect_identity(self):
        first = assignment(attempt=1, mode="retry_once")
        body, headers = request_parts(first)
        first_response = self.client.post("/execute", json=body, headers=headers)
        self.assertEqual(503, first_response.status_code)

        second = assignment(attempt=2, mode="retry_once")
        body, headers = request_parts(second)
        second_response = self.client.post("/execute", json=body, headers=headers)
        outcome = parse_outcome_message(second_response.json(), 1)

        self.assertNotEqual(first.assignment_id, second.assignment_id)
        self.assertEqual(first.effect_scope, second.effect_scope)
        self.assertEqual("completed", outcome["status"])
        self.assertEqual(1, outcome["result"]["logical_effect_executions"])
        snapshot = snapshot_state()
        self.assertEqual(2, len(snapshot["deliveries"]))
        self.assertEqual([1], list(snapshot["effect_executions"].values()))

    def test_cancellation_endpoint_unblocks_in_flight_delivery(self):
        value = assignment(mode="cancel")
        body, headers = request_parts(value)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                self.client.post,
                "/execute",
                json=body,
                headers=headers,
            )
            deadline = time.monotonic() + 2
            while not snapshot_state()["deliveries"] and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(snapshot_state()["deliveries"])

            cancel_response = self.client.post(
                "/cancel",
                json=cancellation_message(value.assignment_id),
                headers={
                    "Idempotency-Key": f"cancel:{value.assignment_id}",
                    "X-Agent-Bus-Assignment-Id": value.assignment_id,
                },
            )
            execute_response = future.result(timeout=2)

        self.assertEqual(200, cancel_response.status_code)
        outcome = parse_outcome_message(execute_response.json(), 1)
        self.assertEqual("completed", outcome["status"])
        self.assertTrue(outcome["result"]["cancelled"])
        self.assertEqual([value.assignment_id], snapshot_state()["cancellations"])

    def test_identity_header_mismatch_is_rejected(self):
        value = assignment()
        body, headers = request_parts(value)
        headers["X-Agent-Bus-Effect-Scope"] = "wrong"

        response = self.client.post("/execute", json=body, headers=headers)

        self.assertEqual(400, response.status_code)
        self.assertEqual([], snapshot_state()["deliveries"])


if __name__ == "__main__":
    unittest.main()

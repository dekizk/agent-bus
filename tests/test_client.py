import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from client import BusClient


class OffsetTests(unittest.TestCase):
    def test_cancel_task_publishes_a_stable_task_scoped_request(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"id": 7}
        with tempfile.TemporaryDirectory() as directory:
            client = BusClient(
                "http://127.0.0.1:8765",
                actor="human",
                offset_dir=Path(directory),
            )
            with patch("client.httpx.post", return_value=response) as post:
                self.assertEqual(
                    {"id": 7},
                    client.cancel_task(4, reason=" superseded "),
                )

        body = post.call_args.kwargs["json"]
        self.assertEqual("task.cancel_requested", body["topic"])
        self.assertEqual({"task_id": 4, "reason": "superseded"}, body["payload"])
        self.assertEqual("cancel:task:4", body["idempotency_key"])
        with self.assertRaises(ValueError):
            client.cancel_task(0)

    def test_stopped_subscription_does_not_connect(self):
        stop = threading.Event()
        stop.set()
        with tempfile.TemporaryDirectory() as directory:
            client = BusClient(
                "http://127.0.0.1:8765",
                actor="worker",
                offset_dir=Path(directory),
            )
            with patch("client.httpx.stream") as stream:
                self.assertEqual([], list(client.subscribe(stop_event=stop)))
        stream.assert_not_called()

    def test_offsets_are_monotonic_and_use_safe_consumer_names(self):
        with tempfile.TemporaryDirectory() as directory:
            client = BusClient(
                "http://127.0.0.1:8765",
                actor="worker/../../alice",
                offset_dir=Path(directory),
            )
            client.save_offset(10)
            client.save_offset(4)

            self.assertEqual(10, client.load_offset())
            self.assertEqual(Path(directory), client.offset_file.parent)
            self.assertNotIn("/", client.offset_file.name)

    def test_stream_keepalive_invokes_idle_reconciliation_callback(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def raise_for_status(self):
                return None

            def iter_lines(self):
                return iter(
                    [
                        ": keepalive",
                        "",
                        'data: {"id": 7, "topic": "task.created", "payload": {}}',
                    ]
                )

        idle_calls = []
        with tempfile.TemporaryDirectory() as directory:
            client = BusClient(
                "http://127.0.0.1:8765",
                actor="pm",
                offset_dir=Path(directory),
            )
            with patch(
                "client.httpx.stream", return_value=FakeResponse()
            ) as stream_request:
                events = list(
                    client._stream_once(
                        None,
                        0,
                        on_idle=lambda: idle_calls.append("idle"),
                        correlation_id="workflow-one",
                    )
                )

        self.assertEqual(["idle"], idle_calls)
        self.assertEqual([7], [item["id"] for item in events])
        self.assertEqual(
            "workflow-one",
            stream_request.call_args.kwargs["params"]["correlation_id"],
        )

    def test_publish_and_query_send_correlation_filter(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"id": 1}

        with tempfile.TemporaryDirectory() as directory:
            client = BusClient(
                "http://127.0.0.1:8765",
                actor="agent",
                offset_dir=Path(directory),
            )
            with patch("client.httpx.post", return_value=response) as post:
                client.publish(
                    "custom.tick",
                    {},
                    correlation_id="workflow-one",
                    producer={
                        "implementation": "test-client",
                        "instance_id": "process-1",
                        "version": None,
                    },
                )
            with patch("client.httpx.get", return_value=response) as get:
                client.query(correlation_id="workflow-one")

        self.assertEqual(
            "workflow-one",
            post.call_args.kwargs["json"]["correlation_id"],
        )
        self.assertEqual(
            "process-1",
            post.call_args.kwargs["json"]["producer"]["instance_id"],
        )
        self.assertEqual(
            "workflow-one",
            get.call_args.kwargs["params"]["correlation_id"],
        )

    def test_get_event_uses_direct_lookup_endpoint(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"id": 7}
        with tempfile.TemporaryDirectory() as directory:
            client = BusClient(
                "http://127.0.0.1:8765",
                actor="worker",
                offset_dir=Path(directory),
            )
            with patch("client.httpx.get", return_value=response) as get:
                self.assertEqual({"id": 7}, client.get_event(7))
        self.assertEqual("http://127.0.0.1:8765/events/7", get.call_args.args[0])
        with self.assertRaises(ValueError):
            client.get_event(0)

    def test_query_all_preserves_correlation_across_pages(self):
        with tempfile.TemporaryDirectory() as directory:
            client = BusClient(
                "http://127.0.0.1:8765",
                actor="agent",
                offset_dir=Path(directory),
            )
            with patch.object(
                client,
                "query",
                side_effect=[
                    [{"id": 2}],
                    [{"id": 5}],
                    [],
                ],
            ) as query:
                events = client.query_all(
                    page_size=1,
                    correlation_id="workflow-one",
                )

        self.assertEqual([2, 5], [event["id"] for event in events])
        self.assertEqual(
            ["workflow-one", "workflow-one", "workflow-one"],
            [
                call.kwargs["correlation_id"]
                for call in query.call_args_list
            ],
        )


if __name__ == "__main__":
    unittest.main()

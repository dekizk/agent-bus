import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from client import BusClient


class OffsetTests(unittest.TestCase):
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
            with patch("client.httpx.stream", return_value=FakeResponse()):
                events = list(
                    client._stream_once(
                        None,
                        0,
                        on_idle=lambda: idle_calls.append("idle"),
                    )
                )

        self.assertEqual(["idle"], idle_calls)
        self.assertEqual([7], [item["id"] for item in events])


if __name__ == "__main__":
    unittest.main()

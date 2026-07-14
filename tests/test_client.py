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

    def test_cleanup_stale_offsets_keeps_only_current_instance(self):
        with tempfile.TemporaryDirectory() as directory:
            stale = BusClient(
                "http://127.0.0.1:8765",
                actor="alice",
                offset_dir=Path(directory),
                offset_name="alice-old1",
            )
            stale.save_offset(5)
            other_worker = BusClient(
                "http://127.0.0.1:8765",
                actor="bob",
                offset_dir=Path(directory),
                offset_name="bob-live",
            )
            other_worker.save_offset(9)

            current = BusClient(
                "http://127.0.0.1:8765",
                actor="alice",
                offset_dir=Path(directory),
                offset_name="alice-new2",
            )
            current.save_offset(7)
            current.cleanup_stale_offsets("alice")

            remaining = {path.name for path in Path(directory).iterdir()}
            self.assertIn("alice-new2.offset", remaining)
            self.assertIn("bob-live.offset", remaining)
            self.assertNotIn("alice-old1.offset", remaining)
            self.assertNotIn("alice-old1.offset.lock", remaining)
            self.assertEqual(7, current.load_offset())

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

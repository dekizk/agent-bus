"""Event streams notice departed clients and server shutdown promptly."""

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

import bus
from local_config import initialize_local_config

ROOT = Path(__file__).resolve().parents[1]


class FakeRequest:
    def __init__(self):
        self.disconnected = False

    async def is_disconnected(self):
        return self.disconnected


class StreamLoopTests(unittest.TestCase):
    """Drive the SSE generator directly with a stubbed history window."""

    def run_stream(self, scenario, *, poll=0.01, keepalive=0.05, fetch=None):
        calls = []

        def default_fetch(last_id, topics, limit, correlation_id):
            calls.append(last_id)
            return [], last_id, False

        async def main():
            request = FakeRequest()
            with (
                patch.object(bus, "new_event", asyncio.Condition()),
                patch.object(bus, "STREAM_POLL_SECONDS", poll),
                patch.object(bus, "STREAM_KEEPALIVE_SECONDS", keepalive),
                patch.object(bus, "fetch_stream_window", fetch or default_fetch),
                patch.object(bus, "_shutting_down", False),
            ):
                response = await bus.stream(request, from_id=0, topics=None, correlation_id=None)
                return await scenario(response.body_iterator, request)

        return asyncio.run(main()), calls

    def test_keepalive_cadence_and_history_checks_are_unchanged(self):
        async def scenario(stream, request):
            first = await asyncio.wait_for(stream.__anext__(), 1)
            second = await asyncio.wait_for(stream.__anext__(), 1)
            return first, second

        (first, second), calls = self.run_stream(scenario)
        self.assertEqual(": keepalive\n\n", first)
        self.assertEqual(": keepalive\n\n", second)
        # One read on start and one after each keepalive interval (five poll
        # slices here), never one read per slice.
        self.assertEqual(2, len(calls))

    def test_departed_client_is_noticed_within_a_poll_slice(self):
        async def scenario(stream, request):
            waiter = asyncio.ensure_future(stream.__anext__())
            await asyncio.sleep(0.05)
            request.disconnected = True
            started = time.monotonic()
            with self.assertRaises(StopAsyncIteration):
                await asyncio.wait_for(waiter, 1)
            return time.monotonic() - started

        elapsed, _ = self.run_stream(scenario, keepalive=100)
        self.assertLess(elapsed, 0.5)

    def test_shutdown_ends_open_streams_within_a_poll_slice(self):
        async def scenario(stream, request):
            waiter = asyncio.ensure_future(stream.__anext__())
            await asyncio.sleep(0.05)
            bus.begin_shutdown()
            with self.assertRaises(StopAsyncIteration):
                await asyncio.wait_for(waiter, 1)
            return bus._shutting_down

        stopped, _ = self.run_stream(scenario, keepalive=100)
        self.assertTrue(stopped)
        self.assertFalse(bus._shutting_down)  # the patch restored module state

    def test_a_new_event_is_delivered_without_waiting_for_a_slice(self):
        delivered = {"id": 7, "topic": "task.created"}

        def fetch(last_id, topics, limit, correlation_id):
            if bus.event_generation:
                return ([delivered] if last_id < 7 else []), 7, False
            return [], last_id, False

        async def scenario(stream, request):
            waiter = asyncio.ensure_future(stream.__anext__())
            await asyncio.sleep(0.02)
            async with bus.new_event:
                bus.event_generation += 1
                bus.new_event.notify_all()
            return await asyncio.wait_for(waiter, 1)

        with patch.object(bus, "event_generation", 0):
            chunk, _ = self.run_stream(scenario, poll=10, keepalive=100, fetch=fetch)
        self.assertIn('"id": 7', chunk)


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@unittest.skipIf(os.name != "posix", "uses POSIX signals")
class ServeShutdownTests(unittest.TestCase):
    def test_ctrl_c_stops_promptly_with_a_connected_stream(self):
        with tempfile.TemporaryDirectory() as directory:
            config = initialize_local_config(directory)
            value = json.loads(config.read_text())
            port = free_port()
            value["bus"]["port"] = port
            config.write_text(json.dumps(value))
            environment = {k: v for k, v in os.environ.items() if not k.startswith("AGENT_BUS_")}
            environment["PYTHONPATH"] = str(ROOT)
            log = Path(directory, "serve.log")
            with log.open("w") as handle:
                server = subprocess.Popen(
                    [sys.executable, "-m", "agent_bus_cli", "serve", "--config", str(config)],
                    cwd=directory, env=environment, stdout=handle, stderr=subprocess.STDOUT,
                )
            try:
                url = f"http://127.0.0.1:{port}"
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    try:
                        if httpx.get(f"{url}/health", timeout=1).status_code == 200:
                            break
                    except httpx.HTTPError:
                        time.sleep(0.1)
                else:
                    self.fail(f"bus did not start:\n{log.read_text()}")

                connected = threading.Event()

                def follow():
                    try:
                        with httpx.stream("GET", f"{url}/events/stream", timeout=None) as response:
                            connected.set()
                            for _ in response.iter_lines():
                                pass
                    except httpx.HTTPError:
                        pass

                threading.Thread(target=follow, daemon=True).start()
                self.assertTrue(connected.wait(5))
                time.sleep(0.3)

                started = time.monotonic()
                server.send_signal(signal.SIGINT)
                server.wait(timeout=10)
                elapsed = time.monotonic() - started
            finally:
                if server.poll() is None:
                    server.kill()
                    server.wait()
            output = log.read_text()
            self.assertLess(elapsed, 4, output)
            self.assertIn("Finished server process", output)
            self.assertNotIn("Traceback", output)


if __name__ == "__main__":
    unittest.main()

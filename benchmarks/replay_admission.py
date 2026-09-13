"""Credential-free mixed-log replay and actual runtime admission benchmark.

No executors run. The transport shim measures SQLite query/JSON/reducer costs;
it deliberately excludes HTTP, SSE, TLS, and model latency.
"""
import argparse
import json
import platform
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bus
from runtime import WorkerRuntime
from projection_store import load_projection, save_projection


def timed(fn):
    start = time.perf_counter()
    value = fn()
    return value, round((time.perf_counter() - start) * 1000, 3)


def workload(size, shape, worker_count, heartbeat_count):
    rows = []
    def add(topic, actor, payload):
        rows.append((100.0, topic, actor, "benchmark", json.dumps(payload)))
    for i in range(worker_count):
        add("agent.registered", f"worker-{i}", {"name": f"worker-{i}", "instance_id": f"instance-{i}",
            "capacity": 2, "capabilities": []})
    for i in range(1, size + 1):
        deps = [] if i == 1 else [i - 1] if shape == "deep" else [1]
        add("task.created", "benchmark", {"task_id": i, "title": f"Fixture {i}", "depends_on": deps})
        for j in range(heartbeat_count):
            worker = j % worker_count
            add("agent.heartbeat", f"worker-{worker}", {"name": f"worker-{worker}", "instance_id": f"instance-{worker}"})
            # Raw fixture telemetry is excluded by topic, just as live telemetry is.
            add("model.invocation.completed", "benchmark", {"fixture": "ignored"})
    return rows


def run(size, shape, workers, heartbeats):
    previous = bus.DB_PATH
    try:
        with tempfile.TemporaryDirectory(prefix="agent-bus-replay-") as directory:
            bus.DB_PATH = Path(directory) / "events.db"
            bus.init_db()
            rows = workload(size, shape, workers, heartbeats)
            with bus.db() as conn:
                conn.executemany("INSERT INTO events(ts,topic,actor,correlation_id,payload) VALUES (?,?,?,?,?)", rows)
            bus.rebuild_task_index()
            cold, cold_ms = timed(lambda: load_projection(bus.DB_PATH, use_snapshot=False))
            _, build_ms = timed(lambda: save_projection(bus.DB_PATH, cold))
            warm, warm_ms = timed(lambda: load_projection(bus.DB_PATH))
            assert vars(cold.state) == vars(warm.state)
            tracemalloc.start()
            measured = load_projection(bus.DB_PATH, use_snapshot=False)
            _, replay_peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            del measured
            tracemalloc.start()
            measured = load_projection(bus.DB_PATH)
            _, snapshot_peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            del measured

            class Transport:
                actor = "worker-0"
                returned = 0
                calls = 0
                def query_all(self, *, after_id=0, topics=None):
                    # BusClient.query_all materializes history too. This shim
                    # excludes network cost while retaining row/JSON cost.
                    events = bus.fetch_after(after_id, topics, limit=len(rows) + 20)
                    self.returned += len(events)
                    self.calls += 1
                    return events
            transport = Transport()
            runtime = WorkerRuntime(transport, name="worker-0", instance_id="instance-0",
                                    executor=None, capacity=2, log=lambda _: None)
            def assign():
                task = bus.append_event("task.created", "benchmark", {"title": "Admission target"})
                tid = task["payload"]["task_id"]
                return bus.append_event("task.assigned", "pm", {
                    "task_id": tid, "attempt": 1, "assignment_id": f"task:{tid}:attempt:1",
                    "assignee": "worker-0", "worker_instance_id": "instance-0", "goal": "Probe only",
                }, caused_by=task["id"])
            first = assign()
            tracemalloc.start()
            accepted, initial_ms = timed(lambda: runtime._assignment_is_accepted(first))
            _, admission_peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            assert accepted
            initial_rows = transport.returned
            second = assign()
            accepted, incremental_ms = timed(lambda: runtime._assignment_is_accepted(second))
            assert accepted
            return {"tasks": size, "shape": shape, "workers": workers, "events": len(rows),
                    "full_replay_ms": cold_ms, "snapshot_build_ms": build_ms, "snapshot_load_ms": warm_ms,
                    "full_replay_rows": cold.replayed_events, "snapshot_replay_rows": warm.replayed_events,
                    "full_replay_peak_bytes": replay_peak, "snapshot_peak_bytes": snapshot_peak,
                    "initial_admission_ms_with_tracemalloc": initial_ms,
                    "initial_admission_rows": initial_rows, "initial_admission_peak_bytes": admission_peak,
                    "incremental_admission_ms": incremental_ms,
                    "incremental_admission_rows": transport.returned - initial_rows}
    finally:
        bus.DB_PATH = previous


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", type=int, nargs="+", default=[100, 1000])
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--heartbeats", type=int, default=10)
    args = parser.parse_args()
    if min(args.sizes) < 1 or args.workers < 1 or args.heartbeats < 0:
        parser.error("sizes/workers must be positive; heartbeats nonnegative")
    print(json.dumps({"python": platform.python_version(), "platform": platform.platform(),
                      "results": [run(size, shape, args.workers, args.heartbeats)
                                  for size in args.sizes for shape in ("wide", "deep")]}, indent=2))

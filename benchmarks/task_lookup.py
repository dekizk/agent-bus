"""Isolated task lookup/append and coordination replay measurements.

Run from a checkout: .venv/bin/python benchmarks/task_lookup.py
Fixtures use bulk SQL (not measured); measured appends use the public storage API.
No server, agent, credentials, or existing database is used.
"""
import argparse
import json
import platform
import statistics
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bus
from projection import replay_events


def measure(fn, repeats):
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - start) * 1000)
    return round(statistics.median(samples), 4)


def run(size, repeats):
    original = bus.DB_PATH
    try:
        with tempfile.TemporaryDirectory(prefix="agent-bus-benchmark-") as directory:
            bus.DB_PATH = Path(directory) / "events.db"
            bus.init_db()
            with bus.db() as conn:
                conn.executemany(
                    "INSERT INTO events(ts,topic,actor,correlation_id,payload) "
                    "VALUES (1,'task.created','benchmark','benchmark',?)",
                    [(json.dumps({"task_id": i, "title": f"Task {i}"}),)
                     for i in range(1, size + 1)],
                )
                conn.execute("UPDATE counters SET value=? WHERE name='task_id'", (size,))
            # Fixtures bypass append; explicitly regenerate any derived index.
            if hasattr(bus, "rebuild_task_index"):
                bus.rebuild_task_index()
            with bus.db() as conn:
                last = measure(lambda: bus._find_task_created(conn, size), repeats)
                missing = measure(lambda: bus._find_task_created(conn, size + 1000000), repeats)
            append = measure(lambda: bus.append_event(
                "task.created", "benchmark", {"title": "Dependent", "depends_on": [size]},
            ), repeats)
            events = bus.fetch_after(0, ["task.created"], limit=size + repeats)
            replay_ms = measure(lambda: replay_events(events), 3)
            tracemalloc.start()
            state = replay_events(events)
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            assert len(state.tasks) == size + repeats
            return dict(tasks=size, repeats=repeats, late_lookup_ms=last,
                        missing_lookup_ms=missing, dependency_append_ms=append,
                        replay_ms=replay_ms, replay_peak_bytes=peak)
    finally:
        bus.DB_PATH = original


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", type=int, nargs="+", default=[100, 1000, 10000])
    parser.add_argument("--repeats", type=int, default=10)
    args = parser.parse_args()
    if min(args.sizes) < 1 or args.repeats < 1:
        parser.error("sizes and repeats must be positive")
    print(json.dumps({"python": platform.python_version(), "platform": platform.platform(),
                      "results": [run(size, args.repeats) for size in args.sizes]}, indent=2))

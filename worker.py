"""
Worker agent: registers itself, waits for task.assigned events addressed to
it, "works" the task (stub: sleep), then emits task.completed.

Usage: python worker.py <name> [--block <task_id>]
  --block makes the worker block that task once (to demo the
  decision.needed flow).
"""

import sys
import time

from client import BusClient

BUS_URL = "http://127.0.0.1:8765"


def main():
    name = sys.argv[1]
    block_once = int(sys.argv[2].split("=")[1]) if len(sys.argv) > 2 and sys.argv[2].startswith("--block=") else None

    bus = BusClient(BUS_URL, actor=name)
    bus.publish("agent.registered", {"name": name})
    print(f"[{name}] registered, waiting for work", flush=True)

    from_id = bus.load_offset()
    for ev in bus.subscribe(topics=["task.assigned"], from_id=from_id):
        p = ev["payload"]
        if p.get("assignee") != name:
            bus.save_offset(ev["id"])
            continue
        tid = p["task_id"]
        print(f"[{name}] picked up task {tid}: {p.get('goal', '')}", flush=True)
        bus.publish("task.started", {"task_id": tid}, caused_by=ev["id"])
        time.sleep(0.5)  # pretend to work (LLM call goes here)

        if block_once == tid:
            block_once = None
            bus.publish("task.blocked", {"task_id": tid,
                        "reason": "Choose storage backend: SQLite or Postgres"},
                        caused_by=ev["id"])
            print(f"[{name}] BLOCKED task {tid}", flush=True)
        else:
            bus.publish("task.completed", {"task_id": tid,
                        "summary": f"{name} finished: {p.get('goal', '')}"},
                        caused_by=ev["id"])
            print(f"[{name}] completed task {tid}", flush=True)
        bus.save_offset(ev["id"])


if __name__ == "__main__":
    main()

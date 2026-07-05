"""
Project Manager agent: a privileged subscriber on the bus.

State is DERIVED: on startup it replays the full log to rebuild the task
graph, then follows live events. Killing and restarting the PM never loses
work — that's the whole point of log semantics.

Rule-based policy (swap `decide()` for an LLM call later):
  - task.created            -> assign to the least-loaded registered worker
  - task.completed          -> mark done; assign any queued unassigned tasks
  - task.blocked            -> emit decision.needed (human in the loop)
  - decision.made           -> unblock the task and reassign it
  - agent.registered        -> note worker; drain unassigned queue
"""

import fcntl
import sys
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path

from client import BusClient

BUS_URL = "http://127.0.0.1:8765"
LOCK_PATH = Path(__file__).parent / "pm_agent.lock"


class PMState:
    def __init__(self):
        self.workers: set[str] = set()
        self.tasks: dict[int, dict] = {}          # task_id -> {status, assignee, title}
        self.load: dict[str, int] = defaultdict(int)  # worker -> open task count

    def least_loaded(self) -> str | None:
        if not self.workers:
            return None
        return min(sorted(self.workers), key=lambda w: self.load[w])


@contextmanager
def single_pm_lock():
    with LOCK_PATH.open("w") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("[pm] another PM is already running")
        lock_file.write(str(Path.cwd()))
        lock_file.flush()
        yield


def apply_event(state: PMState, ev: dict) -> None:
    """Pure reducer: rebuild derived state from events already in the log."""
    topic, p = ev["topic"], ev["payload"]

    if topic == "agent.registered":
        state.workers.add(p["name"])

    elif topic == "task.created":
        tid = p["task_id"]
        if tid not in state.tasks:
            state.tasks[tid] = {"status": "open", "assignee": None, "title": p.get("title", "")}

    elif topic == "task.assigned":
        t = state.tasks.get(p["task_id"])
        if t and t["assignee"] != p["assignee"]:
            if t["assignee"]:
                state.load[t["assignee"]] -= 1
            t["assignee"], t["status"] = p["assignee"], "assigned"
            state.load[p["assignee"]] += 1

    elif topic == "task.completed":
        t = state.tasks.get(p["task_id"])
        if t and t["status"] != "done":
            t["status"] = "done"
            if t["assignee"]:
                state.load[t["assignee"]] -= 1

    elif topic == "task.blocked":
        t = state.tasks.get(p["task_id"])
        if t and t["status"] != "blocked":
            if t["assignee"]:
                state.load[t["assignee"]] -= 1
            t["status"], t["assignee"] = "blocked", None

    elif topic == "decision.made":
        t = state.tasks.get(p["task_id"])
        if t and t["status"] == "blocked":
            t["status"] = "open"


def plan_emissions(state: PMState, ev: dict) -> list[dict]:
    """Decide what to emit for one live event without mutating PM state."""
    topic, p = ev["topic"], ev["payload"]
    out: list[dict] = []

    if topic == "task.blocked":
        out.append({"topic": "decision.needed", "payload": {
            "task_id": p["task_id"], "reason": p.get("reason", "unspecified")},
            "caused_by": ev["id"],
            "idempotency_key": f"decision-needed:{ev['id']}"})

    planned_load = defaultdict(int, state.load)
    for tid, t in state.tasks.items():
        if t["status"] == "open" and t["assignee"] is None:
            if not state.workers:
                break
            w = min(sorted(state.workers), key=lambda worker: planned_load[worker])
            if w:
                out.append({"topic": "task.assigned", "payload": {
                    "task_id": tid, "assignee": w, "title": t["title"],
                    "goal": t["title"]},  # goal-only handoff, never PM history
                    "caused_by": ev["id"],
                    "idempotency_key": f"assign:{tid}:{ev['id']}"})
                planned_load[w] += 1

    return out


def main():
    with single_pm_lock():
        bus = BusClient(BUS_URL, actor="pm")
        state = PMState()

        history = bus.query(after_id=0)
        head = max((e["id"] for e in history), default=0)
        print(f"[pm] replaying log up to #{head}, then going live...", flush=True)
        for ev in history:
            apply_event(state, ev)

        for ev in bus.subscribe(from_id=head):
            apply_event(state, ev)
            for emit in plan_emissions(state, ev):
                sent = bus.publish(
                    emit["topic"],
                    emit["payload"],
                    caused_by=emit.get("caused_by"),
                    idempotency_key=emit.get("idempotency_key"),
                )
                # Optimistically apply our own emission NOW instead of waiting
                # for it to come back through the stream. Otherwise an unrelated
                # event arriving in between would re-plan against stale state and
                # double-assign the same task (with a different idempotency key,
                # so the bus wouldn't dedupe it). Re-applying the same event when
                # it does arrive via the stream is harmless: apply_event is
                # idempotent (it checks current status/assignee before mutating).
                apply_event(state, sent)
                print(f"[pm] {ev['topic']}#{ev['id']} -> {sent['topic']}#{sent['id']} {sent['payload']}", flush=True)


if __name__ == "__main__":
    sys.exit(main())

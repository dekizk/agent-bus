# agent-bus

A minimal append-only event bus for coordinating local agents, with a
project-manager (PM) agent that assigns work by reacting to events.

The core idea: instead of agents polling a shared kanban board, every action
is an immutable event in a SQLite log. Agents publish events and subscribe to
the stream. All coordination state (who owns which task) is *derived* by
replaying the log — so any process can be killed and restarted without losing
work.

## Components

| File          | Role                                                          |
|---------------|---------------------------------------------------------------|
| `bus.py`      | The bus: FastAPI + SQLite. Append, query, and SSE streaming.  |
| `client.py`   | `BusClient`: publish / subscribe / query + offset persistence. |
| `pm_agent.py` | PM agent: replays the log, then assigns open tasks to the least-loaded worker. |
| `worker.py`   | Demo worker: picks up `task.assigned` events addressed to it. |
| `events.db`   | The event log (SQLite, WAL mode). The single source of truth. |
| `.offsets/`   | Per-consumer resume points (`<actor>.offset`).                |

## Setup

```sh
python3 -m venv .venv          # use Homebrew python3, not system 3.9
. .venv/bin/activate
pip install -r requirements.txt
```

## Quick start

Each in its own terminal (bus first):

```sh
# 1. the bus
uvicorn bus:app --port 8765

# 2. the project manager
python pm_agent.py

# 3. one or more workers
python worker.py alice
python worker.py bob --block=2    # bob will block task 2 once, to demo decisions
```

Then create a task and watch it flow:

```sh
curl -X POST http://127.0.0.1:8765/events \
  -H 'content-type: application/json' \
  -d '{"topic":"task.created","actor":"human","payload":{"title":"demo task"}}'
```

The PM sees `task.created`, emits `task.assigned` to the least-loaded worker;
the worker emits `task.started` then `task.completed`. Every hop is visible in
the log.

## How to use the bus

### Publish an event

```sh
curl -X POST http://127.0.0.1:8765/events \
  -H 'content-type: application/json' \
  -d '{"topic":"task.created","actor":"human","payload":{"title":"my task"},"idempotency_key":"my-task-1"}'
```

Fields:

- `topic` (required) — event type, e.g. `task.created`, `decision.made`.
- `actor` (required) — who is publishing (`human`, `pm`, worker name...).
- `payload` — arbitrary JSON. For `task.created`, omit `task_id` and the
  server assigns the next one atomically.
- `caused_by` — id of the event that triggered this one (audit/causality trail).
- `idempotency_key` — optional. Retrying a publish with the same
  `(actor, idempotency_key)` returns the original event instead of appending
  a duplicate. Use it for anything you might retry.

### Read history

```sh
curl 'http://127.0.0.1:8765/events'                                  # everything
curl 'http://127.0.0.1:8765/events?after_id=15'                      # after event 15
curl 'http://127.0.0.1:8765/events?topics=task.blocked,decision.needed'
```

### Follow live (SSE)

```sh
curl -N 'http://127.0.0.1:8765/events/stream?from_id=0'    # replay all, then follow
curl -N 'http://127.0.0.1:8765/events/stream?from_id=26'   # only new events
```

Leave this running in a terminal as a live dashboard. Ctrl-C to stop.

### Act as the human in the loop

When a worker blocks a task, the PM emits `decision.needed`. Answer it by
publishing `decision.made` (set `caused_by` to the `decision.needed` event id):

```sh
curl -X POST http://127.0.0.1:8765/events \
  -H 'content-type: application/json' \
  -d '{"topic":"decision.made","actor":"human","caused_by":14,"payload":{"task_id":2,"decision":"SQLite"}}'
```

The PM unblocks the task and reassigns it.

### Inspect the raw log

It's just SQLite:

```sh
sqlite3 events.db 'SELECT id,topic,actor,caused_by,payload FROM events ORDER BY id'
```

## Writing your own agent

```python
from client import BusClient

bus = BusClient("http://127.0.0.1:8765", actor="my-agent")
bus.publish("agent.registered", {"name": "my-agent"})

for ev in bus.subscribe(topics=["task.assigned"], from_id=bus.load_offset()):
    if ev["payload"].get("assignee") != "my-agent":
        bus.save_offset(ev["id"])
        continue
    # ... do the work ...
    bus.publish("task.completed",
                {"task_id": ev["payload"]["task_id"], "summary": "done"},
                caused_by=ev["id"])
    bus.save_offset(ev["id"])
```

Notes:

- `subscribe()` reconnects automatically with exponential backoff if the bus
  restarts, resuming from the last event it yielded — no missed or duplicate
  events. You don't need to handle disconnects yourself.
- Call `save_offset()` after handling each event; on restart, pass
  `from_id=bus.load_offset()` to resume exactly where you left off.
- Handlers must be idempotent: after a crash you may re-see the last event.

## Event vocabulary

| Topic              | Emitted by | Meaning                                      |
|--------------------|-----------|----------------------------------------------|
| `task.created`     | human/any | New task. Server assigns `task_id` if omitted. |
| `task.assigned`    | pm        | Task handed to a worker (`assignee`, `goal`). |
| `task.started`     | worker    | Worker began the task.                        |
| `task.completed`   | worker    | Task done (`summary`).                        |
| `task.blocked`     | worker    | Worker needs a decision (`reason`).           |
| `decision.needed`  | pm        | Surfaced to the human.                        |
| `decision.made`    | human     | Answer; PM unblocks and reassigns.            |
| `agent.registered` | worker    | Worker announces itself.                      |

## Crash recovery

Everything is designed to be killable:

- **Bus** — state is in `events.db` (WAL mode); restart it and clients
  reconnect on their own.
- **PM** — on startup it replays the full log through a pure reducer to
  rebuild the task graph, then goes live. Only one PM can run at a time
  (non-blocking file lock on `pm_agent.lock`).
- **Workers** — resume from their saved offset in `.offsets/`.

Restart order after a full stop: bus, then PM, then workers.

## Design notes

- SQLite is the source of truth; all agent state is derived by replay.
- Idempotent publishes are scoped by `(actor, idempotency_key)`, enforced by
  a partial unique index.
- `task_id`s come from a `counters` table via an atomic
  `UPDATE ... RETURNING` (requires SQLite ≥ 3.35).
- The PM applies its own emissions to local state immediately after
  publishing (not when they echo back on the stream), which prevents
  double-assigning a task when unrelated events interleave.
- DB connections are opened per operation and explicitly closed;
  `journal_mode=WAL` is set once at init (it persists), `busy_timeout` per
  connection.

# agent-bus

`agent-bus` is a local, append-only orchestration log for coordinating agents.
It is designed to replace board-style agent management with event-driven
ownership, execution attempts, recovery, and human decisions.

The bus does not store a mutable kanban card as truth. Every action is an
immutable SQLite event. The project manager (PM) derives current state by
replaying those events, then reconciles any effects that are missing. Processes
can therefore restart without relying on hidden in-memory ownership state.

## Identity model

Four identities keep workflows, retries, and replays unambiguous:

- `correlation_id` identifies one wider workflow or user goal across tasks.
- `task_id` identifies the logical outcome requested by a human or agent.
- `assignment_id` identifies one execution attempt for that task.
- `instance_id` identifies one running worker process.

A correlation may contain several tasks, and each task may have several
attempts. `caused_by` remains the direct parent event; it is not overloaded as
the workflow identifier.

A task may have several attempts but only its current attempt may change task
state. A late completion from an expired worker is retained in the audit log
but rejected by the PM projection.

The normal lifecycle is:

```text
task.created
    -> task.assigned (attempt 1)
    -> task.started
    -> task.completed
```

Recovery and human-input paths are explicit events:

```text
task.assigned -> task.assignment_expired -> task.assigned (attempt 2)
task.started  -> task.blocked -> decision.needed -> decision.made
              -> task.assigned (next attempt)
```

## Components

| File | Role |
|---|---|
| `bus.py` | FastAPI, SQLite event storage, validation, queries, and SSE streaming |
| `client.py` | Publish, bounded history reads, reconnecting subscriptions, durable offsets |
| `pm_agent.py` | Pure projection plus deterministic post-replay reconciliation |
| `worker.py` | Demo leased worker with idempotent lifecycle emissions |
| `events.db` | Append-only event log and task-id counter |
| `.offsets/` | Optional durable resume points for stable consumer identities |

## Setup

```sh
python3 -m venv .venv          # Python 3.10+
. .venv/bin/activate
pip install -r requirements.txt
```

Create the environment in `.venv`; do not use the repository root itself as a
virtual-environment directory.

## Quick start

Run each process in its own terminal, bus first:

```sh
uvicorn bus:app --port 8765
python pm_agent.py
python worker.py alice
python worker.py bob --block 2
```

Create a task:

```sh
curl -X POST http://127.0.0.1:8765/events \
  -H 'content-type: application/json' \
  -d '{
    "topic":"task.created",
    "actor":"human",
    "idempotency_key":"task:demo",
    "payload":{"title":"Demonstrate crash-safe orchestration"}
  }'
```

The server assigns `task_id` atomically. The PM assigns a live worker and emits
an `assignment_id`; the worker includes that assignment and its process
`instance_id` in every subsequent lifecycle event. The server also generates a
`correlation_id` for the root task and propagates it through the resulting
event chain.

Workers can advertise scheduling metadata:

```sh
python worker.py coder --capacity 2 --capability python --capability testing
```

A task may require those capabilities:

```json
{
  "topic": "task.created",
  "actor": "human",
  "payload": {
    "title": "Run the Python test suite",
    "required_capabilities": ["python", "testing"]
  }
}
```

## Human decisions

When a worker emits `task.blocked`, the PM deterministically emits one
`decision.needed` for that assignment. Read the event and copy its identifiers
into the response:

```sh
curl 'http://127.0.0.1:8765/events?topics=decision.needed'
```

```sh
curl -X POST http://127.0.0.1:8765/events \
  -H 'content-type: application/json' \
  -d '{
    "topic":"decision.made",
    "actor":"human",
    "caused_by":14,
    "idempotency_key":"decision:14",
    "payload":{
      "task_id":2,
      "assignment_id":"task:2:attempt:1",
      "decision_id":"decision:task:2:attempt:1",
      "decision":"SQLite"
    }
  }'
```

The PM reopens the logical task and creates a new assignment attempt. The old
attempt can no longer complete it.

## Workflow correlation

`correlation_id` is a top-level event-envelope field. A root `task.created`
receives a random correlation automatically when the caller omits one. A new
event with `caused_by` inherits its parent's correlation inside the same
append transaction, so PM agents and workers do not need to copy the value
manually.

Callers may provide a correlation explicitly to group several root tasks under
one wider goal:

```json
{
  "topic": "task.created",
  "actor": "human",
  "correlation_id": "release-2026-07",
  "payload": {"title": "Run integration tests"}
}
```

New events must reference an existing `caused_by` event. If both a child and
its parent have correlations, they must match. `task_id` still identifies only
one task; it is never used as a workflow identifier.

## Crash recovery guarantees

### PM

At startup the PM:

1. reads the complete log in bounded pages;
2. rebuilds workers, tasks, attempts, and decisions through a total reducer;
3. reconciles missing effects before waiting for another event;
4. publishes effects with stable logical idempotency keys.

This closes the crash window where the prototype could record `task.created` or
`task.blocked` but fail before publishing the corresponding assignment or
decision request.

### Workers

Workers publish lifecycle events using keys derived from `assignment_id`:

```text
started:{assignment_id}
blocked:{assignment_id}
completed:{assignment_id}
```

A new worker instance subscribes from its own registration event — assignments
addressed to it cannot exist earlier — so startup cost does not grow with log
history. `BusClient` retains the latest event id in memory across SSE
reconnections. A replacement process registers a new `instance_id`, so demo
workers neither resume the previous process's assignments nor create durable
offset files. Stable consumers that do need cross-process resume can use
`BusClient.load_offset()` and `save_offset()`; those files are replaced
atomically and only move forward. After all pre-v0.2.1 workers have stopped,
their old per-instance files may be removed once; new demo workers do not
replenish them.

An external tool call, payment, deployment, or file mutation performed by a
worker must also use an idempotency token. The bus can make orchestration
effects idempotent; it cannot make an arbitrary external side effect atomic.

### Worker leases

Each worker start gets a new random `instance_id` and publishes heartbeats. The
PM expires an active attempt when the registered process is replaced or its
lease becomes stale, then creates the next attempt when a capable worker is
available.

The PM also reconciles on SSE keepalives, so lease expiry does not depend on a
new task or heartbeat arriving after a worker dies.

Defaults can be changed with:

```sh
AGENT_BUS_HEARTBEAT_SECONDS=5
AGENT_BUS_WORKER_LEASE_SECONDS=20
AGENT_BUS_URL=http://127.0.0.1:8765
```

Lease decisions are emitted into the log as `task.assignment_expired`, so task
history remains replayable and auditable.

## Versioned event contracts

New built-in events use top-level `schema_version: 2` by default. Known v2
topics are validated before they enter the log; malformed events cannot poison
every future PM replay. Unknown topics remain permitted so applications can
extend the bus without changing its core.

Existing databases are migrated automatically. Historical rows are marked as
schema v1 and remain replayable with legacy assignment identities derived from
their event ids. Existing events retain `NULL` correlation IDs; the migration
does not invent workflow relationships that were never recorded.

Core v2 topics:

| Topic | Emitted by | Purpose |
|---|---|---|
| `agent.registered` | worker | Announces a process instance, capabilities, and capacity |
| `agent.heartbeat` | worker | Renews that process instance's lease |
| `task.created` | human/agent | Requests a logical outcome |
| `task.assigned` | PM | Creates a numbered execution attempt |
| `task.started` | worker | Confirms the active attempt began |
| `task.completed` | worker | Completes the active attempt and logical task |
| `task.blocked` | worker | Pauses the attempt for human input |
| `task.assignment_expired` | PM | Records loss or replacement of the assigned worker |
| `decision.needed` | PM | Requests one human decision for a blocked attempt |
| `decision.made` | human | Resolves that decision and permits a new attempt |

## Reading and following the log

History queries are bounded; use `after_id` to paginate:

```sh
curl 'http://127.0.0.1:8765/events?after_id=0&limit=1000'
curl 'http://127.0.0.1:8765/events?topics=task.assigned,task.completed'
curl 'http://127.0.0.1:8765/events?correlation_id=release-2026-07'
```

A response with header `X-Page-Full: 1` filled its page, so more events may
exist — request the next page with `after_id` set to the last event id
received (`BusClient.query_all` does this automatically).

Follow history and live events over SSE:

```sh
curl -N 'http://127.0.0.1:8765/events/stream?from_id=0'
curl -N 'http://127.0.0.1:8765/events/stream?correlation_id=release-2026-07'
```

SSE history is scanned in bounded global-id windows. Filtered subscribers
advance past unrelated events rather than repeatedly rescanning the same tail.
`BusClient.query()`, `query_all()`, and `subscribe()` expose the same
`correlation_id=` filter.

## Trust model

The bus is designed for local, single-user orchestration. Two boundaries to
understand before exposing it any wider:

- **Perimeter auth is opt-in.** Set `AGENT_BUS_TOKEN` in the bus's environment
  and every data route requires `Authorization: Bearer <token>`; `BusClient`
  picks the same variable up automatically (or accepts `token=`). `/health`
  stays open. Without the variable, anyone who can reach the port can publish.
- **Actors are self-reported.** The token authenticates *clients*, not
  identities: any authorized client may publish under any actor string,
  including `pm`. Per-actor authentication would require signed events or
  per-actor credentials, which is out of scope for a local bus.

The PM's single-instance lock is per user, per machine, per bus URL (a lock
file in the user's runtime directory keyed by a hash of `AGENT_BUS_URL`), so
two checkouts on one machine exclude each other. The lock file is opened
without following symlinks, verified as owned by the current user, and made
user-only. It is not distributed leadership: PMs on different machines or
under different OS users are not excluded and must not coordinate the same bus.

## Testing

```sh
pip install -r requirements-dev.txt
python -m unittest discover -v   # or: pytest tests/
```

The suite focuses on orchestration failures: restart reconciliation, stale
attempt rejection, worker replacement, malformed replay events, idempotent
publish retries, correlation propagation and migration, and atomic monotonic
offsets.

## Scope and next boundaries

v0.2 remains a trusted, single-host runtime. Actor names are asserted by
clients, not authenticated identities. The PM lock and wake-up condition are
local-process mechanisms. Before exposing the bus to other machines, add
authentication and replace local exclusivity/notification with shared
infrastructure.

Likely next layers are task dependencies, cancellation, deadlines, artifacts,
progress events, executor adapters, snapshots, and a read-only operational UI.
Those should remain projections and commands over the log—not a return to a
mutable board as the source of truth.

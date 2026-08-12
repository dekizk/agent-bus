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
state. The worker runtime suppresses results after known ownership loss. If a
result races with expiry and still reaches the log, the PM projection rejects
it as stale.

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
task.started  -> task.attempt_failed -> task.assigned (next attempt)
              -> task.failed -> task.retry_requested
              -> task.assigned (next monotonic attempt)
task.started  -> task.blocked -> decision.needed -> decision.made
              -> task.assigned (next attempt)
```

## Components

| File | Role |
|---|---|
| `bus.py` | FastAPI, SQLite event storage, validation, queries, and SSE streaming |
| `client.py` | Publish, bounded history reads, reconnecting subscriptions, durable offsets |
| `pm_agent.py` | Pure projection plus deterministic post-replay reconciliation |
| `executors.py` | Immutable assignment/outcome contract and Python/subprocess adapters |
| `runtime.py` | Leased, concurrent worker runtime with ownership-loss cancellation |
| `adoption.py` | Controlled, shadow, and deterministic canary integration helpers |
| `topics.py` | Canonical coordination and integration topic groups |
| `worker.py` | Demo executor wired through the reusable runtime |
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
python worker.py carol --fail 3
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
event chain. New tasks store their retry policy in the event itself; the
default is two retries after the initial attempt.

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

Inline executor context is also carried from task creation into every
assignment:

```json
{
  "topic": "task.created",
  "actor": "human",
  "payload": {
    "title": "Run the Python test suite",
    "context": {"repository": "agent-bus", "suite": "integration"},
    "required_capabilities": ["python", "testing"]
  }
}
```

Context and final structured results are limited to 16 KiB of encoded JSON.
They are coordination data, not storage for prompts, transcripts, or large
artifacts; content-addressed blob storage belongs in a separate
telemetry/artifact layer.

## Integrating an existing agent

Executors receive one immutable `AssignmentContext` and return exactly one of
`Completed`, `Blocked`, `RetryableFailure`, or `PermanentFailure`. The runtime
owns registration, heartbeats, subscriptions, bounded concurrency, lifecycle
idempotency, and stale-result suppression.

`AssignmentContext.decisions` is a chronological, immutable tuple of accepted
human-decision records. Each record identifies the `decision.made` event, its
actor, the blocked assignment and decision IDs, and the JSON decision value.
Executors must consult these records before returning `Blocked`: later accepted
decisions supersede conflicting earlier decisions or original context, and a
clear decision satisfies a corresponding null or missing context value. The
bus preserves this precedence rule without mutating the original task event.
Historical assignments created before v0.4.1 parse this field as an empty
tuple.

An existing Python agent with a `run()` method can be wrapped without teaching
it about the bus:

```python
from client import BusClient
from executors import Completed, InProcessExecutor
from runtime import WorkerRuntime

class ExistingAgent:
    def run(self, assignment):
        result = do_existing_work(
            goal=assignment.goal,
            context=assignment.context,
            idempotency_key=assignment.assignment_id,
        )
        return Completed("agent finished", {"result_ref": result.ref})

bus = BusClient("http://127.0.0.1:8765", actor="existing-agent")
WorkerRuntime(
    bus,
    name="existing-agent",
    executor=InProcessExecutor(ExistingAgent()),
    capacity=2,
    capabilities=["python"],
).run()
```

`capacity` is enforced by a bounded execution pool, so the worker never runs
more assignments concurrently than it advertises. Unexpected Python
exceptions default to permanent failure; applications may explicitly configure
them as retryable.

### Subprocess agents

`SubprocessExecutor(["agent-command", "--json"])` sends one assignment JSON
object on stdin. Exit code `0` must return one of these JSON objects on stdout:

```json
{"status":"completed","summary":"done","result":{"ref":"artifact-1"}}
{"status":"blocked","reason":"approval required"}
{"status":"retryable_failure","code":"rate_limited","reason":"try later"}
{"status":"permanent_failure","code":"invalid_goal","reason":"cannot run"}
```

Exit code `75` maps to a retryable process failure; other non-zero exits map to
permanent failure. Input, stdout, and stderr are size-limited. Timeouts are
retryable. When assignment ownership is lost or the runtime stops, a running
subprocess is terminated and its result is suppressed. Shell execution is not
used.

In-process Python threads cannot be force-killed safely. Such agents should
make external side effects idempotent with `assignment_id` and expose their own
cooperative cancellation when needed.

## Safe adoption modes

`AdoptionBridge` records a stable decision for work originating in another
system:

```python
from adoption import AdoptionBridge, AdoptionMode, CanarySelector, ExternalOrigin

event = AdoptionBridge(bus).adopt(
    origin=ExternalOrigin("legacy-system", "ticket-42"),
    title="Handle ticket 42",
    mode=AdoptionMode.CANARY,
    selector=CanarySelector(10, namespace="first-rollout"),
    context={"priority": "high"},
)
```

- **Controlled:** emits `task.created`; agent-bus owns execution.
- **Shadow:** emits `integration.task_observed`; the external system remains
  owner and agent-bus does not create an executable task.
- **Canary:** a stable hash selects a configured percentage. Selected work is
  bus-owned; unselected work is recorded as externally owned.

The bridge uses the same idempotency identity for either ownership result. A
later attempt to reinterpret the same external origin as a different owner is
therefore rejected instead of producing two tasks. The external system must
honour the recorded decision: bus-owned work must be removed from its own
execution queue. Shadow mode must never invoke a second side-effecting agent.

That protection is scoped to the publishing actor because bus idempotency keys
are unique by `(actor, idempotency_key)`. Every replica participating in one
adoption rollout must therefore use the same bridge actor. Canary percentages
and `include_refs` affect only external origins that have not yet been adopted;
v0.4 deliberately has no ownership-transfer operation for an existing origin.

Deadlines are intentionally not part of `AssignmentContext` yet. A timestamp
without PM-enforced expiry semantics would imply a guarantee the bus does not
currently provide.

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

The PM accepts the response only for the current pending decision, records it
in its replay projection, reopens the logical task, and creates a new assignment
attempt. The assignment contains a chronological `decisions` array, so a
stateless or replacement executor can use the human input without mutating the
original task context. The old attempt can no longer complete the task.

Keep decisions compact coordination data. Prompts, transcripts, and large
artifacts still belong in an external telemetry/artifact layer.

## Bounded retries and terminal failure

Every new `task.created` records a replayable retry policy:

```json
{
  "topic": "task.created",
  "actor": "human",
  "payload": {
    "title": "Run integration tests",
    "retry_policy": {"max_retries": 2}
  }
}
```

`max_retries` counts retries after the initial assignment. A value of `0`
allows only the initial attempt. The server materializes
`AGENT_BUS_MAX_RETRIES` (default `2`) when the caller omits the policy, so a
later process configuration change cannot alter replayed task behavior.
Historical tasks that predate this field remain unlimited rather than being
silently assigned a new policy.

A lease expiry and a retryable `task.attempt_failed` each consume one retry.
A permanent attempt failure does not spin: the PM immediately reconciles it
to terminal `task.failed`. Blocking for a human decision does not consume the
retry budget.

To revive a failed task, append `task.retry_requested` as a child of its latest
`task.failed` event:

```sh
curl -X POST http://127.0.0.1:8765/events \
  -H 'content-type: application/json' \
  -d '{
    "topic":"task.retry_requested",
    "actor":"human",
    "caused_by":21,
    "idempotency_key":"retry:task:2:failed:21",
    "payload":{
      "task_id":2,
      "additional_retries":1,
      "reason":"The dependency is healthy again"
    }
  }'
```

This records exactly the requested number of new assignment opportunities; it
never resets `attempt`. After retry exhaustion that extends the previous
budget. After a permanent failure it does not silently restore the unused
automatic budget. The next assignment therefore has a new, monotonically
increasing `assignment_id`, and late output from any failed attempt remains
unable to complete the task.

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

1. reads coordination topics in bounded, server-filtered pages;
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
attempt-failed:{assignment_id}
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

The reusable runtime also follows assignment-expiry, terminal-failure, and
replacement-registration events. It cancels adapters that support cancellation
and suppresses lifecycle output once that instance no longer owns an attempt.

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
AGENT_BUS_MAX_RETRIES=2
AGENT_BUS_URL=http://127.0.0.1:8765
```

Lease decisions are emitted into the log as `task.assignment_expired`, so task
history remains replayable and auditable. Assignment expiry and explicit
retryable worker failure share the same bounded recovery policy.

## Versioned event contracts

New built-in events use top-level `schema_version: 2` by default. Known v2
topics are validated before they enter the log; malformed events cannot poison
every future PM replay. Unknown topics remain permitted so applications can
extend the bus without changing its core.

Existing databases are migrated automatically. Historical rows are marked as
schema v1 and remain replayable with legacy assignment identities derived from
their event ids. Existing events retain `NULL` correlation IDs; the migration
does not invent workflow relationships that were never recorded.

As a v0.4 publisher-contract change, new `task.created` events must contain a
non-empty `payload.title`. Historical titleless events remain replayable and
receive a deterministic fallback title in the PM projection.

Core v2 topics:

| Topic | Emitted by | Purpose |
|---|---|---|
| `agent.registered` | worker | Announces a process instance, capabilities, and capacity |
| `agent.heartbeat` | worker | Renews that process instance's lease |
| `task.created` | human/agent | Requests a logical outcome |
| `task.assigned` | PM | Creates a numbered execution attempt with prior human decisions |
| `task.started` | worker | Confirms the active attempt began |
| `task.completed` | worker | Completes the active attempt and logical task |
| `task.blocked` | worker | Pauses the attempt for human input |
| `task.attempt_failed` | worker | Records a retryable or permanent execution failure |
| `task.assignment_expired` | PM | Records loss or replacement of the assigned worker |
| `task.failed` | PM | Terminates a task after policy exhaustion or permanent failure |
| `task.retry_requested` | human/agent | Extends policy and reopens the latest failed task |
| `decision.needed` | PM | Requests one human decision for a blocked attempt |
| `decision.made` | human | Records a response that is carried into the next attempt |

Integration topics are validated separately and deliberately excluded from PM
replay:

| Topic | Emitted by | Purpose |
|---|---|---|
| `integration.task_observed` | bridge | Records externally owned shadow or unselected canary work |

`topics.py` is the canonical source for both groups. Tests assert that the PM
uses exactly the coordination group and never consumes integration topics.

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

SSE history applies topic and correlation filters in SQLite before payloads
are decoded. High-volume extension topics therefore do not inflate PM replay
or force filtered consumers to deserialize unrelated events.
`BusClient.query()`, `query_all()`, and `subscribe()` expose the same filters.

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
attempt rejection, retry exhaustion, permanent failures, human revival,
worker replacement, malformed replay events, idempotent publish retries,
correlation propagation and migration, SQL-level stream filtering, executor
conformance, bounded concurrency, subprocess timeout/cancellation, stable
canaries, single-owner adoption, real-agent integration, and atomic monotonic
offsets.

## Scope and next boundaries

v0.4.1 remains a trusted, single-host runtime. Actor names are asserted by
clients, not authenticated identities. The PM lock and wake-up condition are
local-process mechanisms. Before exposing the bus to other machines, add
authentication and replace local exclusivity/notification with shared
infrastructure.

The next version will be selected after several genuine integration trials.
Candidate layers include task dependencies and DAG orchestration, and model/tool
telemetry with content-addressed blob storage. Cancellation, deadlines,
snapshots, and a read-only operational UI remain later candidates. Trial
evidence, rather than version numbering alone, should determine their order.
Those features should remain projections and commands over the log—not a return
to a mutable board as the source of truth.

# agent-bus

`agent-bus` is a local, append-only orchestration log for coordinating agents.
It is designed to replace board-style agent management with event-driven
ownership, execution attempts, recovery, and human decisions.

See [ROADMAP.md](ROADMAP.md) for the product direction, adoption principles,
and remaining path from the current local control plane to v1.0.

The bus does not store a mutable kanban card as truth. Every action is an
immutable SQLite event. The project manager (PM) derives current state by
replaying those events, then reconciles any effects that are missing. Processes
can therefore restart without relying on hidden in-memory ownership state.

v0.8 adds an installable, read-only operations CLI over that same replay. It
shows task and workflow state, worker leases, DAG readiness, retry allowance,
human-decision waits, and workflow usage without creating a second database or
letting the display become an orchestration authority.

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

Tasks may also form a directed acyclic graph (DAG). A dependent remains
unassigned until every declared upstream task has completed:

```text
task.created (A) -> task.completed (A)
                                  -> task.assigned (B, depends_on A)
task.failed (A)                   -> task.dependency_failed (B)
```

Recovery and human-input paths are explicit events:

```text
task.assigned -> task.assignment_expired -> task.assigned (attempt 2)
task.started  -> task.attempt_failed -> task.assigned (next attempt)
              -> task.failed -> task.retry_requested
              -> task.assigned (next monotonic attempt)
task.started  -> task.blocked -> decision.needed -> decision.made
              -> task.assigned (next attempt)
task.created/assigned/started/blocked -> task.cancel_requested -> task.cancelled
task.created/assigned/started/blocked -> task.deadline_exceeded
```

## Components

| File | Role |
|---|---|
| `bus.py` | FastAPI, SQLite event storage, validation, queries, and SSE streaming |
| `client.py` | Publish, bounded history reads, reconnecting subscriptions, durable offsets |
| `projection.py` | Pure coordination reducer shared by the PM and operator tools |
| `pm_agent.py` | Deterministic post-replay reconciliation and single-PM runtime |
| `operations.py` | Task explanations, worker health, DAG views, and telemetry totals |
| `observer.py` | GET-only history and SSE client with no offsets or local state |
| `agent_bus_cli.py` | Human-friendly `agent-bus` operations command with JSON output |
| `executors.py` | Immutable assignment/outcome contract and Python/subprocess adapters |
| `runtime.py` | Leased, concurrent worker runtime with ownership-loss cancellation |
| `adoption.py` | Controlled, shadow, and deterministic canary integration helpers |
| `telemetry.py` | Optional model/tool telemetry sink, lifecycle helpers, and producer identity |
| `artifacts.py` | Atomic, content-addressed local storage for opt-in captured content |
| `topics.py` | Canonical coordination, integration, and telemetry topic groups |
| `worker.py` | Demo executor wired through the reusable runtime |
| `events.db` | Append-only event log and task-id counter |
| `.offsets/` | Optional durable resume points for stable consumer identities |

## Setup

```sh
python3 -m venv .venv          # Python 3.10+
. .venv/bin/activate
python -m pip install -e .
```

Create the environment in `.venv`; do not use the repository root itself as a
virtual-environment directory. The editable install provides the `agent-bus`
command while keeping this checkout as the source. The project is not yet
claiming a published PyPI release; `pip install agent-bus` becomes the intended
path once release packaging is published and hardened.

## Quick start

Run each process in its own terminal, bus first:

```sh
python -m uvicorn bus:app --port 8765
python -m pm_agent
python -m worker alice
python -m worker bob --block 2
python -m worker carol --fail 3
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

The server stores `events.db` in the directory where it is started. Set
`AGENT_BUS_DB_PATH` to an explicit file path when another location is desired.

## Read-only operations CLI

The v0.8 commands read the public HTTP/SSE API only. They never publish events,
create consumer offsets, cache a mutable projection, or participate in
scheduling:

```sh
agent-bus doctor
agent-bus workers
agent-bus task 1
agent-bus explain 1
agent-bus workflow YOUR_CORRELATION_ID
agent-bus tail YOUR_CORRELATION_ID
```

`task` shows current state, attempt ownership, dependencies, retry allowance,
deadline, and the event ids supporting its explanation. `workflow` renders all
tasks and dependency edges for one correlation and totals model tokens, cost,
duration, failures, and still-open spans from the separate telemetry stream.
When usage or cost was not reported, the CLI says so instead of displaying a
misleading zero.

All finite commands accept `--json` before or after the command for scripts:

```sh
agent-bus task 1 --json
agent-bus --json workflow YOUR_CORRELATION_ID
```

The CLI uses `AGENT_BUS_URL`, `AGENT_BUS_TOKEN`, and
`AGENT_BUS_WORKER_LEASE_SECONDS` by default. The equivalent `--url`, `--token`,
and `--lease-seconds` flags are available for one invocation. `doctor` reports
bus/schema health, replay counts, worker lease health, safely ignored stale
events, and tasks that appear to be waiting for PM reconciliation.

Workers can advertise scheduling metadata:

```sh
python -m worker coder --capacity 2 --capability python --capability testing
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

Context and each final structured result are limited to 16 KiB of encoded JSON.
Resolved dependency input is limited to 32 KiB in aggregate.
They are coordination data, not storage for prompts, transcripts, or large
artifacts. v0.6 keeps that content out of SQLite by default and can store
explicitly opted-in captures in a separate content-addressed artifact store.

## Cancellation and deadlines

v0.7 adds durable management controls without introducing mutable task state.
Cancellation is a command event followed by a PM-derived terminal event:

```sh
curl -X POST http://127.0.0.1:8765/events \
  -H 'content-type: application/json' \
  -d '{
    "topic":"task.cancel_requested",
    "actor":"human",
    "idempotency_key":"cancel:task:1",
    "payload":{"task_id":1,"reason":"superseded by a new request"}
  }'
```

Python publishers can call `BusClient.cancel_task(task_id, reason=...)`, which
uses the same stable task-scoped idempotency key. The server verifies that the
task exists and inherits its `correlation_id`. The PM then emits exactly one
`task.cancelled`, including the last assignment identity when the task was
active. A request recorded after a task is already terminal is retained in the
log but does not rewrite its outcome.

An optional absolute Unix timestamp can bound the whole logical task:

```json
{
  "topic": "task.created",
  "actor": "human",
  "payload": {
    "title": "Finish before the release cutoff",
    "deadline_at": 2000000000.0
  }
}
```

The deadline is persisted on `task.created`, copied into every assignment, and
exposed as `AssignmentContext.deadline_at`. When it is reached, the PM emits one
`task.deadline_exceeded` instead of assigning or retrying the task. Cancellation
and deadline termination do not consume retry budget. They also terminate
blocked work without creating another human-decision request.

Event order is authoritative: a completion already recorded before a
cancellation request remains completed; once the cancellation request is
accepted by the reducer, later attempt output is stale. Worker outcomes whose
event timestamp is at or after the persisted deadline are likewise ignored by
the PM projection. Executor timeouts and worker lease expiry remain separate
retryable attempt failures—they are not task deadlines.

The runtime arms a local timer from the persisted assignment deadline, and also
follows cancellation requests and terminal control events. It terminates
subprocess adapters, invokes cooperative `cancel(assignment_id)` on in-process
adapters that provide it, and suppresses late lifecycle output. This stops
local execution at the cutoff even if PM reconciliation is temporarily down;
the PM remains responsible for recording the authoritative terminal event.
Python threads cannot be force-killed, so side effects must still be
idempotent. Cancelled or deadline-exceeded prerequisites propagate
`task.dependency_failed` to downstream DAG tasks. v0.7 does not revive these
terminal tasks; create a new task to record new intent.

## Telemetry and artifacts

Telemetry is observational and is never consumed by the PM. Model and tool
lifecycles use their own canonical topic group, so coordination replay remains
small even when an executor emits many telemetry events. Each telemetry event
retains `correlation_id`, `task_id`, `assignment_id`, worker process identity,
and a stable invocation or tool-call id. The top-level `producer` envelope
identifies the implementation and process that emitted it; provider and model
identity remain invocation fields.

Executors can use the framework-neutral sink without depending on PM internals:

```python
from telemetry import BusTelemetrySink, ProducerIdentity

telemetry = BusTelemetrySink(
    bus,
    producer=ProducerIdentity(
        implementation="my-agent-adapter",
        instance_id=worker_instance_id,
        version="1.0.0",
    ),
)
```

The sink supports `model_started/completed/failed` and
`tool_started/completed/failed`. Started and terminal events use deterministic
idempotency keys. A terminal event is caused by its start event when available,
and all events retain the assignment workflow correlation. Telemetry publishing
must remain best-effort from an executor: an observability outage must not alter
task ownership or outcome.

Prompts, outputs, tool arguments, credentials, and transcripts are never added
to telemetry metadata automatically. Content capture is opt-in and requires an
`ArtifactStore`; events then contain only `{sha256, size_bytes, media_type,
kind}` references. Files are written atomically, deduplicated by SHA-256, made
user-only, and verified on reads. One artifact is limited to 16 MiB.

The store performs no automatic deletion. A digest can be shared by several
immutable events, so retention may delete only blobs proven unreferenced by the
event log. Treat the artifact directory as sensitive local data: use a private
directory, redact before capture, never capture credentials, and apply your own
backup and retention policy. `artifacts/` is ignored by Git.

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

`AssignmentContext.dependencies` is an immutable tuple of resolved upstream
completions in the task's declared `depends_on` order. Each entry contains the
upstream `task_id`, immutable `completion_event_id`, summary, and structured
result. Executors consume this field directly; publishers do not copy upstream
results into downstream context.

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
make external side effects idempotent with `assignment_id`. If the wrapped
object exposes `cancel(assignment_id)` or `close()`, `InProcessExecutor`
forwards those lifecycle calls.

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
    deadline_at=2000000000.0,
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

Controlled and canary tasks may include `deadline_at`; adapters receive the
persisted value through `AssignmentContext` and should treat the PM's terminal
deadline event as the ownership boundary.

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
artifacts belong in the separate content-addressed store and should appear in
telemetry only as compact references.

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

## DAG orchestration

A task declares dependency edges when it is created:

```json
{
  "topic": "task.created",
  "actor": "human",
  "payload": {
    "title": "Plan fixes from the audit",
    "depends_on": [4],
    "required_capabilities": ["hermes"]
  }
}
```

Every dependency must already exist, appear only once, and belong to the same
`correlation_id`. Because new nodes may point only to existing nodes, the bus
cannot accept a cycle. A dependent with no explicit correlation inherits the
workflow identity of its dependencies. Fan-in is capped at 128 direct
dependencies per task. These rules support chains, fan-in, and fan-out while
keeping the graph reconstructible from immutable `task.created` events; there
is no mutable DAG table or board.

The PM derives readiness during replay. It assigns a task only after every
dependency has a valid `task.completed` event. The persisted `task.assigned`
contains compact `dependency_refs` pairs (`task_id` and `completion_event_id`),
not copied results. The worker retrieves those immutable completion events via
`GET /events/{event_id}`, verifies their task and workflow identities, and then
constructs `AssignmentContext.dependencies` for the executor. This keeps each
result stored once in the coordination log. A transient lookup failure is
retried three times before the worker stops. Malformed references and 4xx
responses stop it immediately; continuing to heartbeat after silently skipping
an owned assignment would otherwise leave that task assigned forever. Stopping
allows the lease and normal reassignment path to recover safely.

If an upstream task reaches `task.failed`, `task.dependency_failed`,
`task.cancelled`, or `task.deadline_exceeded`, the PM
emits a deterministic `task.dependency_failed` for each affected downstream
task and cascades that state through the graph without assigning doomed work.
This v0.5 terminal propagation is deliberately conservative: reviving an
upstream task does not silently revive already failed descendants. Recreate
those descendants as new tasks after the upstream recovery so the new intent
and causal history are explicit.

## Crash recovery guarantees

### PM

At startup the PM:

1. reads coordination topics in bounded, server-filtered pages;
2. rebuilds workers, tasks, dependency edges, attempts, and decisions through a total reducer;
3. reconciles missing effects before waiting for another event;
4. publishes effects with stable logical idempotency keys.

This closes the crash window where the prototype could record `task.created`,
an upstream completion, or `task.blocked` but fail before publishing the
corresponding ready assignment, dependency failure, or decision request.

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

The reusable runtime also follows cancellation requests, cancellation/deadline
terminal events, assignment expiry, terminal failure, and replacement
registration. It cancels adapters that support cancellation and suppresses
lifecycle output once that instance no longer owns an attempt.

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
does not invent workflow relationships that were never recorded. v0.6 also
adds a nullable top-level `producer` column; historical events retain `NULL`.

As a v0.4 publisher-contract change, new `task.created` events must contain a
non-empty `payload.title`. Historical titleless events remain replayable and
receive a deterministic fallback title in the PM projection.

v0.5 adds optional `payload.depends_on` to `task.created` and
`payload.dependency_refs` to PM assignments without changing the schema version;
their absence retains the historical single-task behavior.

v0.7 adds optional `payload.deadline_at` to task creation and assignments plus
the cancellation/deadline topics below. These are additive v2 contracts;
historical tasks without a deadline retain their previous behavior.

Core v2 topics:

| Topic | Emitted by | Purpose |
|---|---|---|
| `agent.registered` | worker | Announces a process instance, capabilities, and capacity |
| `agent.heartbeat` | worker | Renews that process instance's lease |
| `task.created` | human/agent | Requests a logical outcome and optional existing dependency edges |
| `task.assigned` | PM | Creates a numbered execution attempt with decisions and completion references |
| `task.started` | worker | Confirms the active attempt began |
| `task.completed` | worker | Completes the active attempt and logical task |
| `task.blocked` | worker | Pauses the attempt for human input |
| `task.attempt_failed` | worker | Records a retryable or permanent execution failure |
| `task.assignment_expired` | PM | Records loss or replacement of the assigned worker |
| `task.failed` | PM | Terminates a task after policy exhaustion or permanent failure |
| `task.dependency_failed` | PM | Terminates downstream work whose prerequisite failed |
| `task.retry_requested` | human/agent | Extends policy and reopens the latest failed task |
| `task.cancel_requested` | human/agent | Requests terminal cancellation of an existing task |
| `task.cancelled` | PM | Records crash-safe terminal cancellation and its last attempt |
| `task.deadline_exceeded` | PM | Terminates a task after its persisted absolute deadline |
| `decision.needed` | PM | Requests one human decision for a blocked attempt |
| `decision.made` | human | Records a response that is carried into the next attempt |

Integration topics are validated separately and deliberately excluded from PM
replay:

| Topic | Emitted by | Purpose |
|---|---|---|
| `integration.task_observed` | bridge | Records externally owned shadow or unselected canary work |

Telemetry topics are also validated and deliberately excluded from PM replay:

| Topic | Purpose |
|---|---|
| `telemetry.model.started` | Records the start of one stable model invocation |
| `telemetry.model.completed` | Records duration, bounded usage metadata, and optional artifact refs |
| `telemetry.model.failed` | Records a typed retryable/permanent invocation failure |
| `telemetry.tool.started` | Records one tool-call start and optional parent invocation |
| `telemetry.tool.completed` | Records tool duration and optional artifact refs |
| `telemetry.tool.failed` | Records a typed retryable/permanent tool failure |

`topics.py` is the canonical source for all three groups. Tests assert that the
PM uses exactly the coordination group and never consumes integration or
telemetry topics.

## Reading and following the log

History queries are bounded; use `after_id` to paginate:

```sh
curl 'http://127.0.0.1:8765/events?after_id=0&limit=1000'
curl 'http://127.0.0.1:8765/events?topics=task.assigned,task.completed'
curl 'http://127.0.0.1:8765/events?correlation_id=release-2026-07'
curl 'http://127.0.0.1:8765/events?topics=telemetry.model.completed&correlation_id=release-2026-07'
curl 'http://127.0.0.1:8765/events/42'
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
correlation/producer migration, DAG validation, chain/fan-in/fan-out
readiness, dependency-failure cascades, result-reference resolution, SQL-level
stream filtering, executor conformance, bounded concurrency, subprocess
timeout/cancellation, stable canaries, single-owner adoption, real-agent
integration, telemetry isolation/idempotency, artifact integrity, and atomic
monotonic offsets. v0.7 also covers cancellation/completion races, deadline
boundaries, crash-window reconciliation, cooperative adapter revocation, and
cancelled/deadline dependency propagation. v0.8 adds shared-projection
equivalence, explanations for every lifecycle state, DAG/usage summaries,
GET-only observer behavior, human and JSON CLI output, actionable exit codes,
and proof that read-only commands create no projection or offset files.

## Scope and next boundaries

v0.8 remains a trusted, single-process, single-host runtime. Actor names are
asserted by clients, not authenticated identities. The PM lock and SSE wake-up
condition are process-local mechanisms; do not run the FastAPI app with
multiple uvicorn workers. Before distributing the bus, add authenticated actor
identity and replace local exclusivity/notification with shared infrastructure.

Future product sequencing, adoption goals, and release boundaries live only in
[ROADMAP.md](ROADMAP.md). New operational views and controls must remain
projections and commands over the event log—not a return to a mutable board as
the source of truth.

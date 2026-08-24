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

v0.9 adds a stable integration package, versioned CLI/HTTP adapter protocol,
standalone conformance check, configuration-driven workers, and local-first
onboarding commands. Existing agents remain bounded executors while agent-bus
retains ownership, recovery, and immutable history.

## Identity model

Four identities keep workflows, retries, and replays unambiguous:

- `correlation_id` identifies one wider workflow or user goal across tasks.
- `task_id` identifies the logical outcome requested by a human or agent.
- `assignment_id` identifies one execution attempt for that task.
- `instance_id` identifies one running worker process.

Executors also receive a derived `effect_scope`, which remains stable across
attempts for the same logical task. Python adapters can call
`assignment.effect_id("operation-name")` to obtain a deterministic key for one
irreversible external operation. This is deliberately distinct from
`assignment_id`: the latter identifies a delivery attempt and changes on retry.

A correlation may contain several tasks, and each task may have several
attempts. `caused_by` remains the direct parent event; it is not overloaded as
the workflow identifier.

A task may have several attempts but only its current attempt may change task
state. The worker runtime consumes each addressed delivery once, rejects an
already-expired assignment before execution, and suppresses results after known
ownership loss. A lifecycle claim can still race with an expiry while an HTTP
publication is already in flight; if it reaches the immutable log, the PM
projection rejects that claim as stale.

Lifecycle events are claims. Only the replay projection determines whether a
claim was accepted for the active assignment; rejected claims remain visible
for audit rather than being deleted from history.

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
| `agent_bus/` | Stable public integration imports for application and adapter authors |
| `executor_protocol.py` | Strict, versioned CLI and HTTP assignment/outcome envelopes |
| `integration.py` | Python, configured CLI, and guarded HTTP adapter wrappers |
| `conformance.py` | Standalone side-effect-free adapter contract probe |
| `local_config.py` | Validated loopback-only local onboarding configuration |
| `runtime.py` | Leased, concurrent worker runtime with ownership-loss cancellation |
| `adoption.py` | Controlled, shadow, and deterministic canary integration helpers |
| `telemetry.py` | Optional model/tool telemetry sink, lifecycle helpers, and producer identity |
| `artifacts.py` | Atomic, content-addressed local storage for opt-in captured content |
| `topics.py` | Canonical coordination, integration, and telemetry topic groups |
| `worker.py` | Demo executor wired through the reusable runtime |
| `events.db` | Append-only event log and task-id counter |
| `.offsets/` | Optional durable resume points for stable consumer identities |

## Architecture and invariants

The bus, PM, workers, and operator views have deliberately separate roles:

1. publishers append intent and outcomes to the event log;
2. `projection.py` derives current coordination state through a pure replay;
3. the PM reconciles any missing assignment, recovery, decision, or terminal
   event using stable idempotency keys;
4. workers execute only assignments addressed to their current process
   instance; and
5. read-only tools rebuild views from HTTP history without becoming another
   scheduler or source of truth.

The design depends on these invariants:

- **Immutable history.** Accepted events are appended, never edited. A
  correction or new request is another event.
- **Replayable effects.** PM and worker lifecycle publications use logical
  idempotency keys so replay after a crash does not duplicate an effect.
- **Current ownership only.** A task may have several attempts, but only its
  current assignment may advance it. Late output is suppressed or ignored.
- **Immutable DAG intent.** Dependency edges exist only on `task.created`.
  New intent means new tasks, not edited edges or mutable board state.
- **Bounded coordination data.** Context, results, decisions, and resolved
  dependencies remain compact. Large content belongs in external artifacts.
- **Telemetry is observational.** Model and tool events can explain execution
  but never decide task ownership, readiness, retries, or terminal state.
- **Local trust boundary.** The current bus assumes one trusted PM on one host;
  actor strings are self-reported even when perimeter authentication is on.
- **External effects remain the adapter's responsibility.** File writes,
  deployments, payments, and API calls must use a stable logical effect key,
  such as `assignment.effect_id("deploy")`, across retry attempts.

## Setup

```sh
python3 --version              # must be Python 3.10+
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
python -c 'import sys; assert sys.version_info >= (3, 10), sys.version'
agent-bus init
```

Create the environment in `.venv`; do not use the repository root itself as a
virtual-environment directory. The editable install provides the `agent-bus`
command while keeping this checkout as the source. The project is not yet
claiming a published PyPI release; `pip install agent-bus` becomes the intended
path once release packaging is published and hardened. If the final assertion
fails, recreate `.venv` with a supported interpreter such as `python3.11`.

## Quick start: one verifiable task

Run these in four terminals from the same directory. Activate `.venv` in each
terminal. The generated configuration binds only to loopback and stores
runtime data under the ignored `.agent-bus/` directory.

```sh
# terminal 1
agent-bus serve

# terminal 2
agent-bus pm

# terminal 3
agent-bus demo-worker alice

# terminal 4
RESPONSE=$(agent-bus submit "Verify the agent-bus quick start" --json)
TASK_ID=$(printf '%s' "$RESPONSE" | python -c \
  'import json,sys; print(json.load(sys.stdin)["payload"]["task_id"])')
CORRELATION_ID=$(printf '%s' "$RESPONSE" | python -c \
  'import json,sys; print(json.load(sys.stdin)["correlation_id"])')
```

The server assigns `task_id` atomically. The PM assigns a live worker and emits
an `assignment_id`; the worker includes that assignment and its process
`instance_id` in every subsequent lifecycle event. The server also generates a
`correlation_id` for the root task and propagates it through the resulting
event chain. New tasks store their retry policy in the event itself.
`agent-bus submit` defaults to no automatic retries unless `--max-retries` is
supplied; direct event publishers retain the server default.

After about one second, verify the bus, task, and derived workflow:

```sh
agent-bus doctor
agent-bus task "$TASK_ID"
agent-bus workflow "$CORRELATION_ID"
```

The expected state is a healthy schema-v2 bus with one healthy worker, a
`completed` task whose last attempt belongs to `alice`, and a one-task
`completed` workflow. The demo worker emits no model telemetry, so
`tokens not reported` and `cost not reported` are expected rather than errors.

For an explicit machine-readable pass/fail check:

```sh
agent-bus task "$TASK_ID" --json | python -c '
import json, sys
task = json.load(sys.stdin)
assert task["status"] == "completed", task
assert task["assignment_active"] is False, task
print("PASS: task completed exactly once")
'

agent-bus workflow "$CORRELATION_ID" --json | python -c '
import json, sys
workflow = json.load(sys.stdin)
assert workflow["status"] == "completed", workflow
assert workflow["task_count"] == 1, workflow
print("PASS: workflow is replayable and complete")
'
```

If the task is still `assigned` or `started`, wait briefly and repeat the
verification. Persistent warnings from `agent-bus doctor` usually mean the PM,
worker, or bus is not using the same URL. Read-only commands use an explicit
`--url` first, then `agent-bus.local.json` in the current directory, then
`AGENT_BUS_URL`, and finally the loopback default. If the local file and
environment disagree, the CLI stops with both values instead of silently
connecting to the wrong bus; unset `AGENT_BUS_URL` or pass `--url` deliberately.

`agent-bus workflow "$CORRELATION_ID" --mermaid` prints a read-only Mermaid
flowchart. It is another projection over the log, never an editable board.

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

The CLI uses the current local config before `AGENT_BUS_URL`, while
`AGENT_BUS_TOKEN` and `AGENT_BUS_WORKER_LEASE_SECONDS` remain environment
defaults. The equivalent `--url`, `--token`, and `--lease-seconds` flags are
available for one invocation. `doctor` reports
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

Application code should import the supported integration surface from
`agent_bus`. Flat modules remain available for compatibility, but are internal
building blocks rather than the preferred long-term import path. The public
package includes assignment/outcome types, `WorkerRuntime`, Python/CLI/HTTP
adapters, adoption helpers, telemetry hooks, and artifact-reference validation.

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
from agent_bus import BusClient, Completed, PythonAgentAdapter, WorkerRuntime

class ExistingAgent:
    def run(self, assignment):
        result = do_existing_work(
            goal=assignment.goal,
            context=assignment.context,
            idempotency_key=assignment.effect_id("do-existing-work"),
        )
        return Completed("agent finished", {"result_ref": result.ref})

bus = BusClient("http://127.0.0.1:8765", actor="existing-agent")
WorkerRuntime(
    bus,
    name="existing-agent",
    executor=PythonAgentAdapter(ExistingAgent()),
    capacity=2,
    capabilities=["python"],
).run()
```

Before connecting it to a live bus, run the contract probe:

```sh
agent-bus adapter check --python-target my_package.agent:ExistingAgent
```

The target may be a zero-argument class, an object with `run(assignment)`, or a
callable in an installed package or a module beneath the command's current
working directory. It must recognize
`assignment.context["agent_bus_conformance_probe"]`
and return a typed outcome without performing external side effects. The check
does not require a running bus or PM. A complete copyable implementation lives
in `examples/python_agent/`.

`capacity` is enforced by a bounded execution pool, so the worker never runs
more assignments concurrently than it advertises. Unexpected Python
exceptions default to permanent failure; applications may explicitly configure
them as retryable.

### Subprocess agents

The configured CLI adapter uses protocol v1. It sends one strict envelope on
stdin and expects one envelope on stdout:

```json
{"protocol":{"name":"agent-bus.executor","version":1,"supported_versions":[1]},"assignment":{}}
{"protocol":{"name":"agent-bus.executor","version":1},"outcome":{"status":"completed","summary":"done","result":{}}}
```

`version` is the selected wire format. `supported_versions` advertises the
sender's capabilities and must include that selected version; receivers ignore
additional future version numbers they do not yet implement rather than
rejecting an otherwise valid v1 message. Automatic version selection or
downgrade is not part of v0.9.

Use a checked-in, credential-free configuration to describe the process:

```json
{
  "schema_version": 1,
  "worker": {"name": "my-cli", "capabilities": ["review"], "capacity": 1},
  "adapter": {
    "type": "cli",
    "command": ["my-agent", "--json"],
    "protocol_version": 1,
    "timeout_seconds": 300
  }
}
```

```sh
agent-bus adapter check --config adapter.json
agent-bus adapter run adapter.json
```

Exit code `75` maps to a retryable process failure; other non-zero exits map to
permanent failure. Input, stdout, and stderr are size-limited. Timeouts are
retryable. When assignment ownership is lost or the runtime stops, a running
subprocess is terminated and its result is suppressed. Shell execution is not
used.

In-process Python threads cannot be force-killed safely. Such agents should
make logical external effects idempotent with `assignment.effect_id(...)`. If
the wrapped object exposes `cancel(assignment_id)` or `close()`,
`PythonAgentAdapter` forwards those lifecycle calls.

The legacy unversioned `SubprocessExecutor` input remains readable for existing
v0.4 integrations. New configuration files accept only protocol v1, so later
wire changes can be negotiated rather than silently misparsed. See
`examples/cli_agent/` for the smallest working implementation and
`examples/hermes/` for a real agent integration.

### HTTP agents

Use an HTTP adapter when the agent cannot run in the same process or as a child
process. It uses the same protocol-v1 envelope and sends `assignment_id` as the
attempt-scoped `Idempotency-Key` and `X-Agent-Bus-Assignment-Id` headers. It
also sends the retry-stable `X-Agent-Bus-Effect-Scope` header; the HTTP agent
must combine that scope with a stable operation name for irreversible
downstream effects. Loopback HTTP works by default. A remote endpoint must
explicitly set `allow_remote: true`, use HTTPS, and name a populated secret
environment variable with `token_env`; a credential is never stored in the
config file.

```json
{
  "schema_version": 1,
  "worker": {"name": "local-http-agent", "capabilities": ["http"]},
  "adapter": {
    "type": "http",
    "endpoint": "http://127.0.0.1:9000/execute",
    "cancellation_endpoint": "http://127.0.0.1:9000/cancel",
    "protocol_version": 1,
    "timeout_seconds": 300
  }
}
```

HTTP `408`, `425`, `429`, transport errors, and `5xx` responses are retryable;
other `4xx` responses and malformed outcomes are permanent attempt failures.
Cancellation delivery is best-effort. The runtime's ownership fence remains
authoritative and suppresses late output even if a remote agent cannot stop.

### Adapter safety contract

- Treat `assignment_id` as one delivery-attempt identity. Use
  `assignment.effect_id("stable-operation-name")` in Python, or combine the
  protocol's `effect_scope` with a stable operation name, for each file write,
  API call, deployment, payment, or other irreversible logical effect that
  must not repeat when the task is retried.
- Check `deadline_at` before starting and cooperate with
  `cancel(assignment_id)` where possible. Never publish a late result yourself.
- Return only `Completed`, `Blocked`, `RetryableFailure`, or
  `PermanentFailure`; do not invent mutable status outside the event log.
- Keep inline result data small and return immutable artifact references for
  larger content. Prompts and tool output stay out of SQLite by default.
- Do not schedule follow-up tasks inside the adapter. DAG creation and task
  ownership belong to the publisher and PM.

## Safe adoption modes

`AdoptionBridge` records a stable decision for work originating in another
system:

```python
from agent_bus import AdoptionBridge, AdoptionMode, CanarySelector, ExternalOrigin

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

The bridge uses the same idempotency identity for either ownership result, and
the bus transactionally claims each `(external system, task reference)` across
all bridge actor names. A later attempt to reinterpret the origin—or a second
misconfigured bridge actor trying to claim it—is rejected with a dual-ownership
conflict instead of producing two tasks. The claim table is a rebuildable
uniqueness projection; the immutable adoption event remains truth.

The external system must honour the recorded decision: bus-owned work must be
removed from its own execution queue before an agent may execute it. Shadow
mode must never invoke a second side-effecting agent. Canary percentages and
`include_refs` affect only external origins that have not yet been adopted;
there is deliberately no ownership-transfer operation for an existing origin.

Controlled and canary tasks may include `deadline_at`; adapters receive the
persisted value through `AssignmentContext` and should treat the PM's terminal
deadline event as the ownership boundary.

### Safe rollout recipe

1. **Shadow:** publish observations from the external source and compare the
   derived view with its current workflow. agent-bus performs no work.
2. **Canary:** select only new origins, durably record the ownership decision,
   and remove selected work from the external executor before allowing the bus
   worker to run it.
3. **Controlled:** stop the old source from enqueueing new work, drain or leave
   existing externally owned origins alone, then create new bus-owned tasks.

For a database or queue you do not control transactionally with agent-bus, use
an outbox. In one external transaction, update the source task and insert an
outbox row containing a stable `system`, `task_ref`, title, and payload. A
bridge process publishes that row through `AdoptionBridge` with a stable actor,
then marks the outbox row delivered only after the event is accepted. Retrying
the row is safe; changing its recorded owner is rejected. Never dequeue work
for execution merely because the bridge attempted a network call—wait for the
durable ownership decision.

### Integration troubleshooting

- **409 / dual ownership:** the external origin was already observed or
  controlled. Inspect that event; do not change actor names or keys to bypass
  the decision.
- **Adapter check performs real work:** teach the adapter to recognize the
  conformance marker before any side effect.
- **Malformed CLI output:** emit exactly one protocol-v1 JSON envelope on
  stdout; send diagnostics to stderr and stay within the configured byte cap.
- **Timeout or repeated retry:** ensure the agent's own timeout is shorter than
  the adapter deadline and that irreversible work uses a retry-stable effect
  identity rather than the changing assignment attempt.
- **Remote HTTP rejected:** use HTTPS, set `allow_remote`, and put the bearer
  token in the environment variable named by `token_env`.
- **Task remains open:** run `agent-bus explain TASK_ID`; capability mismatch,
  absent workers, dependencies, PM state, and lease health are distinguished.

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
reconnections. A malformed SSE frame raises the stable `BusProtocolError` and
terminates that subscription at the last confirmed event instead of silently
skipping data or advancing its resume position. A replacement process
registers a new `instance_id`, so demo
workers neither resume the previous process's assignments nor create durable
offset files. Stable consumers that do need cross-process resume can use
`BusClient.load_offset()` and `save_offset()`; those files are replaced
atomically and only move forward. After all pre-v0.2.1 workers have stopped,
their old per-instance files may be removed once; new demo workers do not
replenish them.

The reusable runtime also follows cancellation requests, cancellation/deadline
terminal events, assignment expiry, terminal failure, and replacement
registration. It rejects already-expired and already-consumed assignment
deliveries, cancels adapters only during their execution phase, serializes
adapter cancellation with cleanup, and suppresses lifecycle output once that
instance no longer owns an attempt.

An external tool call, payment, deployment, or file mutation performed by a
worker must also use a retry-stable logical effect token. The bus can make
orchestration effects idempotent; it cannot make an arbitrary external side
effect atomic.

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

Every stored event uses the same envelope:

| Field | Contract |
|---|---|
| `id` | Positive, server-assigned SQLite id used for audit references and resume cursors |
| `ts` | Server-assigned Unix timestamp |
| `topic` | Non-empty routing name; built-in names are grouped in `topics.py` |
| `actor` | Non-empty, self-reported publisher name |
| `schema_version` | Positive contract version; new built-in events require v2 |
| `idempotency_key` | Optional logical publication identity, unique per `(actor, key)` |
| `caused_by` | Optional existing parent event id for direct causality and correlation inheritance |
| `correlation_id` | Optional workflow identity; generated for a root task and shared across its DAG |
| `producer` | Required on built-in telemetry; identifies implementation, process, and optional version |
| `payload` | Topic-specific JSON object |

New built-in events use top-level `schema_version: 2` by default. Unknown
topics remain permitted so applications can extend the bus without changing
its core, but they are not automatically coordination topics and receive no
built-in payload validation.

Validation and recovery deliberately operate at three different layers:

1. `bus.py` structurally validates known v2 envelopes and payloads before
   append and rejects contract violations with HTTP 422.
2. `projection.py` remains total during replay: irrelevant, duplicate, stale,
   malformed, or invalid-transition rows are ignored instead of preventing
   later valid history from being derived.
3. `runtime.py` resolves referenced dependency completions and constructs an
   executable `AssignmentContext`. If an assignment cannot be interpreted
   safely, the worker stops so lease expiry can recover it rather than
   continuing to heartbeat around work it skipped.

Append-time structural validation is therefore not a proof that arbitrary
events published by a client claiming to be `pm` are executable. For example,
the runtime still requires a usable assignment goal or title. Correct local
operation assumes the real single PM is the publisher of PM lifecycle events.

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
telemetry topics. `validate_event()` in `bus.py` is the canonical built-in
payload validator; `tests/test_bus.py` records its compatibility and rejection
behavior. Keep those sources authoritative instead of treating this compact
README table as a separately maintained exhaustive schema.

### Shared contract limits

| Data | Limit |
|---|---|
| Task context | 16 KiB encoded JSON |
| Final structured result | 16 KiB encoded JSON |
| Resolved dependency input | 32 KiB aggregate encoded JSON |
| Direct task dependencies or assignment references | 128 |
| Telemetry `attributes` or model `usage` object | 8 KiB encoded JSON each |
| Artifact references on one telemetry event | 16 |
| One content-addressed artifact | 16 MiB |
| Correlation, producer, telemetry identity, and external-origin fields | 128 characters where applicable |

These are coordination safety boundaries, not a transport for large model
content. Artifact references in telemetry do not yet make oversized DAG
results transparently resolvable by downstream executors.

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
  and every data route requires an `Authorization: Bearer <token>` header;
  `BusClient` picks the same variable up automatically (or accepts `token=`).
  `/health` stays open. Without the variable, anyone who can reach the port can
  publish.
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
python -m build                  # source archive and wheel preflight
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
and proof that read-only commands create no projection or offset files. v0.9
adds strict protocol round-trips, standalone Python/CLI conformance, validated
integration and local configs, guarded HTTP behavior, cross-actor origin
claims, public-package imports, safe Mermaid export, runtime phase fencing,
retry-stable external-effect identities, malformed-stream errors, and explicit
local-config/environment URL conflict handling.

## Scope and next boundaries

v0.9 remains a trusted, single-process, single-host runtime. Actor names are
asserted by clients, not authenticated identities. The PM lock and SSE wake-up
condition are process-local mechanisms; do not run the FastAPI app with
multiple uvicorn workers. Before distributing the bus, add authenticated actor
identity and replace local exclusivity/notification with shared infrastructure.

Future product sequencing, adoption goals, and release boundaries live only in
[ROADMAP.md](ROADMAP.md). New operational views and controls must remain
projections and commands over the event log—not a return to a mutable board as
the source of truth.

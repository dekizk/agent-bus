# agent-bus roadmap

## Product direction

`agent-bus` should become an agent-agnostic control plane that people can run
alongside agents they already use. Adopting it should not require rewriting an
agent, replacing its model provider, or moving the agent's internal logic into
agent-bus.

The product is intended to be a realistic alternative to kanban-style agent
monitoring. A board represents work through mutable cards that humans or agents
must keep synchronized. agent-bus instead records immutable facts about work,
derives current state by replaying those facts, and coordinates the next safe
action. Any dashboard or CLI is a view over that history—not another source of
truth.

| Board-style monitoring | agent-bus |
|---|---|
| Mutable cards describe current state | Immutable events record what happened |
| Status can drift from real execution | State is derived from execution history |
| Humans or agents move cards | Workers emit lifecycle outcomes |
| Retries and ownership are often implicit | Attempts, leases, and ownership are explicit |
| Dependencies are visual conventions | DAG readiness is enforced by the coordinator |
| Recovery depends on manually fixing state | Replay and reconciliation recover missing effects |
| Usually tied to one workflow product | Framework-neutral contracts connect existing agents |

The goal is not to build a better task board. The goal is to make a task board
unnecessary as the orchestration authority while still giving operators a
clear, approachable view of their agents.

## Product principles

Every version should preserve these constraints:

1. **Agent agnostic.** Any Python agent, CLI agent, or external workflow should
   be able to integrate through a small stable boundary.
2. **Adopt incrementally.** Shadow, canary, and controlled modes let a team
   observe first and transfer ownership only when ready.
3. **Easy first success.** A new user should be able to install agent-bus,
   connect one existing agent, submit one task, and inspect its state in under
   ten minutes.
4. **One orchestration owner.** Integration must prevent an external scheduler
   and agent-bus from executing the same task simultaneously.
5. **The event log is truth.** Operational views, indexes, snapshots, and UIs
   are disposable projections that can be rebuilt.
6. **Local-first and safe by default.** The smallest useful deployment should
   remain easy to understand, run, back up, and remove.
7. **Bounded agents.** Agents execute assignments; they do not silently become
   a second project manager, scheduler, or source of task state.
8. **Progressive authority.** Tool access, content capture, and external side
   effects remain explicit opt-ins.
9. **Explainable decisions.** Assignment, waiting, retry, cancellation, and
   terminal states should always be traceable to recorded events and policy.
10. **Evidence-driven scope.** Real integrations and failure trials should
    determine which orchestration features are built next.

## Current foundation — shipped through v0.7

The project already provides the core mechanics needed for a local event-driven
agent control plane:

- append-only SQLite events with schema validation and idempotent publishing;
- workflow correlation, causal links, task identity, attempts, and worker
  process identity;
- crash-safe PM replay and deterministic reconciliation;
- worker registration, capabilities, capacity, heartbeats, leases, and stale
  attempt rejection;
- bounded retry policy, permanent failure, human retry, blocking, and human
  decisions;
- framework-neutral executor outcomes with in-process and subprocess adapters;
- shadow, deterministic canary, and controlled adoption modes;
- automatic DAG readiness, dependency result references, and terminal failure
  propagation;
- separate model/tool telemetry and content-addressed artifact storage;
- durable cancellation, task-wide deadlines, local adapter revocation, and DAG
  propagation for cancelled or expired work;
- a real Hermes adapter and live evidence covering execution, human decisions,
  recovery, DAGs, telemetry, cancellation, and deadlines.

## Immediate release step — v0.7

Before starting another feature version:

- commit and push the verified v0.7 implementation;
- retain the live Hermes evidence in `examples/hermes/TRIAL_NOTES.md`;
- use v0.7 on several genuine tasks and record every point where an operator
  has to inspect raw JSON, manually correlate events, or guess why work is
  waiting.

That friction should shape v0.8.

## v0.8 — approachable operations and read-only visibility

The next version should make the existing control plane understandable without
requiring users to read raw event rows.

### Operator projection

Add a read-only projection that can answer:

- What state is this task or workflow in?
- Why is it waiting?
- Which worker owns the current attempt, and is its lease healthy?
- Which dependencies are incomplete or terminal?
- How many retries remain?
- Is the task blocked on a person, cancellation, or a deadline?
- Which event caused the current state?
- How much model usage and reported cost belongs to the workflow?

The projection must be derived entirely from the event log and rebuildable from
scratch.

### Human-friendly CLI

Target commands should include:

```text
agent-bus doctor
agent-bus workers
agent-bus task TASK_ID
agent-bus workflow CORRELATION_ID
agent-bus explain TASK_ID
agent-bus tail CORRELATION_ID
```

Output should default to concise human-readable summaries, with `--json` for
automation. `explain` should state the concrete reason a task cannot advance,
not merely repeat its status.

### Installation and first-run experience

The v0.8 design should also establish the intended easy path:

```text
pip install agent-bus
agent-bus init
agent-bus doctor
agent-bus serve
```

Exact packaging may be completed in v0.8 or v0.9, but commands, configuration,
error messages, and documentation should be designed together. A user should
not need to understand SQLite, SSE, reducer internals, or four identity fields
before completing the first example.

### v0.8 acceptance criteria

- One command explains every current task state.
- A two-task workflow can be understood without inspecting raw event JSON.
- Every displayed value is traceable to event ids.
- Restarting or deleting the projection does not change orchestration truth.
- The quick start is tested from a clean environment.
- A newcomer can connect the demo executor and inspect a completed workflow in
  under ten minutes.

## v0.9 — integration SDK and onboarding

Once the system is easy to observe, make it easy to attach to arbitrary agents.
This is intentionally earlier than advanced scheduling policy because broad
integration is central to the product, not an optional final layer.

### Stable public integration surface

- Package and document `AssignmentContext`, executor outcomes, runtime hooks,
  adoption modes, telemetry, and artifact references as public APIs.
- Stabilize the subprocess JSON protocol and version negotiation.
- Provide a generic wrapper for Python objects exposing `run()`.
- Provide a configuration-driven wrapper for existing CLI agents.
- Define an HTTP/webhook bridge for agents that cannot run in the same process.
- Provide explicit cancellation, deadline, idempotency, and external side-effect
  requirements for adapter authors.

### Integration kit

- `agent-bus adapter check` conformance tests;
- a minimal Python-agent example;
- a minimal CLI-agent example;
- the Hermes example as a realistic reference integration;
- copyable shadow, canary, and controlled rollout recipes;
- an external outbox pattern for safely adopting tasks from another system;
- troubleshooting guidance for ownership, credentials, timeouts, and malformed
  outcomes.

### v0.9 acceptance criteria

- An existing Python agent can be connected without changing its core logic.
- An existing JSON-capable CLI agent can be connected through configuration.
- Adapters can prove conformance without running the full repository test suite.
- Shadow-to-canary-to-controlled adoption has one documented safe path.
- Dual ownership is mechanically difficult and prominently diagnosed.

## v0.10 — scheduling and management policy

Add richer controls only after visibility and integration trials show they are
needed:

- immutable priority classes rather than manually ordered cards;
- fair scheduling between workflows or tenants;
- per-workflow and per-agent concurrency limits;
- token, cost, attempt, or wall-clock budgets;
- `not_before` scheduling;
- crash-safe pause and resume commands;
- explicit task supersession;
- deterministic explanations for every scheduling choice.

Pause/resume should use the same ownership discipline as cancellation but keep
the logical task eligible for a later attempt. Priority must not turn into an
unexplained mutable queue position.

If trials show that pause/resume and scheduling fairness are independently
complex, split this work into separate versions rather than forcing both into a
large release.

## v0.11 — local scale, retention, and recovery hardening

Address known local-scale boundaries while preserving the event log as truth:

- replace full-log task identity scans with a transactionally maintained task
  index;
- add disposable materialized projections and replay snapshots;
- verify that every projection can be deleted and rebuilt;
- benchmark large logs, many workers, and wide/deep DAGs;
- add safe artifact reachability analysis and retention tools;
- test backup, restore, corruption detection, and recovery procedures;
- consider archival log segments without silently rewriting history;
- add long-running soak, crash-injection, and race tests.

## v1.0 — stable local agent control plane

v1.0 should make a clear, supportable promise: one local event-driven control
plane can reliably coordinate heterogeneous agents without a mutable board as
its source of truth.

Release requirements:

- stable event-envelope, executor, adapter, and CLI contracts;
- a documented compatibility and deprecation policy;
- tested database upgrades and migrations;
- packaged CLI and service entry points;
- configuration validation and useful diagnostics;
- documented backup and recovery guarantees;
- multiple live agent integrations;
- fault-injection and long-running reliability evidence;
- clear trust, security, and deployment boundaries;
- complete quick-start, integration, operations, and troubleshooting guides.

## Post-v1.0 — distributed operation

Distribution should come after local semantics and user experience are stable:

- authenticated producer and actor identities;
- per-actor authorization;
- shared PM leadership and leader election;
- multi-process-safe notifications;
- multi-host workers and leases;
- shared artifact storage;
- PostgreSQL or another durable shared event store;
- clock-skew handling, distributed tracing, and high availability.

This phase must preserve the same replayable semantics rather than replacing
them with hidden broker or scheduler state.

## Cross-cutting trials

Continue running these alongside feature development:

- telemetry under hard worker loss and lease-expiry recovery;
- week-long operation with at least one genuine existing-agent workflow;
- wide-DAG and large-result pressure tests;
- repeated completion/cancellation/deadline races;
- artifact growth and retention measurements;
- shadow and canary adoption from a real external task source;
- clean-environment onboarding by someone unfamiliar with the codebase.

Record concrete evidence and friction in the relevant integration trial notes.

## Usability release gate

Every version should answer these questions before it is declared complete:

- Can a new user discover and install it?
- Can they connect an existing agent instead of rewriting it?
- Can they understand current work without reading raw events?
- Can they tell exactly why a task is waiting or terminal?
- Can they adopt it without creating two owners for one task?
- Are failure messages actionable?
- Are examples safe, disposable, and copyable?
- Does every operational view remain a projection over immutable history?

If a feature is powerful but makes those answers worse, it is not ready.

## Features deliberately not implied by this roadmap

- A mutable kanban database as a second source of truth
- Editable dependency edges — new intent means new tasks, not modified history
- Agents editing their own status cards
- Agent-specific orchestration logic in the core
- A second scheduler hidden inside an adapter
- Full prompts, outputs, or tool payloads stored inline in SQLite by default
- Telemetry events participating in PM coordination replay
- Distributed complexity before local operation is dependable and approachable

# Release notes

## v0.12.0 — 2026-10-20

Demonstrations and everyday operator ergonomics. No event, executor,
configuration, or database changes.

### Changes

- `agent-bus demo <scenario>` plays a scripted, narrated scenario against a
  real bus, PM, and workers started as separate processes in a fresh
  directory on a free port. It never touches existing history or a running
  bus, and crashes are genuine `SIGKILL`s. Scenarios: `recovery` (worker and
  PM killed mid-task), `approval` (an agent asks before an irreversible tool
  call and makes it at most once), `budget` (a workflow cost cap stops new
  work, and raising it resumes), and `audit-triage` (fan-out reviewers, a
  crashed reviewer, a human severity call, and exactly-once submission via
  `assignment.effect_id`). Stand-in agents report model usage through the
  normal telemetry sink, so every demo shows token and cost accounting, and
  the live event tail shows usage and policy events. Options: `--pace`, `--yes`, `--dir`,
  `--keep-running`, and `--read-pauses`, which holds each narration long
  enough to read for recordings and talks. The history is kept for
  inspection afterwards.
- `agent-bus submit --depends-on TASK_ID` (repeatable) creates DAG edges
  without composing raw events.
- `agent-bus demo-worker` gains `--block`, `--fail`, `--fail-permanently`,
  and `--delay`, matching the standalone `worker.py`.
- `agent-bus serve` stops promptly. Event streams now notice a departed
  client or a stop signal within about a second instead of up to 15 seconds,
  so Ctrl-C no longer waits, or hangs until a second Ctrl-C, while the PM or
  workers are connected. Idle keepalives and history re-reads keep their
  15-second cadence, and clients reconnect with their normal backoff.
- `agent-bus serve` now exits with status 2 and a short message when it
  cannot start, for example when the port is in use. It previously exited
  with uvicorn's status (1 or 3, depending on the uvicorn version); 3
  contradicted the documented "not found" meaning.
- The README opens with a short introduction, an animation of
  `agent-bus demo recovery`, and the one-command demos.
  `scripts/record_demo.sh` re-records the animation with asciinema and agg.
  [`examples/audit_triage/`](examples/audit_triage/README.md) explains the
  audit pipeline and how to replace its stand-in agents with real ones.

### Validation

- 357 regression tests passed on the latest dependencies and on the declared
  minimums (one existing dependency deprecation warning). They include live
  runs of all three demo scenarios and a real `agent-bus serve` stopped with
  Ctrl-C while a client stream is open.
- Release wheel built from a clean checkout and verified by
  `scripts/check_release.py`.
- GitHub CI matrix passed for the merged demo work (PR #1).
- Live trials: all three demos on Linux and `audit-triage` on macOS, with
  every process log free of tracebacks; the bus stopped and restarted under a
  connected PM and worker, both of which resumed and completed new work.

### Upgrade and operating limits

Follow the normal [upgrade steps](COMPATIBILITY.md#upgrade-downgrade-and-deployment-versions).
Nothing is migrated, and the v0.11.0 released-database baseline continues to
pass against this release. Editable installs must be reinstalled
(`python -m pip install -e .`) so the new `demo` module is importable.

Demo agents are deterministic stand-ins with canned results; they show
coordination behaviour, not model quality. Demos use POSIX signals. The
v0.11.0 operating limits below still apply.

## v0.11.0 — 2026-09-13

Local scale and recovery hardening, preserving immutable event history and
framework-neutral agent integration.

### Changes

- Transactionally maintained, rebuildable task identity index avoids scanning
  task history for identity and dependency lookup.
- Opt-in local PM snapshots accelerate startup. Checksums, database identity,
  event anchors, and reducer fingerprints protect cache validity; invalid
  caches fall back to replay without changing authoritative history.
- Bounded-page worker admission reduces temporary history memory while
  retaining independent exact-prefix validation.
- Read-only artifact reachability/integrity audits and verified backup/restore
  bundles provide conservative local recovery tooling.

### Validation

- 276 regression tests passed (one existing dependency deprecation warning).
- Release wheel built and installed into a separate environment outside the
  checkout; version, CLI initialization, recovery/snapshot help, and core module
  import origins verified. Dependencies were reused from the host environment;
  this is not a fresh-machine dependency-resolution test.
- Benchmarks cover task lookup, replay, and worker admission memory;
  measurements are machine/workload specific, not performance guarantees.
- Live snapshot/restart and post-restore DAG checks passed.
- A 30-minute demo soak processed 939 tasks, recovered from 89 worker kills
  and 59 PM restarts, and passed 180 replay comparisons plus backup/restore.
- A real Hermes DAG recovered after worker loss and propagated its upstream
  result correctly. Missing usage for the interrupted attempt remained explicit.

Evidence and reproduction details: [recovery](RECOVERY.md),
[lookup benchmarks](benchmarks/RESULTS.md),
[replay benchmarks](benchmarks/REPLAY_RESULTS.md), and
[Hermes trial notes](examples/hermes/TRIAL_NOTES.md).

### Upgrade and operating limits

Stop writers and artifact publishers and preserve a backup before upgrading.
The task identity index is derived and backfilled without rewriting events.
PM snapshots remain opt-in (`agent-bus pm --snapshot`); they are disposable
caches, not portable authoritative history. See [recovery operations](RECOVERY.md)
for backup verification and restoring into a new directory.

This is a trusted local deployment release, not distributed coordination or
production certification. Week-long genuine-agent operation and broader live
race/scale tests remain open. Retained reducer state still grows with history.
There is no automatic artifact deletion, archival segmentation, secure erasure,
or repair of corrupted authoritative events. Artifact publication is not atomic
with database backup: quiesce publishers for a complete bundle. External effects
still require their own idempotency safeguards. Hard worker loss can leave
model spans open and usage unknown; reported cost is then incomplete.

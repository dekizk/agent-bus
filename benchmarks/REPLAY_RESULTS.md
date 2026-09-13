# v0.11 phase 2 — replay, snapshots, and worker admission

Benchmark measurements: 2026-09-13, macOS 26.2 arm64, Python 3.13.7. Benchmark data
was generated in disposable local databases; these measurements used no live
agent, user workflow, credentials, or paid model calls. A separate live local
demo-worker recovery trial is recorded below.

```sh
.venv/bin/python benchmarks/replay_admission.py
.venv/bin/python benchmarks/replay_admission.py --sizes 1000 --heartbeats 100
```

The harness includes 20 registered workers, wide (root with many children) and
deep (linear chain) dependency shapes, task creation, repeated heartbeats, and
an equal number of synthetic raw telemetry rows. It measures the real shared
reducer and `WorkerRuntime._assignment_is_accepted`, without executing work.
The graph fixtures exercise replay shape, not completion of a live DAG.

Each reported timing is one local sample, not a percentile or throughput claim.
OS caches are warm. A regression run overlapped part of the longer measurement;
repeat on an idle host before using the numbers for capacity planning. SQL
fixture seeding and index rebuilding are outside measured replay. Snapshot
build is reported separately. Initial admission timing includes `tracemalloc`
overhead; incremental admission timing does not. A direct SQLite transport shim
includes row decoding/materialization but excludes HTTP/SSE/TLS/network costs.

## Results

| Tasks / total events | Shape | Streamed full replay ms | Snapshot load ms | Snapshot build ms |
| --- | --- | ---: | ---: | ---: |
| 100 / 2,120 | Wide | 6.142 | 6.301 | 9.041 |
| 100 / 2,120 | Deep | 5.853 | 6.226 | 5.804 |
| 1,000 / 21,020 | Wide | 52.214 | 53.804 | 47.221 |
| 1,000 / 21,020 | Deep | 52.389 | 51.636 | 42.333 |
| 1,000 / 201,020 | Wide | 457.629 | 52.940 | 45.969 |
| 1,000 / 201,020 | Deep | 512.282 | 52.019 | 41.962 |

The four smaller measurements preceded final restart/fingerprint hardening;
the longer run was repeated after that change. The codec and replay semantics
were unchanged between these measurements.

For the 201,020-event fixtures:

- Full replay consumed 101,020 coordination rows; raw telemetry was excluded.
- A current snapshot required zero historical event rows to be replayed.
- Streamed replay peak traced allocations were about 2.5 MB. Snapshot loading
  peaked around 11.5 MB because JSON parsing temporarily duplicates state.
- First worker admission consumed 101,022 coordination rows, took about
  1.69–1.85 seconds **with allocation tracing enabled**, and peaked around
  115.5 MB. The existing `query_all` path materializes the history list.
- Second admission fetched exactly two additional rows and took 1.21–1.25 ms.

## Interpretation and next decision

Keep PM snapshots **opt-in**. They are beneficial when history grows much faster
than the projected task state, but they do not improve the small fixtures and
cost more temporary memory than streamed replay. Snapshot persistence also has
its own build cost; PM startup refreshes the snapshot before reconciliation.

The clearest next performance target is **streaming the initial worker admission
history**, retaining independent exact-prefix validation. The incremental path
already has the desired small read volume. These results do not justify handing
workers a remotely supplied snapshot yet. Pagination/network measurements and
live many-worker/fan-in trials remain necessary before claiming deployment-scale
performance. No version-wide v0.11 completion or live Hermes trial is claimed.

## Checkpoint review and recovery coverage

Phase 1 review found no blocking issue in the transactional index or its rebuild
path. An additional interrupted-initial-migration regression verifies that a
failed backfill cannot leave a partial table that startup would treat as complete.
Older writers against an upgraded database remain unsupported and documented.

Phase 2 tests cover complete state/container round trips, policy/usage/decision
state, equivalent next PM decisions, stale snapshot suffix replay, historical
bounds, checksum/version/identity/anchor rejection, delete/rebuild, failed writes,
process loss, fresh-process loading, concurrent publication during pinned replay,
and the HTTP database-identity/anchor round trip. Source compatibility is based
on loaded code, not rereading edited source files under an already-running PM.

## Live local PM restart/snapshot trial — 2026-09-13

**PASS.** Separate loopback HTTP server, real PM processes, and a real worker
runtime using `DemoExecutor`; no Hermes/model calls or existing workflows.
Five tasks completed, with exactly one assignment and one completion each.

| Task | Trial condition | Assignment event | Completion event |
| ---: | --- | ---: | ---: |
| 1 | Baseline PM without snapshots | 5 | 7 |
| 2 | Pending workflow pause preserved across snapshot restart; explicitly resumed | 19 | 21 |
| 3 | Task created after the cached prefix | 14 | 16 |
| 4 | Deleted snapshot; full-replay fallback | 24 | 26 |
| 5 | Corrupted snapshot checksum; full-replay fallback | 29 | 31 |

Evidence checked:

- Built the first snapshot through event 9, including a pending workflow pause.
  Restarted PM logged `snapshot loaded; replayed 1 coordination events` through
  event 10, then acknowledged the pause at event 11.
- Task 3 completed while task 2 remained unassigned. Explicit resume request 17
  produced acknowledgement 18 and only then assignment 19 for task 2.
- After clearing the snapshot, restarted PM reported the snapshot absent and
  replayed history before completing task 4.
- After corrupting only the disposable cache checksum in the trial database,
  restarted PM reported `snapshot checksum mismatch` and replayed 27 coordination
  events before completing task 5.
- Final cache rebuilt through event 31 exactly matched a separate full replay,
  required zero suffix events, and left all event rows unchanged.
- All trial-owned server/PM/worker processes were stopped afterward.

Local evidence directory:
`outputs/agent-bus-trials/v011-snapshot-20260913-161120-f54a20`
under the Codex task workspace. Contains `summary.json`, `events.json`, the
SQLite database/configuration, and separate server/worker/PM logs. Event export
SHA-256: `c52c102ed5a180efae81716e7cd7fe3a82783159a99045529c4323b1b0b07582`.
Runner: `outputs/v011_snapshot_trial.py` in the same workspace.

Scope: this closes the local PM restart/snapshot smoke check. It is not evidence
of live model usage accounting, active-work interruption, many-worker load,
long-running soak reliability, or completion of the remaining v0.11 roadmap.

## Phase 3 — bounded worker admission (2026-09-13)

The standard runtime now uses lazy `BusClient.iter_events`, bounded to the
assignment event ID. It retains independent reducer validation, not PM cache
trust. The earlier Phase 2 admission measurements above remain the baseline.

Repeated the 1,000-task / 201,020-event benchmark with 100 heartbeats per task:

| Shape | Initial admission peak bytes, before → after | Initial ms after, with tracing | Incremental rows / ms after |
| --- | ---: | ---: | ---: |
| Wide | 115,500,847 → 4,463,012 | 1,644.303 | 2 / 0.876 |
| Deep | 115,523,477 → 4,525,164 | 1,698.548 | 2 / 1.501 |

Both initial reads consumed 101,022 coordination rows and accepted the same
assignments. Temporary peak allocations fell approximately 96%; execution time
is not claimed materially improved. The retained projection still grows with
task/accounting history. These remain SQLite-shim measurements, excluding real
network transport costs; the live check below separately exercises HTTP.

Live local restart trial: **PASS**, evidence directory
`outputs/agent-bus-trials/v011-snapshot-20260913-165931-b128e4` in the Codex task
workspace. Runner: `outputs/v011_snapshot_trial.py --restart-worker`.

- Task 1 completed at event 7 using the original worker.
- With PM and worker stopped, the isolated fixture was extended with 1,005
  historical coordination heartbeats (no task/index rows changed).
- A replacement worker registered with a new instance, then independently
  traversed history exceeding the 1,000-event page size over the live HTTP bus.
- Tasks 3, 2, 4, and 5 completed at events 1022, 1027, 1032, and 1037;
  all four assignments named the replacement instance. Each of all five tasks
  had exactly one assignment and completion.
- Workflow pause/resume and PM valid/missing/corrupt snapshot restart checks
  also passed. Final snapshot and full replay agreed; rebuilding changed no events.
- All trial processes were stopped. No existing workflows or model services used.

The evidence bundle contains process logs, config/database, `summary.json`, and
`events.json` (SHA-256
`7a5ab89c2c5d164ef9877f72b9450030ec62f6f20c524b5e3f93014dd0de8289`).
Regressions: 264 passed, one existing dependency deprecation warning. Tests cover
lazy page requests, page-size boundaries, later publication beyond the target,
malformed/duplicate/out-of-order records, wrong topic filters, missing/changed
assignments, full-replay equivalence, and later-page failure stopping heartbeats
without executor invocation.

This trial is not a lease-expiry or mid-execution worker-kill test. Remaining
v0.11 work includes artifact retention, backup/restore/corruption recovery and
long-running reliability evidence.

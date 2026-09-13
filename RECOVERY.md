# Local artifact audit, backup, and restore

These v0.11 development tools preserve immutable event history. They do not
repair damaged events, delete history, or promise exactly-once external effects.
Use bundles from trusted local sources: hashes detect corruption, not malicious
replacement of both a file and its manifest.

## Retention audit: report only

Use the actual content-addressed store directories configured by your adapters.
Repeat `--artifact-root` if multiple stores serve the same bus:

```sh
agent-bus storage-audit --config agent-bus.local.json \
  --artifact-root /path/to/your/artifacts --min-age-days 7 --json
```

The audit scans all event payloads, including telemetry, completed tasks, and
nested custom-event references. It verifies referenced hashes/sizes and reports
aged, unreferenced files, unclassified files, and missing/corrupt/symlinked data.
Ambiguous digest-bearing objects or unreadable event JSON stop classification;
they are not silently treated as unreachable. References must carry `sha256`,
`size_bytes`, `media_type`, and `kind` (extra wrapper fields are allowed).

**Nothing is deleted.** A file absent from this event prefix may be about to be
published, or referenced by another database sharing the store. Age alone is
not proof of safe deletion. Automatic GC awaits an explicit artifact publication
fence and shared-store ownership rules. Do not turn the candidate list into a
deletion script. Files referenced by retained history remain protected even if
their tasks completed. No secure-erasure guarantee is made.

## Create and verify a complete bundle

1. Stop task submitters, PMs, workers, and any other artifact publishers for the
   maintenance window. Preserve their configuration separately.
2. Ensure all artifact roots referenced by this database are available.
3. Choose a new backup directory whose parent already exists:

```sh
mkdir -p ./backups
agent-bus backup ./backups/checkpoint-001 --config agent-bus.local.json \
  --artifact-root /path/to/your/artifacts
agent-bus verify-backup ./backups/checkpoint-001
```

SQLite's backup API takes a consistent database copy, including committed WAL
content. The copy is consolidated to standalone `events.db` with no WAL/journal
sidecar dependency. SQLite copying itself supports active writers, but there is
no atomic filesystem/log snapshot: quiesce artifact publishers for the complete
operation. Missing or corrupt referenced bytes fail the backup. No artifact root
is needed when history has no artifact references.

Bundles contain `events.db`, referenced files under `artifacts/sha256/...`, and a
versioned `manifest.json` with database checksum, identity, event cursor/count,
and artifact count. Unreferenced blobs are excluded. Directories are private
(0700), files private (0600). Files and directories are flushed before completion.
Configuration, credentials, adapter packages, offsets, and external side effects
are **not** included. Store encrypted/off-host copies separately if needed.

A fresh destination is reserved exclusively. Until successful completion it
contains `.incomplete`; failures leave it marked for inspection. Never remove
that marker to force acceptance. Retry with a different fresh destination.
Verification rejects incomplete bundles, checksum mismatches, malformed
references, missing artifacts, symlinks on supported artifact paths, and
unexpected SQLite sidecars. It runs SQLite integrity checks as well. Do not
open the backup as a live bus database; restore it into a new location first.

## Restore without overwriting anything

```sh
agent-bus restore ./backups/checkpoint-001 ./recovered-bus
```

`./recovered-bus` must not already exist, even as an empty directory. No existing
config or running HTTP server is needed. Verification occurs before creating the
destination; copied bytes are checked again. Derived identity/adoption/supersession
indexes are rebuilt, coordination snapshots discarded, and full replay checked.
Successful output contains `events.db`, referenced `artifacts`, and a restore
manifest. A restore manifest is not a new backup manifest: use `backup` to
produce another verified bundle. The original database and bundle are untouched.

Before resuming:

1. Stop the original bus/PM/workers. A restored copy retains the original database
   and task identities; never run divergent copies as one logical bus.
2. Create/review a local config pointing `bus.database_path` at the restored
   `events.db`. Point adapter artifact stores at the restored `artifacts` folder.
   Reinstall the needed adapters and supply credentials separately.
3. Start the restored bus, inspect tasks/workflows, then start one PM and fresh
   worker instances. Do not restore stale consumer offsets blindly.
4. Review tasks that were active at backup time. Lease expiry may reassign them.
   Restoring cannot undo external actions performed after the backup; integrations
   must use stable effect identities and external idempotency protections.

If authoritative history is corrupted, stop writes and preserve the damaged
database plus sidecars for diagnosis. Restore the newest verified backup into
a separate directory; do not edit the damaged log or delete evidence to make
integrity checks pass. Recovery is limited to the backup's recorded cursor.

## Recorded recovery drill — 2026-09-13

Passed an isolated command-line drill with two persisted events, one nested
referenced artifact, and one aged orphan. Audit reported the orphan without
deleting it. Backup verification passed; restore reproduced event-derived state
and artifact bytes. An existing destination was refused. A separate deliberately
corrupted artifact bundle was rejected before a restore directory was created.
Original event history remained unchanged. No live models or user workflows used.

Evidence: `outputs/agent-bus-trials/v011-recovery-20260913-171121-fb0c55`
in the Codex task workspace; `summary.json` includes exact commands, exit codes,
checksums, manifests, and outcomes. Runner: `outputs/v011_recovery_trial.py`.
An earlier preliminary drill (`...170948-e77927`) preceded standalone SQLite
bundle hardening; the later drill is the current evidence.

Tests also exercise uncommitted SQLite writers, corrupted database bytes,
interrupted copies, symlinks, missing artifacts, and replay/index rebuilding.
This is a small offline recovery drill, not a power-loss, adversarial filesystem,
large-backup, post-restore live-execution, or long-running soak certification.

## Post-restore live execution — 2026-09-13

**PASS.** An isolated three-task DAG used real loopback bus/PM/worker processes,
with a deliberately slow original demo executor and a fresh reference demo
worker after restore. No model calls or existing user workflows were involved.

The first task completed. The second started and was interrupted by killing its
worker; its dependent third task remained open. The PM was stopped before the
worker kill, and all original processes were stopped before backup. The verified
bundle captured 12 events and one referenced artifact. Restore into a new
directory reproduced the exact projected state and artifact bytes.

After the original worker's three-second trial lease elapsed, the restored bus,
fresh worker, and PM (`--snapshot`) started. The PM correctly used full replay
because restoration discarded the snapshot.

| Task | State at backup | Assignment events | Completion event |
| ---: | --- | --- | ---: |
| 1 | Completed | 6 only | 8, preserved |
| 2 | Started, interrupted | 11 originally; 16 after restore | 18 |
| 3 | Open, dependency waiting | 19 after task 2 completed | 21 |

The old assignment expired at event 14. Task 2's replacement was explicitly
attempt 2 on the new worker instance; task 3 referenced task 2's completion at
event 18. Every task had exactly one completion. No already-completed task was
reassigned. All 12 original events remained the exact prefix of the 21-event
restored log; the original database stayed at its backup state. All trial-owned
processes were stopped afterward.

Evidence directory: `outputs/agent-bus-trials/v011-post-restore-20260913-173255-51be1b`
in the Codex task workspace. It contains before/after event exports,
`summary.json`, separate process logs, the original database, verified bundle,
and restored database. Correlation: `post-restore-eaecc69c`.
Runner: `outputs/v011_post_restore_trial.py`.

This closes the focused live recovery check. It does not claim exactly-once
external side effects, real-agent/model recovery, power-loss durability, or
long-running soak reliability.

## Sustained local soak — 2026-09-13

**PASS.** A 30-minute workload-generation run finished and drained in 1,807.58
seconds. One isolated loopback bus, two demo workers, and a snapshot-enabled PM
processed 179 five-task root/fan-out/fan-in DAGs. No model calls or existing user
workflows were involved.

- 939 tasks: 895 completed and 44 deliberately cancelled; 12,171 events.
- 89 active-worker kills produced 89 `worker lease expired` events and recovery.
- 59 PM restarts and 59 acknowledged workflow pause/resume cycles.
- 180 full-replay comparisons passed, including the final drained state.
- No duplicate task completions; assignment dependency references pointed to
  earlier matching completion events.
- 179 progress/memory samples; peak observed RSS was 56.75 MiB for the server,
  75.02/66.02 MiB for the worker roles, and 84.23 MiB for the PM, below the
  512 MiB per-process safety threshold. These sampled peaks do not prove absence
  of leaks over longer durations.
- Artifact audit verified 895 referenced artifacts with no issues or orphan
  candidates. Backup verification and restored full-state equivalence passed.
- Export checksum verified; process logs contained no traceback or idempotency
  conflict. All trial-owned processes were stopped afterward.

Evidence: `outputs/agent-bus-trials/v011-soak-20260913-174221-9f1e4b` in the
Codex task workspace, including `summary.json`, `samples.jsonl`, `events.json`,
process logs, the backup bundle, and restored database. Runner:
`outputs/v011_soak.py`. Event-export SHA-256:
`ea520005f6f17e12f8c0b43d29326a71c883614105481e560bf923324b08dde8`.

This closes the first bounded local soak checklist, not all v0.11 validation.
It does not establish week-long stability, genuine-agent/model behavior,
power-loss durability, exactly-once external effects, or exhaustive concurrent
completion/cancellation/deadline race coverage.

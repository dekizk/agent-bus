# Release notes

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

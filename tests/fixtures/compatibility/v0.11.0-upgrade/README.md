# Released database fixture

Captured from v0.11.0, commit `c06f33ac68873140ec5eca98ef9e84dc2d3bf63b`.
`database.sql` is SQLite's dump of a disposable database created and populated
by that release's bus and PM, including its actual schema, snapshot, indexes,
idempotency keys, counters, causal links, and policy. It is not a hand-written
approximation of the old schema. It contains only synthetic local data.

`expected.json` records release-produced events, CLI JSON/exit results, adapter
messages/effect IDs, the next reconciliation plan, SQL checksum, and hashes of
the imported release modules. The capture script verifies those modules against
the pinned Git commit. No old package or network access is needed by tests.

The eight tasks cover completed parent / ready dependent, human-blocked, failed,
paused, cancellation awaiting acknowledgement, active work, and a deadline.
The saved snapshot intentionally precedes the final three events. Missing
usage holds reservations against an explicit workflow token policy.

To reproduce into a **new** directory, extract the pinned commit with
`git archive`, then run from the development checkout:

```sh
python scripts/capture_upgrade_fixture.py --source /path/to/extracted-release --output /path/to/new-capture
```

Review differences; never regenerate expectations just to make a test pass.
The SQL fixture preserves logical stored values, not original SQLite file/WAL
bytes. Tests may accept or discard old snapshots, provided replay agrees;
private snapshot serialization is not a compatibility promise. This covers
v0.11.0 to current development, not earlier releases or downgrade support.

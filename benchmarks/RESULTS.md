# v0.11 phase 1 — task identity index

Measured 2026-09-13 on macOS 26.2 arm64, Python 3.13.7. Baseline storage
was commit `a1c3dd2`; the same harness was run before and after the index change.
Each run creates disposable databases. No live user database or agent is used.

Reproduce from the repository root:

```sh
.venv/bin/python benchmarks/task_lookup.py --sizes 100 1000 10000 --repeats 10
```

The fixture bulk-inserts independent `task.created` events, then regenerates the
derived index if available. Fixture setup is excluded. Lookup uses one open
connection and warmed OS/SQLite caches. Appends use real `append_event`, a new
connection, and a dependency on the last fixture task. Ten samples are reported
as medians. Replay uses three samples and the same pure reducer used by the PM;
its input includes the ten appended tasks. Peak memory is measured separately
with `tracemalloc` and excludes the already-loaded event list. These are local
microbenchmarks, not end-to-end service throughput or a production sizing guide.

| Fixture tasks | Late lookup ms, before → after | Missing lookup ms, before → after | Dependency append ms, before → after |
| ---: | ---: | ---: | ---: |
| 100 | 0.2251 → 0.0042 | 0.2092 → 0.0028 | 1.3923 → 1.0862 |
| 1,000 | 2.1335 → 0.0044 | 2.1344 → 0.0030 | 3.1503 → 1.0613 |
| 10,000 | 21.6393 → 0.0044 | 21.4531 → 0.0030 | 23.2253 → 1.1414 |

| Fixture tasks | Replay ms, before → after | Replay peak allocated bytes, both runs |
| ---: | ---: | ---: |
| 100 | 0.5726 → 0.6925 | 247,344 |
| 1,000 | 5.6284 → 5.8044 | 2,295,568 |
| 10,000 | 62.4678 → 68.8560 | 22,713,576 |

The intended gain is removing task-history parsing from lookup and append.
Replay was not modified; its timing variation is not evidence of improvement
or regression from this index. A full regression run was also active during
the post-change measurement, so timings are illustrative, not thresholds.
Query-plan tests separately require primary-key searches for both lookup joins.

Remaining work: worker admission (initial and incremental) with transport costs,
mixed telemetry/coordination logs, many workers, realistic wide/deep DAGs,
snapshots, cold-cache and disk-growth measurements, and long-running workloads.
Existing adoption/supersession migrations still scan history at startup; this
change does not claim constant-time startup.

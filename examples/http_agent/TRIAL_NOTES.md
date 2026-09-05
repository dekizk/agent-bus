# HTTP adapter trial notes

## 2026-08-27 — v0.9 loopback HTTP integration

Purpose: close the remaining live-evidence gap for the public HTTP bridge by
running a real local HTTP agent through conformance, normal completion, a
retry, and cooperative cancellation.

Setup:

- ran a loopback FastAPI agent implementing the same protocol behavior now
  preserved in this checked-in example;
- connected it through the public configured HTTP adapter with capability
  `http-example`;
- used the bus, PM, and adapter runtime rather than publishing lifecycle
  outcomes directly;
- inspected both the replayable event log and the agent's observation endpoint.

Observed evidence:

- the standalone conformance delivery completed without recording a logical
  effect;
- task 6 (`18159664eaa44f6c99f4b7102c98a933`) followed
  `task.created` #339 -> `task.assigned` #340 -> `task.started` #341 ->
  `task.completed` #342 on attempt 1;
- task 7 (`8dc3207dc9094566ba5f6b95b5a0b840`) returned HTTP 503 on attempt 1,
  producing `task.attempt_failed` #352, then completed attempt 2 at #355;
- task 7's attempt-scoped assignment IDs changed from `task:7:attempt:1` to
  `task:7:attempt:2`, while both deliveries used effect scope
  `effect-scope:789c7c3cfeb804c90911d9f7caacfccb52b709f3142e1a471576ebcdfe5f724c`;
- the HTTP agent recorded exactly one execution of the retry-stable logical
  effect across those two deliveries;
- task 8 (`454bc08c732d4bbd9827b5d410f56d57`) started at #366, received
  `task.cancel_requested` #371, and reached `task.cancelled` #372;
- the agent recorded cancellation of `task:8:attempt:1`, and a query after
  event #372 found no late started, assigned, failed, or completed lifecycle
  event for that attempt;
- both services remained healthy, and the HTTP observation endpoint agreed
  with the immutable lifecycle history.

Result: PASS. The loopback HTTP bridge handles a bounded assignment, translates
retryable HTTP failure into a monotonic new attempt, preserves logical-effect
identity across that retry, and cooperates with cancellation while the runtime
retains final lifecycle authority.

Boundary: the live harness and checked-in example use an in-memory effect ledger
for visibility. Production agents need durable idempotency storage before
performing irreversible work.

## 2026-09-05 — v0.10 phase 1 scheduling

Purpose: prove immutable priority ordering, delayed eligibility, and
non-preemption with the checked-in HTTP integration at capacity one.

Setup:

- used a disposable bus database and one `minimal-http-agent` worker;
- created an older low-priority task, two urgent tasks, and one delayed
  high-priority task under correlation `v010-scheduling-20260830` before
  starting the PM;
- later held one low-priority assignment active and created an urgent task
  while the sole worker remained occupied;
- saved the workflow projection, selected lifecycle events, and HTTP-agent
  observations independently.

Observed evidence:

- initial assignment order was task 2 (urgent A), task 3 (urgent B), task 1
  (older low), then task 4 (delayed high), at events #16, #19, #22, and #29;
- the two equal urgent tasks followed immutable creation order (#8 before #9),
  while both correctly preceded the older low task created at #6;
- task 4 recorded `not_before: 1788566209.88241` and was not assigned until
  `1788566211.242413`, approximately 1.36 seconds after eligibility;
- tasks 1–4 each completed once on attempt 1 with one recorded logical-effect
  execution;
- task 5 started its low-priority attempt at event #108; urgent task 6 was
  created at #117 but received no assignment while task 5 retained the only
  worker;
- task 5's requested cancellation became authoritative at #123, and only then
  was task 6 assigned at #124 and completed at #126;
- the HTTP agent observed cancellation of `task:5:attempt:1`; no logical effect
  was recorded for that deliberately held task, while all five completed tasks
  recorded exactly one effect each;
- the final replay projection contained five completed tasks and one cancelled
  task, with no retries or duplicate executions.

Result: PASS. Priority controls only the next eligible assignment, equal
priorities retain deterministic creation order, `not_before` is a hard lower
eligibility bound, and new urgent work does not revoke an active lower-priority
owner.

Friction and follow-up:

1. Starting Uvicorn outside the checkout made `examples` unimportable. The
   example now passes an explicit `--app-dir` and documents the absolute-path
   fallback.
2. The existing editable installation predated the new top-level
   `scheduling.py`; reinstalling with `python -m pip install -e .` refreshed
   console-entry-point imports. A built wheel includes the module through the
   updated package configuration.
3. The final workflow label was `ended_with_failures` because one task was
   deliberately cancelled. This is not a scheduling error, but phase 2 should
   decide whether an intentional control outcome deserves more neutral workflow
   wording.

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

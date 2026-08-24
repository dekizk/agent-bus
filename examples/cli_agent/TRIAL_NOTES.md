# CLI adapter trial notes

## 2026-08-19 — v0.9 configured CLI onboarding

Purpose: prove that a non-Hermes agent can pass conformance and complete a
controlled assignment through only the public protocol/configuration path.

Setup:

- built the `agent_bus-0.9.0` wheel and installed it into a disposable virtual
  environment;
- verified the installed `agent-bus 0.9.0` entry point, public SDK imports, and
  `agent-bus init` outside the repository;
- ran `agent-bus adapter check --config examples/cli_agent/adapter.json`;
- started the local bus, PM, and configured minimal CLI worker through the new
  onboarding commands;
- submitted one controlled task requiring `cli-example` with correlation
  `v09-live-cli-20260819`.

Observed evidence:

- conformance: PASS for execute hook, typed `Completed` outcome, protocol-v1
  round trip, inline-result limit, cancellation hook, and side-effect guidance;
- `task.created` #4 -> `task.assigned` #5 -> `task.started` #6 ->
  `task.completed` #7;
- assignment: `task:1:attempt:1` on worker instance
  `134f0a14b6b249309f495debb936005a`;
- final summary: `Processed: Prove the configured CLI adapter works`;
- no retryable failures, no ownership race, and no Hermes-specific code;
- Mermaid export derived one completed node from the same workflow history.

Result: PASS. An existing JSON-capable CLI can be attached through a small
configuration and versioned stdin/stdout boundary while agent-bus retains
orchestration ownership.

## 2026-08-24 — v0.9 post-hardening configured CLI re-test

Purpose: confirm that the runtime, protocol, effect-identity, configuration,
stream-error, and onboarding hardening applied after multi-agent review did not
regress the public configured-CLI execution path.

Preflight evidence:

- complete suite: 162 passed with one known FastAPI/Starlette deprecation
  warning;
- focused runtime/executor/client/integration-kit suite: 63 passed;
- configured CLI adapter conformance: PASS, including typed outcome,
  protocol-v1 round trip, inline-result limit, cancellation hook, and the
  attempt-versus-effect identity guidance.

Observed live evidence:

- task 5, `Verify the hardened v0.9 CLI integration`, completed at event #163;
- workflow correlation: `c4a5b24ac244464baae478ddec396ed1`;
- one monotonic assignment, `task:5:attempt:1`, recorded at event #161;
- executor: `minimal-cli-agent`, worker instance
  `1cc45175583149fb8157bb6ee411d1ac`;
- attempt 1 completed with zero retryable failures and one configured retry
  remaining;
- final summary: `Processed: Verify the hardened v0.9 CLI`;
- the replay-derived task and workflow views both reported `completed`;
- no token, cost, or model-call telemetry was reported, as expected for the
  minimal non-model CLI example.

Result: PASS. The hardened v0.9 configured-adapter path still completes one
bounded assignment under agent-bus ownership, and the task and workflow
projections agree with the immutable lifecycle history. The separate automated
race tests—not this happy-path run—provide evidence for expired-delivery
fencing, replay deduplication, phase-safe cancellation, and cleanup
serialization.

Follow-up: the manual Python-target check exposed that an installed console
launcher did not put its current working directory on `sys.path`, even though
the same target imported through `python -c`. The loader now deliberately adds
the launch directory for explicitly requested `module:attribute` targets, with
a regression test covering console-style path initialization.

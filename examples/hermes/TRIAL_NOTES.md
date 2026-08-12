# Hermes integration trial notes

Use this file to record evidence from real tasks before changing the agent-bus
roadmap. Do not record prompts, credentials, private task content, or full model
transcripts.

## 2026-08-05 — initial lifecycle smoke test

Task: summarize one short note supplied in assignment context, with Hermes safe
mode enabled and `clarify` as the only toolset.

Result:

- the agent-bus event, PM assignment, Hermes execution, typed outcome, lifecycle
  publication, and replay projection completed end to end;
- Hermes returned strict JSON without requiring a compatibility parser;
- the final projection was `completed`;
- the usage callback reported 3,582 tokens and `$0.00` estimated cost without
  writing usage into the coordination log;
- the normal `events.db` and working tree were not used by the live task.

Friction observed:

1. A provider selected in Hermes configuration may not have a currently usable
   credential. The first configured provider was logged out. Operators should
   preflight with `hermes auth status PROVIDER`; the adapter correctly translated
   the failed invocation into a retryable outcome.
2. Hermes needs write access to its authentication lock. A restricted worker
   environment that can execute the binary but cannot access the Hermes auth
   store fails before inference. Deployment documentation must treat the Hermes
   home/auth store as an explicit runtime dependency.
3. Usage metadata is useful immediately, but it does not belong in coordination
   events. The callback boundary is sufficient for this trial and supports the
   planned separate telemetry stream.

Roadmap evidence so far:

- no executor-contract change was required;
- no DAG/dependency requirement appeared in this deliberately simple task;
- telemetry remains useful, but more genuine multi-step tasks are needed before
  deciding whether telemetry or DAG orchestration should be prioritized next.

## 2026-08-11 — repository audit and human-decision trial

Tasks: a read-only audit of the disposable agent-bus copy, followed by a task
that intentionally omitted its required release target.

Result:

- the file audit completed through the normal lifecycle and returned three
  concise findings;
- the missing-input task correctly emitted `task.blocked`, and the PM emitted
  `decision.needed`;
- the human response `Use staging` was recorded as `decision.made`, and the PM
  created a second assignment;
- the second assignment retained the original `release_target: null` context
  but did not carry the decision, so the stateless Hermes process correctly
  blocked again.

Friction and resolution:

1. Reopening a task was insufficient unless the accepted human answer became
   part of the next executor input. This was an executor-contract gap rather
   than a Hermes-specific adapter problem.
2. v0.4.1 adds chronological immutable decision records to PM state,
   `task.assigned`, and `AssignmentContext`. Historical assignments default to
   an empty history. Reducer replay, multiple decisions, and an end-to-end
   blocked-to-completed flow are covered by regression tests.
3. The live Hermes re-test passed after the adapter made decision precedence
   explicit. Attempt 5 retained the immutable original `release_target: null`
   context, received the accumulated decision history, applied the latest
   structured `release_target: staging` response, and emitted `task.completed`
   with staging-specific instructions. This confirms the repaired path works
   with a real stateless agent, not only the in-process regression fixture.

## Template for the next task

- Date and task category:
- Toolsets and authority granted:
- Outcome and retry behavior:
- Missing assignment context:
- Missing outcome semantics:
- Cancellation/ownership behavior:
- Telemetry or artifact friction:
- Dependency/DAG friction:
- Candidate roadmap change:

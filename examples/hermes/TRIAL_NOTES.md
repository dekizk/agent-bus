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

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

## 2026-08-12 — worker replacement recovery trial

Task: a read-only architecture-risk audit (`trial-worker-recovery-001`,
events 1145–1175), with the worker process killed and replaced mid-execution.

Result:

- attempt 1 started on the original instance; a replacement instance
  registered under the same worker name while it was running;
- the PM expired attempt 1 immediately on seeing the replacement registration
  (`task.assignment_expired`, reason `worker process was replaced`) rather
  than waiting for the lease timeout;
- attempt 2 was assigned to the new instance and completed with a structured
  result;
- no stale lifecycle event from the killed attempt reached the log: runtime
  ownership-loss suppression and Hermes process-group termination both held
  under a real paid execution.

Friction observed:

1. Recovery cost one full paid re-execution of the task. The coordination log
   records that a retry happened but not what it cost; spend visibility
   currently exists only in the worker's stdout usage lines. This is telemetry
   evidence, not a coordination gap.
2. The recovered task's own output flagged a true limitation: SSE wake-ups are
   process-local, so the bus supports exactly one process — multiple uvicorn
   workers or a second process appending to the same SQLite file can leave
   subscribers unwoken until keepalive. The README constraint should say
   "single process", not just "single host".

## 2026-08-12 — two-task dependency chain trial

Tasks: a documentation audit (task 4) whose structured findings were manually
fed as context into a planning task (task 5), sharing one correlation
(`trial-dependencies-001`, events 2262–2327).

Result:

- both tasks completed on their first attempt through the normal lifecycle;
- task 4 returned three structured findings; task 5, given those findings as
  `upstream_result` context, returned a prioritized implementation plan with
  acceptance criteria;
- one `correlation_id` groups the entire workflow, so the chain is queryable
  end to end.

Friction observed (this is the DAG evidence the roadmap was waiting for):

1. The causal edge was lost. Task 5's `task.created` has `caused_by: null`;
   the log records shared correlation but not that task 5 depended on task 4's
   completion. Setting `caused_by` to the upstream `task.completed` event id
   would have recorded the edge and inherited the correlation automatically —
   the mechanism exists today but nothing encourages or automates it.
2. The upstream result was copied by hand and is now stored three times: in
   task 4's completion, task 5's `task.created` context, and again in the
   materialized `task.assigned`. Chained inline results consume the 16 KiB
   context budget multiplicatively; a two-hop chain with a larger result
   would already be near the limit.
3. The human acted as the scheduler: waiting for task 4, extracting its
   result, and composing task 5 by hand. Mechanical and error-prone —
   exactly what dependencies, readiness, and result propagation would
   automate.

Candidate roadmap change:

- accumulated evidence now favors DAG orchestration as the next layer:
  `depends_on` in `task.created`, PM-held readiness (dependents stay
  unassignable until upstream completes), and upstream results injected into
  dependent assignments by reference rather than by copy — which also
  addresses the duplication problem and naturally introduces the
  blob/reference question;
- telemetry pain so far is real but milder (spend visibility on retries,
  usage queryability) and can follow the DAG layer.

## 2026-08-13 — v0.5 automatic Hermes DAG trial

Tasks: Task A analyzed the agent-bus orchestration note and Task B declared
`depends_on: [5]` to turn A's findings into an implementation plan
(`hermes-v05-dag-success-2`, task events 396–785).

Result:

- the PM held Task B unassigned while Task A was incomplete;
- Task A attempt 1 was assigned to a worker that stopped heartbeating and was
  recorded as `task.assignment_expired`;
- a replacement Hermes instance received monotonic attempt 2 and completed
  Task A with three structured findings;
- the PM immediately assigned Task B with `caused_by: 779` and only
  `dependency_refs: [{"task_id": 5, "completion_event_id": 779}]`;
- Task B's creation and assignment did not copy Task A's result. The runtime
  resolved completion event 779 into `AssignmentContext.dependencies`, and
  Hermes used those findings to return a prioritized three-step plan with
  acceptance criteria;
- both nodes share one correlation, and replay records the initial lease loss,
  replacement recovery, readiness transition, reference resolution, and both
  final completions without a mutable workflow board.

Friction and boundaries observed:

1. Starting the PM after the original worker had stopped created a legitimate
   lease-expiry retry. This confirms crash-safe recovery but also demonstrates
   that infrastructure loss consumes the task's configured retry budget.
2. The successful two-node trial validates the v0.5 DAG abstraction with a
   real external agent: the PM, rather than the operator, performed readiness
   scheduling and the result moved by immutable event reference rather than
   manual copying.
3. Dependency lookup is intentionally local-scale today. Task identity lookup
   scans `task.created` events on the append path, and resolved inline inputs
   are capped at 32 KiB. A projection index and artifact/blob references are
   the respective scale-up paths when real workloads reach those boundaries.

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

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

## 2026-08-15 — v0.6 captured telemetry DAG/retry trial

Tasks: a disposable captured smoke invocation, followed by a two-node Hermes
DAG in which Task A deliberately returned one retryable executor outcome before
completing and Task B consumed A's immutable completion
(`hermes-v06-dag-retry-1786767615`, lifecycle events 25–44).

Result:

- the captured smoke completed with one model start and one terminal event,
  3,931 tokens, reported cost `$0.017624`, and hash-verified model-input and
  model-output references;
- Task A attempt 1 produced a real successful model invocation and then the
  instructed `controlled_trial_retry` outcome. The runtime emitted
  `task.attempt_failed` with `retryable: true`, and the PM created monotonic
  attempt 2;
- Task A attempt 2 completed. Only then did the PM assign Task B, with
  `caused_by: 38` and
  `dependency_refs: [{"task_id": 1, "completion_event_id": 38}]`;
- Task B completed from its resolved upstream dependency. The live PM ignored
  the interleaved telemetry stream and continued replaying coordination topics
  only;
- all three assignments had one deterministic model start and one causally
  linked `telemetry.model.completed` event. The invocation ids were
  `task:1:attempt:1:model:1`, `task:1:attempt:2:model:1`, and
  `task:2:attempt:1:model:1`;
- usage was queryable across the workflow: 3,909, 3,957, and 4,157 tokens with
  reported costs `$0.016776`, `$0.017928`, and `$0.021328` respectively
  (12,023 tokens and `$0.056032` total);
- the six current model-input/output references had six distinct SHA-256
  digests and all passed store integrity verification. Telemetry payloads
  contained no `prompt`, `input_content`, `output_content`, or `tool_data`
  fields.

Friction and boundaries observed:

1. A successful model invocation can correctly coexist with a retryable
   coordination outcome. Attempt 1's model span is `completed` because Hermes
   returned valid output; the resulting executor outcome is separately recorded
   as `task.attempt_failed`. The two streams describe different layers rather
   than conflicting states.
2. The persistent artifact directories contained files from earlier runs in
   addition to the six references for this correlation. This is expected from
   an immutable content-addressed store, but confirms that retention/garbage
   collection remains a real operational follow-up rather than an abstract
   concern.
3. This was a controlled application-level retry, not a process crash. The
   existing coordination suite and earlier worker-replacement trial cover lease
   recovery; a crash may legitimately leave a started telemetry span without a
   terminal event because telemetry is observational and has no durable outbox.

Roadmap evidence:

- v0.6 now closes the usage/cost visibility gap observed during the v0.4/v0.5
  trials without adding telemetry load to PM replay;
- artifact references preserve the SQLite content boundary while making
  captured content independently verifiable;
- management controls, cancellation/deadline semantics, and eventually safe
  unreferenced-artifact collection remain candidates for subsequent versions.

## Next trial criteria — telemetry under lease expiry

The completed 2026-08-15 trial above covered normal completion and a controlled
application-level retry. A future trial should isolate hard worker loss and
lease-expiry recovery: confirm that the replacement attempt receives a new
deterministic invocation id, remains queryable under the same workflow
correlation, and completes without telemetry entering PM replay. If the old
process dies after its start event, the log should preserve that incomplete
span rather than inventing a terminal event; usage may be absent when Hermes
could not finish writing it. With disposable content capture enabled, verify
that any emitted artifact references still pass integrity checks and no
prompt/output fields appear inline in telemetry. Record that separate evidence
using the trial-note template at the end of this file.

## 2026-08-15 — v0.7 cancellation and deadline Hermes trial

Tasks: two disposable, deliberately long Hermes assignments using the
`clarify` toolset and no content capture. Each root had a dependent DAG node.
The first root was cancelled after it started (tasks 3–4, coordination events
950–958, correlation `4295fa733f40496dab9274a0b3c47484`). The second root had
a three-second persisted deadline (tasks 5–6, coordination events 1004–1013,
correlation `5fc8c1086e1840f2848c2754c2d7e8ae`).

Result:

- task 3 was assigned once and emitted `task.started` before the human
  `task.cancel_requested` event 955. The PM emitted exactly one
  `task.cancelled` event 956, caused by that request and retaining
  `task:3:attempt:1` as its last assignment;
- Hermes cancellation produced a causally linked `telemetry.model.failed`
  event with `error_code: hermes_cancelled` after about 80 ms. The runtime
  suppressed the executor's retryable outcome, so no coordination-level
  `task.attempt_failed`, completion, retry, or reassignment was recorded;
- task 4 was never assigned. The PM emitted `task.dependency_failed` event 958,
  caused by task 3's cancellation event;
- task 5's absolute deadline was preserved unchanged from `task.created` into
  `task.assigned`. It started one Hermes invocation, and the runtime's local
  timer cancelled that process after about 2.96 seconds, before the PM recorded
  `task.deadline_exceeded` event 1012;
- the deadline event was caused by task 5's creation event, retained
  `task:5:attempt:1`, and did not create another attempt despite a configured
  two-retry allowance;
- task 6 was never assigned. The PM emitted `task.dependency_failed` event 1013,
  caused by the upstream deadline event;
- a delayed read of the immutable SQLite log found no later completion,
  assignment, attempt failure, duplicate terminal event, or other lifecycle
  change for either workflow.

Replay evidence:

- task 3: `cancelled`, one attempt, zero retryable failures;
- task 4: `dependency_failed`, zero attempts, zero retryable failures;
- task 5: `deadline_exceeded`, one attempt, zero retryable failures;
- task 6: `dependency_failed`, zero attempts, zero retryable failures.

Friction and boundaries observed:

1. Cancellation and deadlines correctly terminate coordination without
   consuming retry budget, even though the interrupted Hermes invocation is
   represented as a retryable failure in the separate telemetry stream. This
   confirms that telemetry describes executor activity without controlling PM
   lifecycle decisions.
2. Local deadline enforcement occurred before the PM's terminal event, showing
   that an active subprocess stops at its persisted cutoff even if PM
   reconciliation is slightly later. The PM event remains the replayable,
   authoritative task outcome.
3. Both terminal states propagated through declared dependency edges without
   assigning doomed downstream work. No operator scheduling or copied result
   payload was required.
4. The live telemetry producer still identified `examples.hermes` as version
   `0.6.0`. The adapter version constant should be advanced to `0.7.0` before
   the release is committed; this is release metadata, not a lifecycle defect.

Roadmap evidence:

- the v0.7 cancellation/deadline model now has live evidence across the bus,
  PM projection, worker runtime, Hermes subprocess cancellation, telemetry,
  replay, and DAG propagation;
- priority, pause/resume, or richer operator inspection should remain
  trial-driven follow-ups rather than being inferred from this successful
  control-plane trial.

## 2026-08-16 — v0.8 packaging and demo-DAG preflight

The v0.8 project was installed from the checkout into a disposable virtual
environment with no dependency downloads. The generated console command
reported `agent-bus 0.8.0` and exposed all six read-only operations commands.

An installed-package local smoke then ran a disposable server, PM, and demo
worker against a two-task DAG (`76f93d6d032243288aa2fed9a186b583`, coordination
events 3–11). `agent-bus doctor` reported a healthy schema-v2 bus and worker;
`agent-bus workflow` showed both tasks completed, Task 2's dependency on Task
1, and state events `#6` and `#11`; `agent-bus task 2 --json` included the
assignment, dependency, retry, ownership, completion summary, and trace fields.
With no telemetry in this demo run, the human output correctly said `tokens not
reported` and `cost not reported` instead of inventing zero-cost evidence.

This preflight confirms packaging, public HTTP replay, PM compatibility, DAG
rendering, JSON output, and missing-telemetry wording. It is not a substitute
for the live Hermes trial below, which must exercise real telemetry and the
remaining commands before v0.8 is called complete.

## 2026-08-16 — v0.8 read-only Hermes visibility trial

The v0.8 CLI replayed the genuine two-task Hermes DAG and telemetry history
from `hermes-v06-dag-retry-1786767615` without invoking another paid model
request or inspecting raw event payloads.

Result:

- `agent-bus doctor` reached the live schema-v2 bus and replayed 1,083
  coordination events into six tasks. It reported zero healthy and two stale
  workers, which accurately describes this archived workflow rather than
  implying that its historical Hermes process is still live;
- `agent-bus workers` traced the stale `hermes` and `hermes-v07` registrations
  to their latest heartbeat events (`#943` and `#1093`) and displayed capacity,
  lease age, and capabilities;
- `agent-bus workflow` rendered two completed tasks and the enforced edge
  `1 -> 2`. Task 1 completed at `#38` after one retryable failure and Task 2
  completed at `#44` only after dependency event `#38`;
- the workflow view totaled three completed model spans, zero failed or open
  spans, 12,023 tokens, 15,074.686916545 ms, and reported cost `$0.056032` for
  `nous/openai/gpt-5.5`. Telemetry evidence was `#28`, `#31`, `#35`, `#37`,
  `#41`, and `#43`;
- `task 1` exposed attempt 2, one retryable failure, zero remaining retries,
  completion summary, last assignment `#33`, and completion trace `#38`.
  `task 2` exposed dependency `1=completed@#38`, assignment `#39`, one
  remaining retry, and completion trace `#44`;
- `explain 1` and `explain 2` returned concrete completed reasons with traces
  `#38` and `#44` rather than merely repeating a status label;
- `tail --from-id 24` replayed the readable causal sequence from task creation
  through Task 1's controlled retry, Task 1 completion, Task 2 readiness, and
  Task 2 completion, interleaving telemetry without exposing full payloads;
- the saved JSON view represented the same DAG, retry, ownership, usage, cost,
  and trace data. Automated and clean-environment checks confirm that finite
  observer commands create no offsets, cache, database, or mutable projection.

Friction found and corrected during the trial:

1. The first candidate labeled the retained assignment provenance on completed
   tasks as `Owner`, which could imply that Hermes still owned terminal work.
   The final candidate now says `Current owner` only for assigned/started work,
   says `Last attempt` for terminal provenance, and includes
   `assignment_active: false` in terminal JSON.
2. Stale worker registrations remain visible indefinitely because they are
   immutable history. Showing them as stale with lease age and event id is
   useful; `doctor` correctly avoids warning that work cannot be assigned when
   there is no open work.

Roadmap evidence:

- every v0.8 read-only command now has live or automated evidence;
- the two-task workflow is understandable without a board or raw event JSON;
- coordination truth remained solely in immutable history while the CLI view
  was rebuilt for each invocation;
- v0.8 is ready for commit and push after final human review.

## 2026-08-18 — v0.8 documentation DAG and live fan-in trial

A genuine five-task Hermes workflow turned the earlier README-improvement
plan into an immutable DAG under correlation
`37e6bec0682a47959561123f90d759be` (events `#15`–`#77`). Hermes received the
`file` toolset with explicit read-only instructions and ran at capacity three.
Content capture was enabled in a disposable artifact directory.

The graph was declared before its prerequisites completed:

- task 1 re-audited the current README against the v0.8 implementation;
- tasks 2–4 each depended on task 1 and independently drafted the quick-start,
  architecture/invariants, and event-contract sections;
- task 5 depended on tasks 2–4 and merged their results into one proposed
  README revision with conflicts and recommended edits.

Result:

- all five tasks completed on their first assignment;
- task 1 completed at `#38`, after which the PM immediately assigned tasks
  2–4 at `#39`, `#40`, and `#41`. Their model invocations began within about
  57 ms of one another, exercising real concurrent fan-out;
- the branch tasks completed at `#54`, `#60`, and `#65`. Task 5 was assigned
  at `#66`, caused by the last prerequisite completion, and completed at
  `#77` only after all three declared edges were satisfied;
- the workflow projection rendered five completed tasks and all six expected
  edges: `1 -> 2`, `1 -> 3`, `1 -> 4`, `2 -> 5`, `3 -> 5`, and `4 -> 5`;
- branch results totaled 16,871 encoded bytes when resolved for task 5,
  remaining below the 32 KiB aggregate dependency limit. The merged result
  was 12,678 bytes, below the 16 KiB result limit;
- ten model-input/output artifact references passed size and SHA-256
  verification, while telemetry retained only compact references;
- five model invocations completed with zero failed or open spans. Hermes
  reported 550,933 total tokens, 234,602.322916035 ms, and `$1.253112` for
  `nous/openai/gpt-5.5`;
- the generated audit identified six concrete documentation issues and
  returned usable draft Markdown. Hermes did not modify the checkout, and the
  working tree remained clean.

Friction and boundaries observed:

1. The attempted hard-loss injection happened after the workflow had already
   completed: task 1 finished at about 07:45, while the operator ran the kill
   step at about 07:50. The command therefore killed an idle worker. Task 1
   correctly remained at attempt 1, and telemetry correctly showed zero open
   spans. This run must not be cited as lease-expiry evidence.
2. The first verification pipeline sent Python output through `tee` without
   enabling shell `pipefail`. Its text exposed the failed expectation
   (`Open spans preserved after hard loss: 0`), but the pipeline status could
   appear successful. Subsequent runbooks must enable `pipefail` and capture
   stderr when an assertion is part of acceptance.
3. The task-1 result was 7,352 bytes despite a prompt request to stay below
   7,000. The runtime correctly accepted it because the enforced contract is
   16 KiB; prompt-specific advisory limits are not protocol enforcement.
4. The moderate 16,871-byte fan-in passed comfortably. This provides real
   multi-result evidence but does not claim that artifact-backed dependency
   dereferencing or near-32-KiB fan-in has been implemented.

Roadmap evidence:

- the read-only workflow view remained useful while work was actively moving
  through fan-out and fan-in, without introducing a mutable board;
- dependency readiness and result propagation required no operator extraction
  or copied task context;
- the run produced useful project output while exercising sustained Hermes
  operation, concurrency, result-size boundaries, telemetry aggregation, and
  artifact integrity;
- hard worker loss remained an explicit unclosed criterion until the focused
  trial below.

## 2026-08-18 — focused hard-loss and lease-expiry recovery trial

A separate one-task trial isolated the missed failure condition under
correlation `v08-lease-1787004087` (task 6, events `#262`–`#281`). An automatic
watcher was armed before publication and killed the worker immediately after
the first `telemetry.model.started` event. The task allowed one retry and used
the `file` toolset under read-only instructions with disposable content
capture.

Result:

- attempt 1 was assigned at `#263`, started at `#264`, and emitted
  `telemetry.model.started` at `#265` from worker instance
  `8cdb9f7b273e427ca778c5984e8c9507` with invocation id
  `task:6:attempt:1:model:1`;
- the hard-killed process emitted no model terminal event and no task outcome.
  The PM recorded exactly one `task.assignment_expired` event at `#266`,
  caused by the first assignment and carrying reason `worker lease expired`;
- attempt 2 was assigned at `#268` to the distinct replacement instance
  `2e98165c44d8416dbf2f853613becad7`, preserving the same task and workflow
  correlation while using the new deterministic invocation id
  `task:6:attempt:2:model:1`;
- the replacement invocation started at `#270`, completed at `#280`, and the
  task completed exactly once at `#281`. The derived task state was
  `completed`, attempt 2, one retryable failure, and zero retries remaining;
- telemetry reported two started model spans, one completed span, zero failed
  spans, and exactly one open span belonging to the killed attempt. It did not
  invent a terminal event or usage for the lost process;
- the successful replacement reported 193,554 total tokens,
  49,375.073791947 ms, and `$0.4101896` for `nous/openai/gpt-5.5`;
- all three captured artifact references passed size and SHA-256 verification,
  and no prompt, input, output, or content field appeared inline in telemetry;
- the corrected verifier used shell `pipefail` and asserted the two starts,
  one terminal span, one open span, one assignment expiry, one task
  completion, attempt-2 completion identity, and artifact integrity before
  printing `PASS`.

Friction and boundaries observed:

1. Starting the replacement initially failed because `start_crash_worker` was
   a shell function defined only in the original terminal. Supplying the full
   standalone worker command resolved it. Future onboarding and trial
   runbooks should not depend on terminal-local helper functions for recovery
   steps.
2. The gap between expiry `#266` and replacement assignment `#268` reflected
   the human delay in starting a new worker. The task remained durably open in
   immutable history during that interval and advanced when compatible
   capacity returned.
3. The open telemetry span is intentional evidence of uncertainty, not a leak
   to be repaired with an invented failure. Retention and operational views
   should continue distinguishing incomplete spans caused by hard loss from
   completed or explicitly failed invocations.

Roadmap evidence:

- this closes the outstanding v0.6 live criterion for telemetry under hard
  worker loss and lease-expiry recovery;
- crash recovery preserved correlation, attempt identity, ownership fencing,
  retry accounting, and exactly-once terminal task state across two worker
  processes;
- coordination remained authoritative while telemetry truthfully preserved
  the incomplete observation, and artifact content remained outside SQLite;
- together with the five-task DAG trial above, v0.8 now has live evidence for
  read-only in-flight visibility, concurrent DAG execution, fan-in, usage
  aggregation, artifact integrity, and hard-loss recovery.

## Trial-note template

- Date and task category:
- Toolsets and authority granted:
- Outcome and retry behavior:
- Missing assignment context:
- Missing outcome semantics:
- Cancellation/ownership behavior:
- Telemetry or artifact friction:
- Dependency/DAG friction:
- Candidate roadmap change:

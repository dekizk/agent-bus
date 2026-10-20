# Audit triage pipeline

A smart-contract audit workflow coordinated by agent-bus: a lead agent scopes
the audit, reviewer agents split the contracts, duplicate findings are merged,
a human makes the contested severity call, and the lead writes the report and
submits each finding to a contest platform exactly once.

Run it end to end with deterministic stand-in agents (no model, network, or
credentials):

```sh
agent-bus demo audit-triage
```

The demo starts its own bus, PM, and workers in a fresh directory and on a
free port, so it never touches your `.agent-bus/` history or a bus you already
run. It kills one reviewer mid-review and the lead mid-submission with
`SIGKILL`, so both recoveries are real. Add `--yes` to accept the default
severity, or `--pace 0.5` to run faster.

## The workflow

| Task | Depends on | Capability | Stand-in behaviour | A real agent would |
|---|---|---|---|---|
| Scope the audit | — | `lead` | lists three contracts | read the repo and contest scope |
| Review `<contract>` (×3) | scope | `review` | returns canned findings | review the contract and return findings |
| Merge duplicate findings | all reviews | `lead` | groups by `root_cause` | cluster findings with the same root cause |
| Triage severity | merge | `lead` | blocks on the contested finding | ask a human whenever severity is disputed |
| Write the report | triage | `lead` | writes `report.md` | draft the submission write-ups |
| Submit findings | report | `lead` | appends to `contest-inbox.jsonl` | submit to the contest platform |

The stand-ins live in `demo.py` (`ScenarioExecutor`). Each stage reads
`context.stage` and the upstream `result` objects that agent-bus delivers in
`assignment.dependencies`.

## What each part demonstrates

- **Fan-out and fan-in.** The merge task has three `--depends-on` edges and is
  only assigned once every review has completed.
- **Worker loss.** When a reviewer dies, its lease expires, the attempt is
  recorded as `task.assignment_expired`, and the review is reassigned.
- **Human decisions.** The triage agent returns `Blocked(question)`. The PM
  records `decision.needed`. Your `agent-bus decide` answer reaches the next
  attempt through `assignment.decisions`.
- **Exactly-once external effects.** The submitter keys every submission with
  `assignment.effect_id("submit-finding:<id>")`. The key is the same on every
  attempt of the task, so when a replacement lead retries the batch, it skips
  findings the crashed attempt already submitted. Apply the same pattern to any
  real external write: pass the key as the platform's idempotency key, or check
  it before writing, as the demo does.

## Using real agents

Build the same graph on your own bus with `agent-bus submit` and run your
agents as workers with the matching capabilities (`lead`, `review`). Wrap an
existing CLI or HTTP agent with the adapter configs in
[`../cli_agent/`](../cli_agent/) or [`../http_agent/`](../http_agent/) and
set `worker.capabilities` to the stage it handles. Keep results compact
(`result` is limited to 16 KiB inline) and put long reports in artifacts.

The reviewer result shape the merge stage expects is:

```json
{
  "contract": "PriceOracle.sol",
  "findings": [
    {"title": "...", "root_cause": "stale-oracle-price",
     "severity": "Medium", "contested": true}
  ]
}
```

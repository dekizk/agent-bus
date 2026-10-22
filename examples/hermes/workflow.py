"""A real workflow: Hermes does the thinking, a Python agent owns the risky step.

    python -m examples.hermes.workflow --provider PROVIDER --model MODEL

Starts a throwaway bus and coordinator, runs Hermes Agent as one worker and a
small Python "publisher" as another, and submits a three-task chain:

1. Hermes summarises what logplane is (a real model call, with real usage).
2. Hermes drafts a short announcement from that summary (depends on 1).
3. The publisher writes the announcement to a file, but only after a human
   approves, and at most once however many times the task is retried.

Everything Hermes does is paid inference through your own Hermes
configuration; the publisher is plain Python. The bus, coordinator, and
workers are separate processes, and the history is kept for inspection.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional, TextIO

from examples.hermes import fleet
from logplane.demo import Harness, deliver_effect, latest_decision, normalize_choice
from logplane.executors import AssignmentContext, Blocked, Completed, PermanentFailure
from logplane.version import VERSION

PUBLISH_CHOICES = ("approve", "deny")
ANNOUNCEMENT_FILE = "announcement.md"
PUBLISH_LEDGER = "published.db"

MATERIAL = (
    "logplane is a local control plane for AI agents. It hands tasks to agents "
    "that run as separate processes, whatever framework each one uses. Every "
    "assignment, result, retry, and human decision is an immutable event in a "
    "SQLite log, and the current state is rebuilt by replaying the log, so a "
    "crashed worker or coordinator loses nothing. Workers hold leases; when one "
    "dies, its task is reassigned. An agent can block on a question before an "
    "irreversible step, and the task waits for a human answer. Each attempt of "
    "a task gets the same effect_id, which the system being called can use as "
    "an idempotency key to reject a retry's repeat. Agents report tokens and "
    "cost per model call, and a workflow budget stops admitting new work at the "
    "limit. It runs on one machine, binds to loopback, and is MIT licensed."
)


class PublisherExecutor:
    """The one agent allowed to perform the irreversible step.

    It is deliberately not a model: the publish decision belongs to a human,
    and the write is keyed by ``assignment.effect_id`` so a retried or
    overlapping attempt cannot publish twice.
    """

    def __init__(self, workdir: Path):
        self.workdir = workdir

    def execute(self, assignment: AssignmentContext):
        if assignment.context.get("agent_bus_conformance_probe"):
            return Completed("probe ok")
        if assignment.context.get("stage") != "publish":
            return PermanentFailure("unknown_stage",
                                    f"unknown stage {assignment.context.get('stage')!r}")
        if not assignment.dependencies:
            return PermanentFailure("missing_input", "publish needs the announcement upstream")
        text = assignment.dependencies[-1].get("result", {}).get("text")
        if not isinstance(text, str) or not text.strip():
            return PermanentFailure("missing_text", "the upstream result has no 'text' field")

        target = self.workdir / ANNOUNCEMENT_FILE
        question = f"Publish the announcement to {target.name}? (approve/deny)"
        decision = latest_decision(assignment)
        if decision is None:
            return Blocked(question)
        choice = normalize_choice(decision, PUBLISH_CHOICES)
        if choice is None:
            return Blocked(f"{question} ({json.dumps(decision)} is not one of: "
                           f"{', '.join(PUBLISH_CHOICES)}.)")
        if choice == "deny":
            return Completed("Human declined; nothing was written", {"outcome": "denied"})

        effect = assignment.effect_id("publish-announcement")
        record = {"file": target.name, "attempt": assignment.attempt,
                  "approved_by": assignment.decisions[-1].get("actor")}
        if deliver_effect(self.workdir / PUBLISH_LEDGER, effect, record):
            target.write_text(text.strip() + "\n", encoding="utf-8")
            summary = f"Published {target.name} after human approval"
        else:
            summary = f"{target.name} was already published by an earlier attempt; not repeated"
        return Completed(summary, {"outcome": "published", "file": str(target),
                                   "effect_id": effect})


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m examples.hermes.workflow",
        description="Hermes Agent plus a Python approval agent under one logplane coordinator",
    )
    fleet.add_hermes_arguments(parser, default_pace=2.0)
    parser.add_argument("--max-cost", type=float, default=1.0,
                        help="workflow cost cap in USD, applied as budget-aware admission")
    return parser.parse_args(argv)


def run(args: argparse.Namespace, *, out: TextIO = sys.stdout,
        stdin: TextIO = sys.stdin) -> int:
    return fleet.run_example(args, _scenario, prefix="hermes-workflow", out=out, stdin=stdin)


def _scenario(h: Harness, args: argparse.Namespace) -> None:
    h.title("A real agent, a human gate, and a cost cap under one coordinator")
    h.say("Hermes Agent does the thinking as one worker. A small Python agent is the only "
          "one allowed to publish, and it asks you first. Both run as ordinary logplane "
          "workers; the coordinator doesn't know or care what is inside them.")
    h.start_bus()
    h.start_pm()
    fleet.start_hermes_worker(h, args, "hermes")
    fleet.start_python_worker(h, "examples.hermes.workflow", "publisher")

    summarise, workflow = h.submit(
        "Summarise what logplane is", capability="hermes", max_retries=1,
        context={"material": MATERIAL,
                 "instructions": "Write three plain sentences for a developer who runs AI "
                                 "agents, using only the material. Return the sentences in "
                                 "result.text."})
    draft, _ = h.submit(
        "Draft a release announcement", depends_on=[summarise], capability="hermes",
        max_retries=1,
        context={"version": VERSION,
                 "instructions": "Using the upstream summary, write an announcement for "
                                 f"logplane v{VERSION} in under 120 words, friendly and "
                                 "concrete, with 'pip install logplane' on its own line. "
                                 "Return the announcement in result.text."})
    publish, _ = h.submit(
        "Publish the announcement", depends_on=[draft], capability="publish", max_retries=2,
        context={"stage": "publish"})
    h.cli(["policy", workflow, "--max-cost", f"{args.max_cost:.2f}",
           "--reserve-cost-per-assignment", f"{min(0.10, args.max_cost / 2):.2f}",
           "--reason", "example workflow cap"])
    h.say(f"A ${args.max_cost:.2f} cap gates new work on this workflow. Hermes reports real "
          "usage per call; if your provider does not report cost, the tokens still count "
          "and the cost column stays at zero.")

    deadline = args.timeout + 60
    h.say("Waiting for Hermes to summarise (one real model call)...")
    fleet.wait_done(h, summarise, deadline)
    h.view(["task", str(summarise)])
    h.say("Now the draft, which depends on the summary: the coordinator hands Hermes the "
          "summary's result as an input, so nothing is copied by hand.")
    fleet.wait_done(h, draft, deadline)
    h.view(["task", str(draft)])

    needed = h.wait_for("decision.needed", task_id=publish, timeout=60)
    h.say("The publisher refuses to write anything without a human. The question is an "
          "event, and the answer will be too:")
    h.view(["decisions"])
    answer = h.ask("Publish the announcement? (approve/deny)", default="approve",
                   choices=PUBLISH_CHOICES)
    h.cli(["decide", str(publish), "--decision-event", str(needed["id"]),
           "--value", json.dumps(answer)])
    h.wait_status(publish, "completed", timeout=60)
    h.pause(0.5)
    h.view(["task", str(publish)])
    h.view(["workflow", workflow])

    target = h.workdir / ANNOUNCEMENT_FILE
    if answer == "approve" and target.exists():
        h.say("The announcement, written by Hermes and published by the Python agent:")
        h.block(target.read_text(encoding="utf-8"))
    state = h.workflow_state(workflow)
    usage, budget = state["telemetry"]["usage"], state["budget"]
    h.recap([
        f"Two real Hermes calls: {usage['total_tokens']:,} tokens and ${usage['cost_usd']:.4f} "
        "recorded in the log, per call and attempt.",
        f"The budget line is higher (${budget['cost_usd']:.4f}) because the publisher reports "
        "no usage, so each of its attempts is charged its reservation rather than guessed at.",
        "The draft used the summary through the task dependency, not through a prompt you "
        "pasted.",
        ("Publishing waited for your answer; the write is keyed by effect_id, so a retry "
         "could not publish twice.") if answer == "approve"
        else "You said no, and that answer is on record with who gave it.",
        "Hermes and the publisher are different kinds of agent; the coordinator treats "
        "them the same.",
    ])


def main(argv: Optional[list[str]] = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "_worker":
        return fleet.run_python_worker(argv[1:], "publisher", "publish", PublisherExecutor)
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())

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
import os
import sys
import tempfile
from pathlib import Path
from typing import Optional, TextIO

from logplane.demo import DemoError, Harness, deliver_effect, latest_decision, normalize_choice
from logplane.executors import AssignmentContext, Blocked, Completed, PermanentFailure
from logplane.version import VERSION

REPO_ROOT = Path(__file__).resolve().parents[2]
PUBLISH_CHOICES = ("approve", "deny")
ANNOUNCEMENT_FILE = "announcement.md"
PUBLISH_LEDGER = "published.db"
TERMINAL = {"completed", "failed", "cancelled", "dependency_failed"}

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


def _publisher_main(argv: list[str]) -> int:
    from logplane.client import BusClient
    from logplane.runtime import WorkerRuntime

    parser = argparse.ArgumentParser(prog="publisher")
    parser.add_argument("--url", required=True)
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--heartbeat", type=float, default=1.0)
    args = parser.parse_args(argv)
    runtime = WorkerRuntime(
        BusClient(args.url, actor="publisher"), name="publisher",
        executor=PublisherExecutor(Path(args.workdir)), capabilities=["publish"],
        heartbeat_seconds=args.heartbeat,
    )
    print("[publisher] registered, waiting for work", flush=True)
    runtime.run()
    return 0


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m examples.hermes.workflow",
        description="Hermes Agent plus a Python approval agent under one logplane coordinator",
    )
    parser.add_argument("--provider", required=True, help="Hermes provider, made explicit")
    parser.add_argument("--model", required=True, help="Hermes model, made explicit")
    parser.add_argument("--hermes-command", default="hermes", help="Hermes executable")
    parser.add_argument("--reasoning", help="optional Hermes reasoning effort override")
    parser.add_argument("--timeout", type=float, default=600.0,
                        help="hard wall-clock limit per Hermes invocation (seconds)")
    parser.add_argument("--max-cost", type=float, default=1.0,
                        help="workflow cost cap in USD, applied as budget-aware admission")
    parser.add_argument("--pace", type=float, default=2.0,
                        help="narration pace; also sets the worker lease (4x pace, min 1.5s)")
    parser.add_argument("--yes", action="store_true", help="approve publishing without asking")
    parser.add_argument("--dir", help="new or empty directory to keep the history in")
    parser.add_argument("--keep-running", action=argparse.BooleanOptionalAction, default=None)
    return parser.parse_args(argv)


def run(args: argparse.Namespace, *, out: TextIO = sys.stdout,
        stdin: TextIO = sys.stdin) -> int:
    if args.dir:
        workdir = Path(args.dir).expanduser().resolve()
        if workdir.exists() and any(workdir.iterdir()):
            print(f"workflow: directory must be new or empty: {workdir}", file=out)
            return 2
        workdir.mkdir(parents=True, exist_ok=True)
    else:
        workdir = Path(tempfile.mkdtemp(prefix="logplane-hermes-workflow-")).resolve()
    tty = hasattr(stdin, "isatty") and stdin.isatty() and hasattr(out, "isatty") and out.isatty()
    interactive = tty and not args.yes
    color = hasattr(out, "isatty") and out.isatty() and "NO_COLOR" not in os.environ
    h = Harness(workdir, out=out, pace=args.pace, interactive=interactive, color=color)
    # Child processes need the repository on their path for the examples package.
    h.environment["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + h.environment["PYTHONPATH"]
    h.environment["LOGPLANE_HEARTBEAT_SECONDS"] = str(h.heartbeat_seconds)
    h.emit(f"Workflow directory: {workdir}")
    h.emit(f"Bus: {h.url} (lease {h.lease_seconds:g}s; separate from any bus you already run)")
    h.emit(f"Hermes: {args.provider} / {args.model}, safe mode, clarify toolset only. "
           "Its model calls are real and paid.")
    code = 0
    try:
        _scenario(h, args)
        keep_running = args.keep_running if args.keep_running is not None else interactive
        if keep_running:
            h.say(f"The bus is still running at {h.url}. Explore it from another terminal:")
            h.block(f"logplane --url {h.url} workflows\nlogplane --url {h.url} tasks")
            h._tail_stop.set()
            try:
                input("  Press Enter to stop. ")
            except EOFError:
                pass
    except DemoError as exc:
        h.emit(f"\nworkflow: {exc}")
        code = 1
    except KeyboardInterrupt:
        code = 130
    finally:
        h.stop_all()
    h.emit()
    h.emit(f"History is kept in {workdir}. To reopen it later:")
    h.emit(f"  logplane serve --config {h.config_path}")
    return code


def _start_hermes(h: Harness, args: argparse.Namespace) -> None:
    work = h.workdir / "hermes-work"
    work.mkdir(exist_ok=True)
    argv = ["-m", "examples.hermes.run_worker", "--bus-url", h.url, "--name", "hermes",
            "--working-directory", str(work), "--provider", args.provider,
            "--model", args.model, "--hermes-command", args.hermes_command,
            "--timeout", str(args.timeout), "--capability", "hermes"]
    if args.reasoning:
        argv += ["--reasoning", args.reasoning]
    mark = h.last_event_id()
    h._spawn("hermes", argv)
    h.wait_until(lambda: h.find_event("agent.registered", actor="hermes", after=mark),
                 20, "the Hermes worker to register")


def _start_publisher(h: Harness) -> None:
    argv = ["-m", "examples.hermes.workflow", "_publisher", "--url", h.url,
            "--workdir", str(h.workdir), "--heartbeat", str(h.heartbeat_seconds)]
    mark = h.last_event_id()
    h._spawn("publisher", argv)
    h.wait_until(lambda: h.find_event("agent.registered", actor="publisher", after=mark),
                 20, "the publisher worker to register")


def _wait_done(h: Harness, task_id: int, timeout: float) -> dict:
    """Wait for a task to finish; a failure stops the workflow with a pointer."""
    def finished():
        state = h.task_state(task_id)
        return state if state["status"] in TERMINAL else None

    state = h.wait_until(finished, timeout, f"task {task_id} to finish")
    if state["status"] != "completed":
        raise DemoError(f"task {task_id} ended {state['status']}; see logs/hermes.log and run: "
                        f"logplane --url {h.url} explain {task_id}")
    return state


def _scenario(h: Harness, args: argparse.Namespace) -> None:
    h.title("A real agent, a human gate, and a cost cap under one coordinator")
    h.say("Hermes Agent does the thinking as one worker. A small Python agent is the only "
          "one allowed to publish, and it asks you first. Both run as ordinary logplane "
          "workers; the coordinator doesn't know or care what is inside them.")
    h.start_bus()
    h.start_pm()
    _start_hermes(h, args)
    _start_publisher(h)

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
    _wait_done(h, summarise, deadline)
    h.view(["task", str(summarise)])
    h.say("Now the draft, which depends on the summary: the coordinator hands Hermes the "
          "summary's result as an input, so nothing is copied by hand.")
    _wait_done(h, draft, deadline)
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
    if argv and argv[0] == "_publisher":
        return _publisher_main(argv[1:])
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())

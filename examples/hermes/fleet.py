"""Shared plumbing for the Hermes examples that run a whole workflow.

Builds on ``logplane.demo.Harness``, the helper behind ``logplane demo``: a
throwaway bus and coordinator on a free port, real worker processes, a live
event tail, and narration. The harness is a CLI implementation detail rather
than a public API, so these examples pin to the repository they ship with.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path
from typing import Callable, Optional, TextIO

from logplane.demo import DemoError, Harness

REPO_ROOT = Path(__file__).resolve().parents[2]


def add_hermes_arguments(parser: argparse.ArgumentParser, *, default_pace: float) -> None:
    parser.add_argument("--provider", required=True, help="Hermes provider, made explicit")
    parser.add_argument("--model", required=True, help="Hermes model, made explicit")
    parser.add_argument("--hermes-command", default="hermes", help="Hermes executable")
    parser.add_argument("--reasoning", help="optional Hermes reasoning effort override")
    parser.add_argument("--timeout", type=float, default=600.0,
                        help="hard wall-clock limit per Hermes invocation (seconds)")
    parser.add_argument("--pace", type=float, default=default_pace,
                        help="narration pace; also sets the worker lease (4x pace, min 1.5s)")
    parser.add_argument("--yes", action="store_true", help="answer every question with its default")
    parser.add_argument("--dir", help="new or empty directory to keep the history in")
    parser.add_argument("--keep-running", action=argparse.BooleanOptionalAction, default=None)


def run_example(args: argparse.Namespace, scenario: Callable[[Harness, argparse.Namespace], None],
                *, prefix: str, out: TextIO = sys.stdout, stdin: TextIO = sys.stdin) -> int:
    """Run one scenario with the same lifecycle as ``logplane demo``."""
    if args.dir:
        workdir = Path(args.dir).expanduser().resolve()
        if workdir.exists() and any(workdir.iterdir()):
            print(f"{prefix}: directory must be new or empty: {workdir}", file=out)
            return 2
        workdir.mkdir(parents=True, exist_ok=True)
    else:
        workdir = Path(tempfile.mkdtemp(prefix=f"logplane-{prefix}-")).resolve()
    tty = hasattr(stdin, "isatty") and stdin.isatty() and hasattr(out, "isatty") and out.isatty()
    interactive = tty and not args.yes
    color = hasattr(out, "isatty") and out.isatty() and "NO_COLOR" not in os.environ
    h = Harness(workdir, out=out, pace=args.pace, interactive=interactive, color=color)
    # Child processes need the repository on their path for the examples package.
    h.environment["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + h.environment["PYTHONPATH"]
    h.environment["LOGPLANE_HEARTBEAT_SECONDS"] = str(h.heartbeat_seconds)
    h.emit(f"Directory: {workdir}")
    h.emit(f"Bus: {h.url} (lease {h.lease_seconds:g}s; separate from any bus you already run)")
    h.emit(f"Hermes: {args.provider} / {args.model}, safe mode, clarify toolset only. "
           "Its model calls are real and paid.")
    code = 0
    try:
        scenario(h, args)
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
        h.emit(f"\n{prefix}: {exc}")
        code = 1
    except KeyboardInterrupt:
        code = 130
    finally:
        h.stop_all()
    h.emit()
    h.emit(f"History is kept in {workdir}. To reopen it later:")
    h.emit(f"  logplane serve --config {h.config_path}")
    return code


def start_hermes_worker(h: Harness, args: argparse.Namespace, name: str,
                        capability: str = "hermes") -> None:
    work = h.workdir / f"{name}-work"
    work.mkdir(exist_ok=True)
    argv = ["-m", "examples.hermes.run_worker", "--bus-url", h.url, "--name", name,
            "--working-directory", str(work), "--provider", args.provider,
            "--model", args.model, "--hermes-command", args.hermes_command,
            "--timeout", str(args.timeout), "--capability", capability]
    if args.reasoning:
        argv += ["--reasoning", args.reasoning]
    _start(h, name, argv)


def start_python_worker(h: Harness, module: str, name: str) -> None:
    """Start a worker defined by ``python -m <module> _worker`` in this package."""
    argv = ["-m", module, "_worker", "--url", h.url, "--name", name,
            "--workdir", str(h.workdir), "--heartbeat", str(h.heartbeat_seconds)]
    _start(h, name, argv)


def _start(h: Harness, name: str, argv: list[str]) -> None:
    mark = h.last_event_id()
    h._spawn(name, argv)
    h.wait_until(lambda: h.find_event("agent.registered", actor=name, after=mark),
                 20, f"worker {name} to register")


def wait_done(h: Harness, task_id: int, timeout: float, *, log: str = "logs") -> dict:
    """Wait for a task to finish; a failure stops the example with a pointer."""
    terminal = {"completed", "failed", "cancelled", "dependency_failed"}

    def finished():
        state = h.task_state(task_id)
        return state if state["status"] in terminal else None

    state = h.wait_until(finished, timeout, f"task {task_id} to finish")
    if state["status"] != "completed":
        raise DemoError(f"task {task_id} ended {state['status']}; see {log}/ and run: "
                        f"logplane --url {h.url} explain {task_id}")
    return state


def run_python_worker(argv: list[str], name: str, capability: str,
                      make_executor: Callable[[Path], object]) -> int:
    from logplane.client import BusClient
    from logplane.runtime import WorkerRuntime

    parser = argparse.ArgumentParser(prog=name)
    parser.add_argument("--url", required=True)
    parser.add_argument("--name", default=name)
    parser.add_argument("--workdir", required=True)
    parser.add_argument("--heartbeat", type=float, default=1.0)
    args = parser.parse_args(argv)
    runtime = WorkerRuntime(
        BusClient(args.url, actor=args.name), name=args.name,
        executor=make_executor(Path(args.workdir)), capabilities=[capability],
        heartbeat_seconds=args.heartbeat,
    )
    print(f"[{args.name}] registered, waiting for work", flush=True)
    try:
        runtime.run()
    except KeyboardInterrupt:
        print(f"[{args.name}] stopped", flush=True)
    return 0

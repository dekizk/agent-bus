"""Approachable operations, onboarding, and integration CLI for agent-bus."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Optional, TextIO

import httpx

from client import BusProtocolError
from observer import ObserverClient
from operations import (
    ProjectionLookupError,
    build_projection,
    explain_task,
    task_view,
    worker_views,
    workflow_mermaid,
    workflow_view,
)
from projection import CoordinationProjection, PROJECTION_TOPICS, apply_event
from topics import TELEMETRY_TOPICS
from version import VERSION


DEFAULT_URL = "http://127.0.0.1:8765"
DEFAULT_LEASE_SECONDS = 20.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-bus",
        description="Understand agent work from immutable event history.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    _add_common_options(parser, defaults=True)
    commands = parser.add_subparsers(dest="command", required=True)

    doctor = commands.add_parser(
        "doctor", help="check the bus and local coordination view"
    )
    _add_common_options(doctor)

    workers = commands.add_parser(
        "workers", help="show worker capacity and lease health"
    )
    _add_common_options(workers)

    task = commands.add_parser("task", help="show the current state of one task")
    task.add_argument("task_id", type=_positive_int)
    _add_common_options(task)

    workflow = commands.add_parser(
        "workflow", help="show one correlated workflow and DAG"
    )
    workflow.add_argument("correlation_id")
    workflow.add_argument(
        "--mermaid",
        action="store_true",
        help="emit a read-only Mermaid flowchart",
    )
    _add_common_options(workflow)

    explain = commands.add_parser(
        "explain", help="explain why one task can or cannot advance"
    )
    explain.add_argument("task_id", type=_positive_int)
    _add_common_options(explain)

    tail = commands.add_parser("tail", help="follow events for one correlated workflow")
    tail.add_argument("correlation_id")
    tail.add_argument("--from-id", type=_nonnegative_int, default=0)
    _add_common_options(tail)

    init = commands.add_parser(
        "init", help="create a safe local agent-bus configuration"
    )
    init.add_argument("directory", nargs="?", default=".")
    _add_output_option(init)

    serve = commands.add_parser("serve", help="run the local event bus")
    serve.add_argument("--config", default="agent-bus.local.json")

    pm = commands.add_parser("pm", help="run the local orchestration manager")
    pm.add_argument("--config", default="agent-bus.local.json")

    demo = commands.add_parser(
        "demo-worker", help="run a bounded worker for the local quick start"
    )
    demo.add_argument("name")
    demo.add_argument("--config", default="agent-bus.local.json")
    demo.add_argument("--capacity", type=_positive_int, default=1)
    demo.add_argument("--capability", action="append", default=[])

    submit = commands.add_parser("submit", help="submit one controlled local task")
    submit.add_argument("title")
    submit.add_argument("--config", default="agent-bus.local.json")
    submit.add_argument("--context", default="{}", help="JSON object passed to the agent")
    submit.add_argument("--capability", action="append", default=[])
    submit.add_argument("--max-retries", type=_nonnegative_int, default=0)
    submit.add_argument("--correlation-id")
    submit.add_argument(
        "--idempotency-key",
        help="stable identity to make a retried submission safe",
    )
    _add_output_option(submit)

    adapter = commands.add_parser(
        "adapter", help="check or run an existing agent integration"
    )
    adapter_commands = adapter.add_subparsers(dest="adapter_command", required=True)
    adapter_check = adapter_commands.add_parser(
        "check", help="run the standalone adapter contract probe"
    )
    target = adapter_check.add_mutually_exclusive_group(required=True)
    target.add_argument("--config", dest="integration_config")
    target.add_argument("--python-target", metavar="MODULE:ATTRIBUTE")
    _add_output_option(adapter_check)
    adapter_run = adapter_commands.add_parser(
        "run", help="run a configuration-driven agent worker"
    )
    adapter_run.add_argument("integration_config")
    adapter_run.add_argument(
        "--local-config", default="agent-bus.local.json"
    )
    return parser


def _add_common_options(parser: argparse.ArgumentParser, *, defaults: bool = False) -> None:
    default = None if defaults else argparse.SUPPRESS
    parser.add_argument(
        "--url",
        default=(None if defaults else default),
        help=(
            "bus URL (default: local config, then AGENT_BUS_URL, then "
            "http://127.0.0.1:8765)"
        ),
    )
    parser.add_argument(
        "--token",
        default=(os.environ.get("AGENT_BUS_TOKEN") if defaults else default),
        help="bearer token (default: AGENT_BUS_TOKEN)",
    )
    parser.add_argument(
        "--lease-seconds",
        type=_positive_number,
        default=(_lease_default() if defaults else default),
        help="worker lease duration used for health explanations",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=(False if defaults else default),
        help="emit machine-readable JSON",
    )


def _add_output_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--json",
        action="store_true",
        default=argparse.SUPPRESS,
        help="emit machine-readable JSON",
    )


def main(
    argv: Optional[list[str]] = None,
    *,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
    clock=time.time,
    client_factory=ObserverClient,
    local_config_path: Optional[str | Path] = None,
) -> int:
    args = build_parser().parse_args(argv)
    if args.command in {"init", "serve", "pm", "demo-worker", "submit", "adapter"}:
        try:
            return _run_local_command(args, stdout=stdout, stderr=stderr)
        except httpx.HTTPError as exc:
            print(f"agent-bus: could not reach local bus: {_friendly_error(exc)}", file=stderr)
            return 2
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            print(f"agent-bus: {exc}", file=stderr)
            return 2
        except KeyboardInterrupt:
            return 130

    try:
        args.url = _resolve_observer_url(
            args.url,
            local_config_path=local_config_path,
        )
    except ValueError as exc:
        print(f"agent-bus: {exc}", file=stderr)
        return 2

    client = client_factory(args.url, token=args.token)
    try:
        if args.command == "doctor":
            value = _doctor(client, now=clock(), lease_seconds=args.lease_seconds)
            _render(value, args, stdout, _format_doctor)
        elif args.command == "workers":
            state, _ = _coordination_snapshot(client)
            value = {
                "workers": worker_views(
                    state,
                    now=clock(),
                    lease_seconds=args.lease_seconds,
                )
            }
            _render(value, args, stdout, _format_workers)
        elif args.command == "task":
            state, _ = _coordination_snapshot(client)
            value = task_view(
                state,
                args.task_id,
                now=clock(),
                lease_seconds=args.lease_seconds,
            )
            _render(value, args, stdout, _format_task)
        elif args.command == "explain":
            state, _ = _coordination_snapshot(client)
            value = {
                "task_id": args.task_id,
                **explain_task(
                    state,
                    args.task_id,
                    now=clock(),
                    lease_seconds=args.lease_seconds,
                ),
            }
            _render(value, args, stdout, _format_explanation)
        elif args.command == "workflow":
            if args.mermaid and args.json:
                print("agent-bus: --mermaid and --json cannot be combined", file=stderr)
                return 2
            state, _ = _coordination_snapshot(client)
            telemetry = (
                []
                if args.mermaid
                else client.query_all(
                    topics=sorted(TELEMETRY_TOPICS),
                    correlation_id=args.correlation_id,
                )
            )
            value = workflow_view(
                state,
                args.correlation_id,
                now=clock(),
                lease_seconds=args.lease_seconds,
                telemetry_events=telemetry,
            )
            if args.mermaid:
                print(workflow_mermaid(value), file=stdout)
            else:
                _render(value, args, stdout, _format_workflow)
        elif args.command == "tail":
            return _tail(client, args, stdout)
        return 0
    except ProjectionLookupError as exc:
        print(f"agent-bus: {exc}", file=stderr)
        return 3
    except (httpx.HTTPError, BusProtocolError, json.JSONDecodeError, ValueError) as exc:
        print(f"agent-bus: could not read {args.url}: {_friendly_error(exc)}", file=stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def _resolve_observer_url(
    explicit_url: Optional[str],
    *,
    local_config_path: Optional[str | Path] = None,
) -> str:
    if explicit_url is not None:
        if not isinstance(explicit_url, str) or not explicit_url.strip():
            raise ValueError("--url must be a non-empty string")
        return explicit_url.rstrip("/")

    from local_config import DEFAULT_LOCAL_CONFIG, LocalConfig

    path = (
        Path(local_config_path).expanduser()
        if local_config_path is not None
        else Path.cwd() / DEFAULT_LOCAL_CONFIG
    )
    environment_url = os.environ.get("AGENT_BUS_URL")
    if path.exists():
        configured_url = LocalConfig.from_file(path).bus_url.rstrip("/")
        if environment_url and environment_url.rstrip("/") != configured_url:
            raise ValueError(
                f"AGENT_BUS_URL ({environment_url}) conflicts with local config "
                f"({configured_url}); unset AGENT_BUS_URL or pass --url explicitly"
            )
        return configured_url
    if environment_url:
        return environment_url.rstrip("/")
    return DEFAULT_URL


def _run_local_command(
    args: argparse.Namespace,
    *,
    stdout: TextIO,
    stderr: TextIO,
) -> int:
    """Run onboarding and integration commands before creating an observer."""
    if args.command == "init":
        from local_config import initialize_local_config

        path = initialize_local_config(args.directory)
        value = {"ok": True, "config": str(path)}
        _render_simple(value, args, stdout, f"Created {path}\nNext: agent-bus serve --config {path}")
        return 0

    if args.command == "adapter" and args.adapter_command == "check":
        from conformance import check_executor
        from integration import IntegrationConfig, PythonAgentAdapter, load_python_target

        if args.integration_config:
            executor = IntegrationConfig.from_file(args.integration_config).build_executor()
        else:
            target = load_python_target(args.python_target)
            executor = target if callable(getattr(target, "execute", None)) else PythonAgentAdapter(target)
        report = check_executor(executor)
        value = report.to_dict()
        lines = [f"Adapter: {'PASS' if report.ok else 'FAIL'}"]
        lines.extend(
            f"  {'ok' if check.ok else 'FAIL'} · {check.name}: {check.detail}"
            for check in report.checks
        )
        _render_simple(value, args, stdout, "\n".join(lines))
        return 0 if report.ok else 4

    from local_config import LocalConfig

    local_path = (
        args.local_config
        if args.command == "adapter" and args.adapter_command == "run"
        else args.config
    )
    local = LocalConfig.from_file(local_path)
    local.apply_environment()

    if args.command == "serve":
        import uvicorn

        uvicorn.run("bus:app", host=local.host, port=local.port)
        return 0
    if args.command == "pm":
        # Import only after the local environment is selected: pm_agent reads
        # its authoritative URL and lease settings at import time.
        import pm_agent

        pm_agent.main()
        return 0
    if args.command == "demo-worker":
        from client import BusClient
        from runtime import WorkerRuntime
        from worker import DemoExecutor

        runtime = WorkerRuntime(
            BusClient(local.bus_url, actor=args.name),
            name=args.name,
            executor=DemoExecutor(args.name),
            capacity=args.capacity,
            capabilities=args.capability,
        )
        runtime.run()
        return 0
    if args.command == "submit":
        from client import BusClient

        context = json.loads(args.context)
        if not isinstance(context, dict):
            raise ValueError("--context must decode to a JSON object")
        payload = {
            "title": args.title,
            "goal": args.title,
            "context": context,
            "required_capabilities": args.capability,
            "retry_policy": {"max_retries": args.max_retries},
        }
        bus = BusClient(local.bus_url, actor="human")
        event = bus.publish(
            "task.created",
            payload,
            correlation_id=args.correlation_id,
            idempotency_key=args.idempotency_key or f"submit:{uuid.uuid4().hex}",
        )
        _render_simple(
            event,
            args,
            stdout,
            f"Submitted task {event['payload']['task_id']} · workflow {event['correlation_id']} · event #{event['id']}",
        )
        return 0
    if args.command == "adapter" and args.adapter_command == "run":
        from client import BusClient
        from integration import IntegrationConfig
        from runtime import WorkerRuntime

        integration = IntegrationConfig.from_file(args.integration_config)
        runtime = WorkerRuntime(
            BusClient(local.bus_url, actor=integration.worker_name),
            name=integration.worker_name,
            executor=integration.build_executor(),
            capacity=integration.capacity,
            capabilities=integration.capabilities,
        )
        runtime.run()
        return 0
    print("agent-bus: unsupported command", file=stderr)
    return 2


def _render_simple(value: dict, args: argparse.Namespace, stdout: TextIO, text: str) -> None:
    if args.json:
        print(json.dumps(value, indent=2, sort_keys=True), file=stdout)
    else:
        print(text, file=stdout)


def _coordination_snapshot(client: ObserverClient):
    events = client.query_all(topics=list(PROJECTION_TOPICS))
    return build_projection(events), events


def _doctor(client: ObserverClient, *, now: float, lease_seconds: float) -> dict:
    health = client.health()
    state, events = _coordination_snapshot(client)
    workers = worker_views(state, now=now, lease_seconds=lease_seconds)
    warnings = []
    if any(task.status == "open" for task in state.tasks.values()) and not any(
        worker["status"] == "healthy" for worker in workers
    ):
        warnings.append("No worker has a healthy lease; open work cannot be assigned.")
    reconciliation_codes = {
        "active_lease_expired",
        "active_worker_missing",
        "active_worker_replaced",
        "cancellation_pending",
        "deadline_reconciliation_pending",
        "decision_request_pending",
        "dependency_failure_reconciliation_pending",
        "permanent_failure_reconciliation_pending",
        "ready_for_assignment",
        "retry_exhaustion_reconciliation_pending",
    }
    pending_reconciliation = sum(
        explain_task(
            state,
            task.task_id,
            now=now,
            lease_seconds=lease_seconds,
        )["code"]
        in reconciliation_codes
        for task in state.tasks.values()
    )
    if pending_reconciliation:
        warnings.append(
            f"{pending_reconciliation} task(s) appear to await PM reconciliation; verify the PM is running."
        )
    # Reducer validity is diagnosed in one linear replay below; avoid treating
    # deliberately stale historical outcomes as corruption.
    invalid_count = _ignored_event_count(events)
    return {
        "ok": bool(health.get("ok")),
        "bus_url": client.base_url,
        "schema_version": health.get("schema_version"),
        "coordination_events": len(events),
        "tasks": len(state.tasks),
        "workers": len(state.workers),
        "healthy_workers": sum(worker["status"] == "healthy" for worker in workers),
        "stale_workers": sum(worker["status"] == "stale" for worker in workers),
        "ignored_replay_events": invalid_count,
        "pending_reconciliation": pending_reconciliation,
        "warnings": warnings,
    }


def _ignored_event_count(events: list[dict]) -> int:
    state = CoordinationProjection()
    ignored = 0
    for event in events:
        if not apply_event(state, event):
            ignored += 1
    return ignored


def _tail(client: ObserverClient, args: argparse.Namespace, stdout: TextIO) -> int:
    for event in client.subscribe(
        from_id=args.from_id,
        correlation_id=args.correlation_id,
    ):
        if args.json:
            print(json.dumps(event, sort_keys=True, separators=(",", ":")), file=stdout)
        else:
            payload = (
                event.get("payload")
                if isinstance(event.get("payload"), dict)
                else {}
            )
            identity = []
            if "task_id" in payload:
                identity.append(f"task {payload['task_id']}")
            if "assignment_id" in payload:
                identity.append(str(payload["assignment_id"]))
            suffix = f" · {' · '.join(identity)}" if identity else ""
            print(
                f"#{event.get('id')} {event.get('topic')} by {event.get('actor')}{suffix}",
                file=stdout,
                flush=True,
            )
    return 0


def _render(value: dict, args: argparse.Namespace, stdout: TextIO, formatter) -> None:
    if args.json:
        print(json.dumps(value, indent=2, sort_keys=True), file=stdout)
    else:
        print(formatter(value), file=stdout)


def _format_doctor(value: dict) -> str:
    lines = [
        f"Bus: {'healthy' if value['ok'] else 'unhealthy'} · {value['bus_url']} · schema v{value['schema_version']}",
        f"History: {value['coordination_events']} coordination events · {value['tasks']} tasks",
        f"Workers: {value['healthy_workers']} healthy · {value['stale_workers']} stale",
    ]
    if value["ignored_replay_events"]:
        lines.append(
            f"Replay: {value['ignored_replay_events']} historical event(s) were safely ignored as stale or invalid"
        )
    lines.extend(f"Warning: {warning}" for warning in value["warnings"])
    return "\n".join(lines)


def _format_workers(value: dict) -> str:
    workers = value["workers"]
    if not workers:
        return "No workers have registered."
    lines = []
    for worker in workers:
        capabilities = ", ".join(worker["capabilities"]) or "none"
        lines.append(
            f"{worker['name']} · {worker['status']} · load {worker['load']}/{worker['capacity']} · "
            f"lease {worker['lease_age_seconds']:.1f}s · capabilities: {capabilities} · event #{worker['last_event_id']}"
        )
    return "\n".join(lines)


def _format_task(value: dict) -> str:
    retry = value["retry_policy"]
    remaining = "unbounded" if retry["remaining"] is None else retry["remaining"]
    lines = [
        f"Task {value['task_id']} · {value['title']}",
        f"State: {value['status']} · event #{value['status_event_id']}",
        f"Why: {value['explanation']['summary']}",
        f"Workflow: {value['correlation_id']}",
        f"Attempts: {value['attempt']} · retryable failures {retry['retryable_failures']} · retries remaining {remaining}",
    ]
    if value["assignment_id"]:
        label = "Current owner" if value["assignment_active"] else "Last attempt"
        lines.append(
            f"{label}: {value['assignee']} ({value['worker_instance_id']}) · "
            f"{value['assignment_id']} · event #{value['assignment_event_id']}"
        )
    if value["dependencies"]:
        labels = ", ".join(
            f"{item['task_id']}={item['status']}@#{item['status_event_id']}"
            for item in value["dependencies"]
        )
        lines.append(f"Dependencies: {labels}")
    if value["deadline_at"] is not None:
        lines.append(f"Deadline: {value['deadline_at']}")
    if value["completion_summary"]:
        lines.append(f"Result: {value['completion_summary']}")
    trace = ", ".join(f"#{event_id}" for event_id in value["explanation"]["event_ids"])
    lines.append(f"Trace: {trace}")
    return "\n".join(lines)


def _format_explanation(value: dict) -> str:
    trace = ", ".join(f"#{event_id}" for event_id in value["event_ids"])
    return f"Task {value['task_id']}: {value['summary']}\nReason: {value['code']}\nTrace: {trace}"


def _format_workflow(value: dict) -> str:
    lines = [
        f"Workflow {value['correlation_id']} · {value['status']} · {value['task_count']} tasks"
    ]
    for task in value["tasks"]:
        dependencies = [item["task_id"] for item in task["dependencies"]]
        dependency_text = f" · depends on {dependencies}" if dependencies else ""
        lines.append(
            f"  Task {task['task_id']} · {task['status']}@#{task['status_event_id']} · {task['title']}{dependency_text}"
        )
        lines.append(f"    {task['explanation']['summary']}")
    lines.append(
        _format_usage(value["telemetry"])
    )
    telemetry_ids = value["telemetry"]["event_ids"]
    if telemetry_ids:
        lines.append(
            "Telemetry trace: " + ", ".join(f"#{event_id}" for event_id in telemetry_ids)
        )
    return "\n".join(lines)


def _format_usage(telemetry: dict) -> str:
    usage = telemetry["usage"]
    model = telemetry["model"]
    token_text = (
        f"{usage['total_tokens']} tokens"
        if telemetry["usage_samples"]
        else "tokens not reported"
    )
    cost_text = (
        f"${usage['cost_usd']:.6f}"
        if telemetry["cost_samples"]
        else "cost not reported"
    )
    return (
        f"Usage: {token_text} · {cost_text} · model calls "
        f"{model['completed']} completed/{model['failed']} failed/"
        f"{model['open']} open"
    )


def _friendly_error(exc: Exception) -> str:
    if isinstance(exc, httpx.ConnectError):
        return "the bus is not reachable; start the server or check --url"
    if isinstance(exc, httpx.HTTPStatusError):
        if exc.response.status_code == 401:
            return "authentication failed; set AGENT_BUS_TOKEN or pass --token"
        return f"server returned HTTP {exc.response.status_code}"
    return str(exc)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed


def _positive_number(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return parsed


def _lease_default() -> float:
    raw = os.environ.get("AGENT_BUS_WORKER_LEASE_SECONDS")
    if raw is None:
        return DEFAULT_LEASE_SECONDS
    try:
        return _positive_number(raw)
    except (argparse.ArgumentTypeError, ValueError):
        return DEFAULT_LEASE_SECONDS


if __name__ == "__main__":
    raise SystemExit(main())

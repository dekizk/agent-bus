"""Explicitly run one paid, disposable Hermes assignment through agent-bus."""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path

import bus
from artifacts import ArtifactStore
from examples.hermes.hermes_executor import HermesExecutor
from pm_agent import PMState, PM_TOPICS, apply_event, reconcile
from runtime import WorkerRuntime
from telemetry import BusTelemetrySink, ProducerIdentity


class DirectClient:
    def __init__(self, actor: str):
        self.actor = actor

    def publish(self, topic: str, payload: dict, **kwargs) -> dict:
        return bus.append_event(topic, self.actor, payload, **kwargs)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run one low-risk real Hermes lifecycle in a temporary database"
    )
    parser.add_argument("--model", required=True)
    parser.add_argument("--provider", required=True)
    parser.add_argument("--hermes-command", default="hermes")
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument(
        "--capture-content",
        action="store_true",
        help="capture this disposable prompt/output as verified local artifacts",
    )
    parser.add_argument(
        "--artifact-directory",
        type=Path,
        help=(
            "persist captured artifacts here; requires --capture-content "
            "(otherwise the smoke test uses its temporary directory)"
        ),
    )
    return parser.parse_args(argv)


def verify_artifact_capture(
    events: list[dict],
    store: ArtifactStore | None,
    *,
    required: bool,
) -> list[dict[str, object]]:
    references = [
        reference
        for event in events
        for reference in event.get("payload", {}).get("artifacts", [])
    ]
    if not required:
        if references:
            raise RuntimeError("content was captured without explicit opt-in")
        return []
    if store is None:
        raise RuntimeError("artifact capture was requested without a store")
    if len(references) != 2:
        raise RuntimeError(
            "a successful captured invocation must have input and output references"
        )
    for reference in references:
        content = store.get_bytes(reference)
        if not content:
            raise RuntimeError("captured artifact must not be empty")
    return references


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.artifact_directory is not None and not args.capture_content:
        raise SystemExit("--artifact-directory requires --capture-content")
    with tempfile.TemporaryDirectory(prefix="agent-bus-hermes-live-") as temp_dir:
        original_db_path = bus.DB_PATH
        bus.DB_PATH = Path(temp_dir) / "events.db"
        try:
            bus.init_db()
            human = DirectClient("human")
            worker_bus = DirectClient("hermes")
            human.publish(
                "task.created",
                {
                    "title": "Summarize a short integration note",
                    "context": {
                        "text": (
                            "agent-bus owns orchestration while Hermes performs "
                            "one bounded assignment as an interchangeable executor."
                        )
                    },
                    "required_capabilities": ["hermes"],
                    "retry_policy": {"max_retries": 0},
                },
                idempotency_key="hermes-live-smoke-task",
            )
            worker_bus.publish(
                "agent.registered",
                {
                    "name": "hermes",
                    "instance_id": "hermes-live-smoke",
                    "capacity": 1,
                    "capabilities": ["hermes"],
                },
                idempotency_key="registered:hermes-live-smoke",
            )

            state = PMState()
            for event in bus.fetch_after(0, list(PM_TOPICS)):
                apply_event(state, event)
            assignment = reconcile(state, DirectClient("pm"), now=time.time())[0]

            usages: list[dict[str, object]] = []
            artifact_store = None
            if args.capture_content:
                artifact_root = (
                    args.artifact_directory.expanduser().resolve()
                    if args.artifact_directory is not None
                    else Path(temp_dir) / "artifacts"
                )
                artifact_store = ArtifactStore(artifact_root)
            telemetry = BusTelemetrySink(
                worker_bus,
                producer=ProducerIdentity(
                    "examples.hermes.live_smoke",
                    "hermes-live-smoke",
                    "0.6.0",
                ),
                artifact_store=artifact_store,
                capture_content=args.capture_content,
            )
            executor = HermesExecutor(
                working_directory=temp_dir,
                model=args.model,
                provider=args.provider,
                toolsets=("clarify",),
                command=(args.hermes_command,),
                timeout=args.timeout,
                usage_callback=lambda assignment_id, usage: usages.append(dict(usage)),
                telemetry_sink=telemetry,
            )
            runtime = WorkerRuntime(
                worker_bus,
                name="hermes",
                instance_id="hermes-live-smoke",
                executor=executor,
                capacity=1,
                capabilities=["hermes"],
                heartbeat_seconds=100,
                log=lambda message: None,
            )
            runtime.run([assignment])

            replayed = PMState()
            for event in bus.fetch_after(0, list(PM_TOPICS)):
                apply_event(replayed, event)
            reconcile(replayed, DirectClient("pm"), now=time.time())
            replayed = PMState()
            for event in bus.fetch_after(0, list(PM_TOPICS)):
                apply_event(replayed, event)
            task = next(iter(replayed.tasks.values()))
            completed = bus.fetch_after(0, ["task.completed"])
            attempt_failures = bus.fetch_after(0, ["task.attempt_failed"])
            terminal_failures = bus.fetch_after(0, ["task.failed"])
            model_started = bus.fetch_after(0, ["telemetry.model.started"])
            model_terminal = bus.fetch_after(
                0,
                ["telemetry.model.completed", "telemetry.model.failed"],
            )
            telemetry_events = sorted(
                [*model_started, *model_terminal],
                key=lambda event: event["id"],
            )
            print(f"Hermes lifecycle status: {task.status}")
            if completed:
                print(f"Summary: {completed[0]['payload']['summary']}")
            if attempt_failures:
                failure = attempt_failures[-1]["payload"]
                print(
                    "Attempt failure: "
                    f"code={failure.get('failure_code')} "
                    f"retryable={failure.get('retryable')} "
                    f"reason={failure.get('reason')}"
                )
            if terminal_failures:
                failure = terminal_failures[-1]["payload"]
                print(
                    "Terminal failure: "
                    f"code={failure.get('reason_code')} "
                    f"reason={failure.get('reason')}"
                )
            if usages:
                print(
                    "Usage: "
                    f"model={usages[0].get('model')} "
                    f"tokens={usages[0].get('total_tokens')} "
                    f"cost_usd={usages[0].get('estimated_cost_usd')}"
                )
            print(
                "Telemetry: "
                f"started={len(model_started)} terminal={len(model_terminal)}"
            )
            references = verify_artifact_capture(
                telemetry_events,
                artifact_store,
                required=args.capture_content,
            )
            if references:
                print(
                    "Artifacts verified: "
                    + json.dumps(references, sort_keys=True, separators=(",", ":"))
                )
                if args.artifact_directory is not None:
                    print(f"Artifact directory: {artifact_store.root}")
            if (
                task.status != "completed"
                or len(model_started) != 1
                or len(model_terminal) != 1
            ):
                raise SystemExit(1)
        finally:
            bus.DB_PATH = original_db_path


if __name__ == "__main__":
    main()

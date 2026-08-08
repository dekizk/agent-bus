"""Explicitly run one paid, disposable Hermes assignment through agent-bus."""

from __future__ import annotations

import argparse
import tempfile
import time
from pathlib import Path

import bus
from examples.hermes.hermes_executor import HermesExecutor
from pm_agent import PMState, PM_TOPICS, apply_event, reconcile
from runtime import WorkerRuntime


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
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
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
            executor = HermesExecutor(
                working_directory=temp_dir,
                model=args.model,
                provider=args.provider,
                toolsets=("clarify",),
                command=(args.hermes_command,),
                timeout=args.timeout,
                usage_callback=lambda assignment_id, usage: usages.append(dict(usage)),
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
            if task.status != "completed":
                raise SystemExit(1)
        finally:
            bus.DB_PATH = original_db_path


if __name__ == "__main__":
    main()

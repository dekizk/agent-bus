"""Reference worker built on the reusable v0.4 executor runtime."""

import argparse
import os
import threading
import time

from client import BusClient
from executors import (
    AssignmentContext,
    Blocked,
    Completed,
    PermanentFailure,
    RetryableFailure,
)
from runtime import WorkerRuntime

BUS_URL = os.environ.get("AGENT_BUS_URL", "http://127.0.0.1:8765")
HEARTBEAT_SECONDS = float(os.environ.get("AGENT_BUS_HEARTBEAT_SECONDS", "5"))


class DemoExecutor:
    """Small reference executor used by the command-line demonstration."""

    def __init__(
        self,
        name: str,
        *,
        block_once: int | None = None,
        fail_once: int | None = None,
        fail_permanently_once: int | None = None,
    ):
        self.name = name
        self.block_once = block_once
        self.fail_once = fail_once
        self.fail_permanently_once = fail_permanently_once
        self._lock = threading.Lock()

    def execute(self, assignment: AssignmentContext):
        # Replace this body with a real agent implementation. The surrounding
        # runtime already owns registration, leases, concurrency, and events.
        time.sleep(0.5)
        with self._lock:
            if self.fail_once == assignment.task_id:
                self.fail_once = None
                return RetryableFailure(
                    "demo_retryable_failure",
                    "demo worker was asked to fail this task",
                )
            if self.fail_permanently_once == assignment.task_id:
                self.fail_permanently_once = None
                return PermanentFailure(
                    "demo_permanent_failure",
                    "demo worker was asked to fail this task",
                )
            if self.block_once == assignment.task_id:
                self.block_once = None
                return Blocked("Choose storage backend: SQLite or Postgres")
        return Completed(
            summary=f"{self.name} finished: {assignment.goal}",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a demo agent-bus worker")
    parser.add_argument("name")
    outcome = parser.add_mutually_exclusive_group()
    outcome.add_argument("--block", type=int, help="block this task once")
    outcome.add_argument(
        "--fail",
        type=int,
        help="report one retryable failure for this task",
    )
    outcome.add_argument(
        "--fail-permanently",
        type=int,
        help="report one permanent failure for this task",
    )
    parser.add_argument("--capacity", type=int, default=1)
    parser.add_argument(
        "--capability",
        action="append",
        default=[],
        dest="capabilities",
        help="worker capability; repeat for more than one",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    executor = DemoExecutor(
        args.name,
        block_once=args.block,
        fail_once=args.fail,
        fail_permanently_once=args.fail_permanently,
    )
    bus = BusClient(BUS_URL, actor=args.name)
    runtime = WorkerRuntime(
        bus,
        name=args.name,
        executor=executor,
        capacity=args.capacity,
        capabilities=args.capabilities,
        heartbeat_seconds=HEARTBEAT_SECONDS,
    )
    runtime.run()


if __name__ == "__main__":
    main()

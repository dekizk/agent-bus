"""Demo worker with leased process identity and idempotent task effects."""

import argparse
import os
import threading
import time
import uuid

import httpx

from client import BusClient

BUS_URL = os.environ.get("AGENT_BUS_URL", "http://127.0.0.1:8765")
HEARTBEAT_SECONDS = float(os.environ.get("AGENT_BUS_HEARTBEAT_SECONDS", "5"))


def heartbeat_loop(
    bus: BusClient,
    name: str,
    instance_id: str,
    stop: threading.Event,
) -> None:
    while not stop.wait(HEARTBEAT_SECONDS):
        try:
            bus.publish(
                "agent.heartbeat",
                {"name": name, "instance_id": instance_id},
            )
        except httpx.HTTPError as exc:
            print(f"[{name}] heartbeat failed ({exc.__class__.__name__})", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a demo agent-bus worker")
    parser.add_argument("name")
    parser.add_argument("--block", type=int, help="block this task once")
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
    if args.capacity < 1:
        raise SystemExit("--capacity must be at least 1")

    name = args.name
    instance_id = uuid.uuid4().hex
    bus = BusClient(BUS_URL, actor=name)
    registered = bus.publish(
        "agent.registered",
        {
            "name": name,
            "instance_id": instance_id,
            "capacity": args.capacity,
            "capabilities": args.capabilities,
        },
    )
    print(f"[{name}] registered instance {instance_id[:8]}, waiting for work", flush=True)

    stop_heartbeat = threading.Event()
    heartbeat = threading.Thread(
        target=heartbeat_loop,
        args=(bus, name, instance_id, stop_heartbeat),
        daemon=True,
        name=f"{name}-heartbeat",
    )
    heartbeat.start()

    block_once = args.block
    try:
        # Assignments for this instance can only exist after its registration
        # event, so start there instead of replaying the whole log. BusClient
        # keeps the latest id in memory across SSE reconnects; a replacement
        # process registers a new instance and must not resume this one's work.
        for event in bus.subscribe(
            topics=["task.assigned"],
            from_id=registered["id"],
        ):
            payload = event["payload"]
            if (
                payload.get("assignee") != name
                or payload.get("worker_instance_id") != instance_id
            ):
                continue

            task_id = payload["task_id"]
            assignment_id = payload["assignment_id"]
            lifecycle_payload = {
                "task_id": task_id,
                "assignment_id": assignment_id,
                "worker_instance_id": instance_id,
            }
            print(
                f"[{name}] picked up task {task_id} attempt {payload['attempt']}: "
                f"{payload.get('goal', '')}",
                flush=True,
            )
            bus.publish(
                "task.started",
                lifecycle_payload,
                caused_by=event["id"],
                idempotency_key=f"started:{assignment_id}",
            )

            # Replace this with an executor that accepts assignment_id as its
            # idempotency token. Orchestration retries are safe, but arbitrary
            # external side effects must also be made idempotent by the worker.
            time.sleep(0.5)

            if block_once == task_id:
                block_once = None
                bus.publish(
                    "task.blocked",
                    {
                        **lifecycle_payload,
                        "reason": "Choose storage backend: SQLite or Postgres",
                    },
                    caused_by=event["id"],
                    idempotency_key=f"blocked:{assignment_id}",
                )
                print(f"[{name}] BLOCKED task {task_id}", flush=True)
            else:
                bus.publish(
                    "task.completed",
                    {
                        **lifecycle_payload,
                        "summary": f"{name} finished: {payload.get('goal', '')}",
                    },
                    caused_by=event["id"],
                    idempotency_key=f"completed:{assignment_id}",
                )
                print(f"[{name}] completed task {task_id}", flush=True)
    finally:
        stop_heartbeat.set()
        heartbeat.join(timeout=1)


if __name__ == "__main__":
    main()

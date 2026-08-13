"""Crash-safe project-manager projection and reconciliation agent.

The PM does not own a mutable board. It derives orchestration state from the
event log, then reconciles missing effects using stable idempotency keys.
"""

import fcntl
import getpass
import hashlib
import json
import os
import stat
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from client import BusClient
from topics import COORDINATION_TOPICS

BUS_URL = os.environ.get("AGENT_BUS_URL", "http://127.0.0.1:8765")


def _lock_path() -> Path:
    """One lock per (user, machine, bus URL), not per checkout directory.

    Keyed by bus URL so two PMs from different working copies still exclude
    each other. This is only a local-process guard: a PM on another machine
    or running as another OS user is not excluded.
    """
    digest = hashlib.sha256(BUS_URL.encode()).hexdigest()[:16]
    base = Path(os.environ.get("XDG_RUNTIME_DIR") or tempfile.gettempdir())
    return base / f"agent-bus-pm-{getpass.getuser()}-{digest}.lock"


LOCK_PATH = _lock_path()
WORKER_LEASE_SECONDS = float(os.environ.get("AGENT_BUS_WORKER_LEASE_SECONDS", "20"))

ACTIVE_TASK_STATUSES = {"assigned", "started"}
PM_TOPICS = tuple(sorted(COORDINATION_TOPICS))


@dataclass
class WorkerRecord:
    name: str
    instance_id: str
    last_seen: float
    capacity: int = 1
    capabilities: frozenset[str] = field(default_factory=frozenset)
    last_event_id: int = 0


@dataclass
class TaskRecord:
    task_id: int
    title: str
    correlation_id: Optional[str] = None
    status: str = "open"
    attempt: int = 0
    max_retries: Optional[int] = None
    retryable_failures: int = 0
    assignee: Optional[str] = None
    worker_instance_id: Optional[str] = None
    assignment_id: Optional[str] = None
    assignment_event_id: Optional[int] = None
    open_event_id: Optional[int] = None
    block_event_id: Optional[int] = None
    block_reason: Optional[str] = None
    decision_id: Optional[str] = None
    decision_needed: bool = False
    decision_event_id: Optional[int] = None
    decisions: list[dict] = field(default_factory=list)
    last_assignment_id: Optional[str] = None
    last_failure_event_id: Optional[int] = None
    last_failure_code: Optional[str] = None
    last_failure_reason: Optional[str] = None
    permanent_failure_pending: bool = False
    failed_event_id: Optional[int] = None
    dependency_failed_event_id: Optional[int] = None
    completion_event_id: Optional[int] = None
    depends_on: tuple[int, ...] = ()
    required_capabilities: frozenset[str] = field(default_factory=frozenset)
    context: dict = field(default_factory=dict)
    external_origin: Optional[dict] = None
    ownership_mode: str = "controlled"
    ownership_owner: str = "agent-bus"


class PMState:
    def __init__(self):
        self.workers: dict[str, WorkerRecord] = {}
        self.tasks: dict[int, TaskRecord] = {}

    def active_workers(self, now: float, lease_seconds: float) -> list[WorkerRecord]:
        return [
            worker
            for worker in self.workers.values()
            if now - worker.last_seen <= lease_seconds
        ]

    def worker_load(self, worker: WorkerRecord) -> int:
        return sum(
            1
            for task in self.tasks.values()
            if task.status in ACTIVE_TASK_STATUSES
            and task.assignee == worker.name
            and task.worker_instance_id == worker.instance_id
        )

    def choose_worker(
        self,
        task: TaskRecord,
        now: float,
        lease_seconds: float,
    ) -> Optional[WorkerRecord]:
        candidates = []
        for worker in self.active_workers(now, lease_seconds):
            load = self.worker_load(worker)
            if load >= worker.capacity:
                continue
            if not task.required_capabilities.issubset(worker.capabilities):
                continue
            candidates.append((load / worker.capacity, load, worker.name, worker))
        return min(candidates, default=(None, None, None, None))[-1]


@contextmanager
def single_pm_lock():
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if not nofollow:
        raise SystemExit("[pm] this platform cannot safely open the PM lock")
    flags |= nofollow

    fd: Optional[int] = None
    try:
        fd = os.open(LOCK_PATH, flags, 0o600)
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
            raise PermissionError("PM lock must be a regular file owned by this user")
        os.fchmod(fd, 0o600)
    except OSError as exc:
        if fd is not None:
            os.close(fd)
        raise SystemExit(f"[pm] cannot safely open PM lock: {exc}") from exc

    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        raise SystemExit("[pm] another PM is already running")

    try:
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, str(os.getpid()).encode())
        os.fsync(fd)
        yield
    finally:
        os.close(fd)


def _positive_int(value: object) -> Optional[int]:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return None


def _nonnegative_int(value: object) -> Optional[int]:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


def _payload(ev: dict) -> Optional[dict]:
    value = ev.get("payload")
    return value if isinstance(value, dict) else None


def _active_assignment_matches(task: TaskRecord, ev: dict, payload: dict) -> bool:
    if task.status not in ACTIVE_TASK_STATUSES:
        return False
    assignment_id = payload.get("assignment_id")
    if assignment_id is None and ev.get("schema_version", 1) == 1:
        if ev.get("caused_by") == task.assignment_event_id:
            assignment_id = task.assignment_id
    if assignment_id != task.assignment_id or ev.get("actor") != task.assignee:
        return False
    instance_id = payload.get("worker_instance_id")
    if instance_id is not None and instance_id != task.worker_instance_id:
        return False
    return True


def _clear_active_assignment(task: TaskRecord) -> None:
    task.assignee = None
    task.worker_instance_id = None
    task.assignment_id = None
    task.assignment_event_id = None


def _retry_budget_exhausted(task: TaskRecord) -> bool:
    return (
        task.max_retries is not None
        and task.retryable_failures > task.max_retries
    )


def apply_event(state: PMState, ev: dict) -> bool:
    """Apply one event without raising on malformed or stale log entries.

    Returning False means the event was irrelevant, duplicate, malformed, or
    invalid for the current state transition. This total reducer ensures a bad
    historical event cannot permanently poison every PM restart.
    """
    try:
        if not isinstance(ev, dict):
            return False
        topic = ev.get("topic")
        payload = _payload(ev)
        event_id = _positive_int(ev.get("id"))
        timestamp = ev.get("ts")
        if not isinstance(topic, str) or payload is None or event_id is None:
            return False
        if not isinstance(timestamp, (int, float)) or isinstance(timestamp, bool):
            return False

        if topic == "agent.registered":
            name = payload.get("name")
            if not isinstance(name, str) or not name or ev.get("actor") != name:
                return False
            instance_id = payload.get("instance_id") or f"legacy:{name}"
            if not isinstance(instance_id, str) or not instance_id:
                return False
            capacity = _positive_int(payload.get("capacity", 1)) or 1
            raw_capabilities = payload.get("capabilities", [])
            capabilities = frozenset(
                item for item in raw_capabilities if isinstance(item, str) and item
            ) if isinstance(raw_capabilities, list) else frozenset()
            existing = state.workers.get(name)
            if existing and existing.last_event_id >= event_id:
                return False
            state.workers[name] = WorkerRecord(
                name=name,
                instance_id=instance_id,
                last_seen=float(timestamp),
                capacity=capacity,
                capabilities=capabilities,
                last_event_id=event_id,
            )
            return True

        if topic == "agent.heartbeat":
            name = payload.get("name")
            instance_id = payload.get("instance_id")
            worker = state.workers.get(name) if isinstance(name, str) else None
            if (
                worker is None
                or ev.get("actor") != name
                or instance_id != worker.instance_id
                or event_id <= worker.last_event_id
            ):
                return False
            worker.last_seen = max(worker.last_seen, float(timestamp))
            worker.last_event_id = event_id
            return True

        task_id = _positive_int(payload.get("task_id"))
        if task_id is None:
            return False

        if topic == "task.created":
            if task_id in state.tasks:
                return False
            raw_required = payload.get("required_capabilities", [])
            required = frozenset(
                item for item in raw_required if isinstance(item, str) and item
            ) if isinstance(raw_required, list) else frozenset()
            retry_policy = payload.get("retry_policy")
            if retry_policy is None:
                max_retries = None
            elif isinstance(retry_policy, dict):
                max_retries = _nonnegative_int(retry_policy.get("max_retries"))
                if max_retries is None:
                    return False
            else:
                return False
            context = payload.get("context", {})
            if not isinstance(context, dict):
                return False
            external_origin = payload.get("external_origin")
            if external_origin is not None and not isinstance(external_origin, dict):
                return False
            ownership = payload.get(
                "ownership",
                {"mode": "controlled", "owner": "agent-bus"},
            )
            if not isinstance(ownership, dict):
                return False
            ownership_pair = (ownership.get("mode"), ownership.get("owner"))
            if ownership_pair not in {
                ("controlled", "agent-bus"),
                ("canary", "agent-bus"),
            }:
                return False
            raw_depends_on = payload.get("depends_on", [])
            if not isinstance(raw_depends_on, list):
                return False
            depends_on: list[int] = []
            for dependency_task_id in raw_depends_on:
                dependency_task_id = _positive_int(dependency_task_id)
                if (
                    dependency_task_id is None
                    or dependency_task_id in depends_on
                    or dependency_task_id not in state.tasks
                ):
                    return False
                dependency = state.tasks[dependency_task_id]
                correlation_id = ev.get("correlation_id")
                if (
                    not isinstance(correlation_id, str)
                    or dependency.correlation_id != correlation_id
                ):
                    return False
                depends_on.append(dependency_task_id)
            state.tasks[task_id] = TaskRecord(
                task_id=task_id,
                title=(
                    payload["title"]
                    if isinstance(payload.get("title"), str) and payload["title"]
                    else f"Task {task_id}"
                ),
                correlation_id=(
                    ev.get("correlation_id")
                    if isinstance(ev.get("correlation_id"), str)
                    else None
                ),
                open_event_id=event_id,
                depends_on=tuple(depends_on),
                max_retries=max_retries,
                required_capabilities=required,
                context=dict(context),
                external_origin=(
                    dict(external_origin)
                    if external_origin is not None
                    else None
                ),
                ownership_mode=ownership["mode"],
                ownership_owner=ownership["owner"],
            )
            return True

        task = state.tasks.get(task_id)
        if task is None:
            return False

        if topic == "task.assigned":
            if ev.get("actor") != "pm":
                return False
            assignment_id = payload.get("assignment_id") or f"legacy:{event_id}"
            assignee = payload.get("assignee")
            if not isinstance(assignment_id, str) or not isinstance(assignee, str):
                return False
            if task.assignment_id == assignment_id:
                return False
            if task.status != "open" or task.assignment_id is not None:
                return False
            attempt = _positive_int(payload.get("attempt")) or task.attempt + 1
            if attempt != task.attempt + 1:
                return False
            expected_refs = [
                {
                    "task_id": dependency_task_id,
                    "completion_event_id": state.tasks[
                        dependency_task_id
                    ].completion_event_id,
                }
                for dependency_task_id in task.depends_on
            ]
            if (
                any(
                    ref["completion_event_id"] is None
                    for ref in expected_refs
                )
                or payload.get("dependency_refs", []) != expected_refs
            ):
                return False
            worker = state.workers.get(assignee)
            worker_instance_id = payload.get("worker_instance_id")
            if worker_instance_id is None:
                worker_instance_id = worker.instance_id if worker else f"legacy:{assignee}"
            if not isinstance(worker_instance_id, str) or not worker_instance_id:
                return False
            task.status = "assigned"
            task.attempt = attempt
            task.assignee = assignee
            task.worker_instance_id = worker_instance_id
            task.assignment_id = assignment_id
            task.assignment_event_id = event_id
            task.decision_needed = False
            task.decision_event_id = None
            task.block_reason = None
            return True

        if topic == "task.started":
            if not _active_assignment_matches(task, ev, payload):
                return False
            if task.status == "started":
                return False
            task.status = "started"
            return True

        if topic == "task.completed":
            if not _active_assignment_matches(task, ev, payload):
                return False
            task.status = "completed"
            task.completion_event_id = event_id
            task.decision_needed = False
            task.block_reason = None
            return True

        if topic == "task.blocked":
            if not _active_assignment_matches(task, ev, payload):
                return False
            task.status = "blocked"
            task.block_event_id = event_id
            task.block_reason = payload.get("reason")
            task.decision_id = f"decision:{task.assignment_id}"
            task.decision_needed = False
            task.decision_event_id = None
            return True

        if topic == "task.attempt_failed":
            if not _active_assignment_matches(task, ev, payload):
                return False
            retryable = payload.get("retryable")
            failure_code = payload.get("failure_code")
            reason = payload.get("reason")
            if (
                not isinstance(retryable, bool)
                or not isinstance(failure_code, str)
                or not failure_code
                or not isinstance(reason, str)
                or not reason
            ):
                return False
            task.last_assignment_id = task.assignment_id
            task.last_failure_event_id = event_id
            task.last_failure_code = failure_code
            task.last_failure_reason = reason
            task.permanent_failure_pending = not retryable
            if retryable:
                task.retryable_failures += 1
            task.status = "open"
            task.open_event_id = event_id
            _clear_active_assignment(task)
            return True

        if topic == "task.assignment_expired":
            if ev.get("actor") != "pm" or task.status not in ACTIVE_TASK_STATUSES:
                return False
            if (
                payload.get("assignment_id") != task.assignment_id
                or payload.get("assignee") != task.assignee
                or payload.get("worker_instance_id") != task.worker_instance_id
            ):
                return False
            task.last_assignment_id = task.assignment_id
            task.last_failure_event_id = event_id
            task.last_failure_code = "assignment_expired"
            task.last_failure_reason = payload.get("reason")
            task.retryable_failures += 1
            task.permanent_failure_pending = False
            task.status = "open"
            task.open_event_id = event_id
            _clear_active_assignment(task)
            return True

        if topic == "task.failed":
            reason_code = payload.get("reason_code")
            reason = payload.get("reason")
            last_assignment_id = payload.get("last_assignment_id")
            attempts = _positive_int(payload.get("attempts"))
            retryable_failures = _nonnegative_int(
                payload.get("retryable_failures")
            )
            payload_max_retries = payload.get("max_retries")
            if payload_max_retries is not None:
                payload_max_retries = _nonnegative_int(payload_max_retries)
                if payload_max_retries is None:
                    return False
            if (
                ev.get("actor") != "pm"
                or task.status != "open"
                or task.last_failure_event_id is None
                or ev.get("caused_by") != task.last_failure_event_id
                or not isinstance(reason_code, str)
                or not reason_code
                or not isinstance(reason, str)
                or not reason
                or not isinstance(last_assignment_id, str)
                or last_assignment_id != task.last_assignment_id
                or attempts != task.attempt
                or retryable_failures != task.retryable_failures
                or payload_max_retries != task.max_retries
                or not (
                    task.permanent_failure_pending
                    or _retry_budget_exhausted(task)
                )
            ):
                return False
            task.status = "failed"
            task.failed_event_id = event_id
            task.open_event_id = None
            return True

        if topic == "task.dependency_failed":
            dependency_task_id = _positive_int(payload.get("dependency_task_id"))
            dependency_event_id = _positive_int(payload.get("dependency_event_id"))
            reason = payload.get("reason")
            dependency = (
                state.tasks.get(dependency_task_id)
                if dependency_task_id is not None
                else None
            )
            if (
                ev.get("actor") != "pm"
                or task.status != "open"
                or task.assignment_id is not None
                or dependency_task_id not in task.depends_on
                or dependency is None
                or dependency.status not in {"failed", "dependency_failed"}
                or dependency_event_id
                != (
                    dependency.failed_event_id
                    if dependency.status == "failed"
                    else dependency.dependency_failed_event_id
                )
                or ev.get("caused_by") != dependency_event_id
                or not isinstance(reason, str)
                or not reason
            ):
                return False
            task.status = "dependency_failed"
            task.dependency_failed_event_id = event_id
            task.open_event_id = None
            return True

        if topic == "task.retry_requested":
            additional_retries = _positive_int(payload.get("additional_retries"))
            reason = payload.get("reason")
            if (
                task.status != "failed"
                or task.failed_event_id is None
                or ev.get("caused_by") != task.failed_event_id
                or additional_retries is None
                or not isinstance(reason, str)
                or not reason
            ):
                return False
            # Grant exactly this many new assignment opportunities. For an
            # exhausted task this is equivalent to extending max_retries; for
            # a permanent failure it deliberately does not restore the unused
            # automatic budget that the permanent classification bypassed.
            task.max_retries = task.retryable_failures + additional_retries - 1
            task.status = "open"
            task.open_event_id = event_id
            task.permanent_failure_pending = False
            task.failed_event_id = None
            return True

        if topic == "decision.needed":
            if ev.get("actor") != "pm" or task.status != "blocked":
                return False
            if (
                payload.get("assignment_id") != task.assignment_id
                or payload.get("decision_id") != task.decision_id
            ):
                return False
            if task.decision_needed:
                return False
            task.decision_needed = True
            task.decision_event_id = event_id
            return True

        if topic == "decision.made":
            if task.status != "blocked":
                return False
            assignment_id = payload.get("assignment_id")
            decision_id = payload.get("decision_id")
            schema_version = ev.get("schema_version", 1)
            if schema_version == 1:
                assignment_id = assignment_id or task.assignment_id
                decision_id = decision_id or task.decision_id
            if assignment_id != task.assignment_id or decision_id != task.decision_id:
                return False
            if schema_version != 1 and (
                not task.decision_needed
                or ev.get("caused_by") != task.decision_event_id
            ):
                return False
            if "decision" in payload:
                actor = ev.get("actor")
                if not isinstance(actor, str) or not actor.strip():
                    return False
                decision = json.loads(
                    json.dumps(
                        payload["decision"],
                        allow_nan=False,
                        separators=(",", ":"),
                    )
                )
                task.decisions.append(
                    {
                        "event_id": event_id,
                        "actor": actor,
                        "assignment_id": assignment_id,
                        "decision_id": decision_id,
                        "decision": decision,
                    }
                )
            elif schema_version != 1:
                return False
            task.status = "open"
            task.open_event_id = event_id
            _clear_active_assignment(task)
            task.decision_needed = False
            task.decision_event_id = None
            task.block_reason = None
            return True

        return False
    except (KeyError, OverflowError, TypeError, ValueError):
        return False


def plan_next_emission(
    state: PMState,
    now: float,
    lease_seconds: float = WORKER_LEASE_SECONDS,
) -> Optional[dict]:
    """Return the next deterministic effect needed to reconcile derived state."""
    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if task.status not in ACTIVE_TASK_STATUSES or task.assignment_id is None:
            continue
        worker = state.workers.get(task.assignee or "")
        if worker is None:
            reason = "worker is no longer registered"
        elif worker.instance_id != task.worker_instance_id:
            reason = "worker process was replaced"
        elif now - worker.last_seen > lease_seconds:
            reason = "worker lease expired"
        else:
            continue
        return {
            "topic": "task.assignment_expired",
            "payload": {
                "task_id": task.task_id,
                "assignment_id": task.assignment_id,
                "assignee": task.assignee,
                "worker_instance_id": task.worker_instance_id,
                "reason": reason,
            },
            "caused_by": task.assignment_event_id,
            "idempotency_key": f"expire:{task.assignment_id}",
        }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if task.status != "open" or task.assignment_id is not None:
            continue
        for dependency_task_id in task.depends_on:
            dependency = state.tasks[dependency_task_id]
            if dependency.status not in {"failed", "dependency_failed"}:
                continue
            dependency_event_id = (
                dependency.failed_event_id
                if dependency.status == "failed"
                else dependency.dependency_failed_event_id
            )
            if dependency_event_id is None:
                continue
            return {
                "topic": "task.dependency_failed",
                "payload": {
                    "task_id": task.task_id,
                    "dependency_task_id": dependency_task_id,
                    "dependency_event_id": dependency_event_id,
                    "reason": (
                        f"dependency task {dependency_task_id} ended in "
                        f"{dependency.status}"
                    ),
                },
                "caused_by": dependency_event_id,
                "idempotency_key": (
                    f"dependency-failed:task:{task.task_id}:"
                    f"dependency:{dependency_task_id}:event:{dependency_event_id}"
                ),
            }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if (
            task.status != "open"
            or task.last_failure_event_id is None
            or not (
                task.permanent_failure_pending
                or _retry_budget_exhausted(task)
            )
        ):
            continue
        if task.permanent_failure_pending:
            reason_code = task.last_failure_code or "permanent_attempt_failure"
            reason = (
                task.last_failure_reason
                or "worker reported a permanent failure"
            )
        else:
            reason_code = "retry_budget_exhausted"
            reason = (
                f"retry budget exhausted after {task.retryable_failures} "
                "retryable failures"
            )
        return {
            "topic": "task.failed",
            "payload": {
                "task_id": task.task_id,
                "reason_code": reason_code,
                "reason": reason,
                "last_assignment_id": task.last_assignment_id,
                "attempts": task.attempt,
                "retryable_failures": task.retryable_failures,
                "max_retries": task.max_retries,
            },
            "caused_by": task.last_failure_event_id,
            "idempotency_key": (
                f"failed:task:{task.task_id}:failure:{task.last_failure_event_id}"
            ),
        }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if task.status == "blocked" and not task.decision_needed:
            return {
                "topic": "decision.needed",
                "payload": {
                    "task_id": task.task_id,
                    "assignment_id": task.assignment_id,
                    "decision_id": task.decision_id,
                    "reason": task.block_reason or "worker requires human input",
                },
                "caused_by": task.block_event_id,
                "idempotency_key": f"decision-needed:{task.assignment_id}",
            }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if task.status != "open" or task.assignment_id is not None:
            continue
        if task.ownership_owner != "agent-bus":
            continue
        if any(
            state.tasks[dependency_task_id].status != "completed"
            for dependency_task_id in task.depends_on
        ):
            continue
        worker = state.choose_worker(task, now, lease_seconds)
        if worker is None:
            continue
        attempt = task.attempt + 1
        assignment_id = f"task:{task.task_id}:attempt:{attempt}"
        return {
            "topic": "task.assigned",
            "payload": {
                "task_id": task.task_id,
                "assignment_id": assignment_id,
                "attempt": attempt,
                "assignee": worker.name,
                "worker_instance_id": worker.instance_id,
                "title": task.title,
                "goal": task.title,
                "context": task.context,
                "decisions": json.loads(json.dumps(task.decisions)),
                "dependency_refs": [
                    {
                        "task_id": dependency_task_id,
                        "completion_event_id": state.tasks[
                            dependency_task_id
                        ].completion_event_id,
                    }
                    for dependency_task_id in task.depends_on
                ],
                "required_capabilities": sorted(task.required_capabilities),
                "retry_policy": {"max_retries": task.max_retries},
                "retryable_failures": task.retryable_failures,
                "ownership": {
                    "mode": task.ownership_mode,
                    "owner": task.ownership_owner,
                },
                **(
                    {"external_origin": task.external_origin}
                    if task.external_origin is not None
                    else {}
                ),
            },
            "caused_by": (
                max(
                    state.tasks[dependency_task_id].completion_event_id
                    for dependency_task_id in task.depends_on
                )
                if task.depends_on and task.attempt == 0
                else task.open_event_id
            ),
            "idempotency_key": f"assign:{assignment_id}",
        }
    return None


def reconcile(
    state: PMState,
    bus: BusClient,
    *,
    now: Optional[float] = None,
    lease_seconds: float = WORKER_LEASE_SECONDS,
    clock: Callable[[], float] = time.time,
) -> list[dict]:
    """Publish and optimistically apply effects until state is stable."""
    emitted: list[dict] = []
    for _ in range(10_000):
        current_time = now if now is not None else clock()
        planned = plan_next_emission(state, current_time, lease_seconds)
        if planned is None:
            return emitted
        sent = bus.publish(
            planned["topic"],
            planned["payload"],
            caused_by=planned.get("caused_by"),
            idempotency_key=planned["idempotency_key"],
        )
        if not apply_event(state, sent):
            raise RuntimeError(
                f"PM emitted {sent.get('topic')}#{sent.get('id')} but could not apply it"
            )
        emitted.append(sent)
        print(
            f"[pm] reconciled -> {sent['topic']}#{sent['id']} {sent['payload']}",
            flush=True,
        )
    raise RuntimeError("reconciliation did not converge")


def main():
    with single_pm_lock():
        bus = BusClient(BUS_URL, actor="pm")
        state = PMState()

        history = bus.query_all(after_id=0, topics=list(PM_TOPICS))
        head = max((event["id"] for event in history), default=0)
        print(f"[pm] replaying log up to #{head}, then reconciling...", flush=True)
        for event in history:
            apply_event(state, event)

        # This closes the prototype's crash window: state replay is followed by
        # deterministic effect reconciliation before waiting for another event.
        reconcile(state, bus)

        for event in bus.subscribe(
            from_id=head,
            topics=list(PM_TOPICS),
            on_idle=lambda: reconcile(state, bus),
        ):
            apply_event(state, event)
            reconcile(state, bus)


if __name__ == "__main__":
    sys.exit(main())

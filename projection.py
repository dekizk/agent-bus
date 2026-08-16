"""Pure, reusable coordination projection derived from immutable events.

This module deliberately performs no I/O, reads no environment configuration,
and emits no events. Both the project manager and read-only operator tools use
the same reducer so their answers cannot drift from orchestration semantics.
"""

import json
import math
from dataclasses import dataclass, field
from typing import Iterable, Optional

from topics import COORDINATION_TOPICS

ACTIVE_TASK_STATUSES = {"assigned", "started"}
DEPENDENCY_TERMINAL_STATUSES = {
    "failed",
    "dependency_failed",
    "cancelled",
    "deadline_exceeded",
}
TASK_TERMINAL_STATUSES = DEPENDENCY_TERMINAL_STATUSES | {"completed"}
PROJECTION_TOPICS = tuple(sorted(COORDINATION_TOPICS))


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
    created_event_id: Optional[int] = None
    status_event_id: Optional[int] = None
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
    terminal_failure_code: Optional[str] = None
    terminal_failure_reason: Optional[str] = None
    dependency_failed_event_id: Optional[int] = None
    dependency_failure_task_id: Optional[int] = None
    dependency_failure_event_id: Optional[int] = None
    dependency_failure_reason: Optional[str] = None
    completion_event_id: Optional[int] = None
    completion_summary: Optional[str] = None
    deadline_at: Optional[float] = None
    cancel_request_event_id: Optional[int] = None
    cancel_reason: Optional[str] = None
    cancelled_event_id: Optional[int] = None
    deadline_exceeded_event_id: Optional[int] = None
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


# Compatibility keeps the historical PMState name public while new read-only
# consumers can describe what it represents without implying PM ownership.
CoordinationProjection = PMState


def _positive_int(value: object) -> Optional[int]:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return None


def _nonnegative_int(value: object) -> Optional[int]:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


def _positive_number(value: object) -> Optional[float]:
    if (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    ):
        return float(value)
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


def retry_budget_exhausted(task: TaskRecord) -> bool:
    return (
        task.max_retries is not None
        and task.retryable_failures > task.max_retries
    )


def _deadline_reached(task: TaskRecord, timestamp: float) -> bool:
    return task.deadline_at is not None and timestamp >= task.deadline_at


def dependency_terminal_event_id(task: TaskRecord) -> Optional[int]:
    return {
        "failed": task.failed_event_id,
        "dependency_failed": task.dependency_failed_event_id,
        "cancelled": task.cancelled_event_id,
        "deadline_exceeded": task.deadline_exceeded_event_id,
    }.get(task.status)


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
            deadline_at = payload.get("deadline_at")
            if deadline_at is not None:
                deadline_at = _positive_number(deadline_at)
                if deadline_at is None:
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
                created_event_id=event_id,
                status_event_id=event_id,
                open_event_id=event_id,
                deadline_at=deadline_at,
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

        if topic == "task.cancel_requested":
            reason = payload.get("reason")
            if (
                task.status in TASK_TERMINAL_STATUSES
                or task.status == "cancellation_requested"
                or _deadline_reached(task, float(timestamp))
                or not isinstance(reason, str)
                or not reason.strip()
            ):
                return False
            if task.assignment_id is not None:
                task.last_assignment_id = task.assignment_id
            task.status = "cancellation_requested"
            task.status_event_id = event_id
            task.cancel_request_event_id = event_id
            task.cancel_reason = reason
            task.decision_needed = False
            task.decision_event_id = None
            return True

        if topic == "task.cancelled":
            attempts = _nonnegative_int(payload.get("attempts"))
            expected_last_assignment_id = (
                task.assignment_id
                if task.assignment_id is not None
                else task.last_assignment_id
            )
            if (
                ev.get("actor") != "pm"
                or task.status != "cancellation_requested"
                or task.cancel_request_event_id is None
                or ev.get("caused_by") != task.cancel_request_event_id
                or payload.get("cancel_request_event_id")
                != task.cancel_request_event_id
                or payload.get("reason") != task.cancel_reason
                or payload.get("last_assignment_id")
                != expected_last_assignment_id
                or attempts != task.attempt
            ):
                return False
            task.status = "cancelled"
            task.status_event_id = event_id
            task.cancelled_event_id = event_id
            task.open_event_id = None
            task.decision_needed = False
            task.decision_event_id = None
            _clear_active_assignment(task)
            return True

        if topic == "task.deadline_exceeded":
            deadline_at = _positive_number(payload.get("deadline_at"))
            attempts = _nonnegative_int(payload.get("attempts"))
            expected_last_assignment_id = (
                task.assignment_id
                if task.assignment_id is not None
                else task.last_assignment_id
            )
            if (
                ev.get("actor") != "pm"
                or task.status in TASK_TERMINAL_STATUSES
                or task.status == "cancellation_requested"
                or task.deadline_at is None
                or deadline_at != task.deadline_at
                or ev.get("caused_by") != task.created_event_id
                or payload.get("last_assignment_id")
                != expected_last_assignment_id
                or attempts != task.attempt
            ):
                return False
            if task.assignment_id is not None:
                task.last_assignment_id = task.assignment_id
            task.status = "deadline_exceeded"
            task.status_event_id = event_id
            task.deadline_exceeded_event_id = event_id
            task.open_event_id = None
            task.decision_needed = False
            task.decision_event_id = None
            _clear_active_assignment(task)
            return True

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
            payload_deadline_at = payload.get("deadline_at")
            if payload_deadline_at is not None:
                payload_deadline_at = _positive_number(payload_deadline_at)
            if payload_deadline_at != task.deadline_at:
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
            task.status_event_id = event_id
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
            task.status_event_id = event_id
            return True

        if topic == "task.completed":
            if (
                _deadline_reached(task, float(timestamp))
                or not _active_assignment_matches(task, ev, payload)
            ):
                return False
            task.last_assignment_id = task.assignment_id
            task.status = "completed"
            task.status_event_id = event_id
            task.completion_event_id = event_id
            summary = payload.get("summary")
            task.completion_summary = summary if isinstance(summary, str) else None
            task.decision_needed = False
            task.block_reason = None
            return True

        if topic == "task.blocked":
            if (
                _deadline_reached(task, float(timestamp))
                or not _active_assignment_matches(task, ev, payload)
            ):
                return False
            task.status = "blocked"
            task.status_event_id = event_id
            task.block_event_id = event_id
            task.block_reason = payload.get("reason")
            task.decision_id = f"decision:{task.assignment_id}"
            task.decision_needed = False
            task.decision_event_id = None
            return True

        if topic == "task.attempt_failed":
            if (
                _deadline_reached(task, float(timestamp))
                or not _active_assignment_matches(task, ev, payload)
            ):
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
            task.status_event_id = event_id
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
            task.status_event_id = event_id
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
                    or retry_budget_exhausted(task)
                )
            ):
                return False
            task.status = "failed"
            task.status_event_id = event_id
            task.failed_event_id = event_id
            task.terminal_failure_code = reason_code
            task.terminal_failure_reason = reason
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
                or dependency.status not in DEPENDENCY_TERMINAL_STATUSES
                or dependency_event_id != dependency_terminal_event_id(dependency)
                or ev.get("caused_by") != dependency_event_id
                or not isinstance(reason, str)
                or not reason
            ):
                return False
            task.status = "dependency_failed"
            task.status_event_id = event_id
            task.dependency_failed_event_id = event_id
            task.dependency_failure_task_id = dependency_task_id
            task.dependency_failure_event_id = dependency_event_id
            task.dependency_failure_reason = reason
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
            task.status_event_id = event_id
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
            if task.status != "blocked" or _deadline_reached(task, float(timestamp)):
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
            task.status_event_id = event_id
            task.open_event_id = event_id
            _clear_active_assignment(task)
            task.decision_needed = False
            task.decision_event_id = None
            task.block_reason = None
            return True

        return False
    except (KeyError, OverflowError, TypeError, ValueError):
        return False


def replay_events(events: Iterable[dict]) -> PMState:
    """Build a fresh projection from an event iterable in log order."""
    state = PMState()
    for event in events:
        apply_event(state, event)
    return state

"""Crash-safe project-manager reconciliation agent.

The PM derives state with the shared pure projection, then reconciles missing
effects using stable idempotency keys.
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
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Optional

from client import BusClient
from projection import (
    ACTIVE_TASK_STATUSES,
    DEPENDENCY_TERMINAL_STATUSES,
    PMState,
    PROJECTION_TOPICS,
    TASK_TERMINAL_STATUSES,
    TaskRecord,
    WorkerRecord,
    apply_event,
    dependency_terminal_event_id,
    retry_budget_exhausted,
)

BUS_URL = os.environ.get("AGENT_BUS_URL", "http://127.0.0.1:8765")
PM_TOPICS = PROJECTION_TOPICS


@dataclass
class OrderedProjectionCursor:
    """Consume each persisted coordination event once in event-ID order."""

    state: PMState
    last_event_id: int = 0

    def consume(self, events: Iterable[dict]) -> list[tuple[dict, bool]]:
        consumed: list[tuple[dict, bool]] = []
        for event in events:
            event_id = event.get("id")
            if (
                not isinstance(event_id, int)
                or isinstance(event_id, bool)
                or event_id <= self.last_event_id
            ):
                continue
            applied = apply_event(self.state, event)
            self.last_event_id = event_id
            consumed.append((event, applied))
        return consumed

    def catch_up(self, bus: BusClient) -> list[tuple[dict, bool]]:
        events = bus.query_all(
            after_id=self.last_event_id,
            topics=list(PM_TOPICS),
        )
        return self.consume(events)


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
_UNSET_POLICY_DEFAULT = object()


def _default_workflow_max_active_assignments() -> Optional[int]:
    raw = os.environ.get("AGENT_BUS_DEFAULT_WORKFLOW_MAX_ACTIVE_ASSIGNMENTS")
    if raw is None or raw.strip().lower() in {"", "none", "null", "unbounded"}:
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise SystemExit(
            "[pm] AGENT_BUS_DEFAULT_WORKFLOW_MAX_ACTIVE_ASSIGNMENTS must be "
            "a positive integer or 'unbounded'"
        ) from exc
    if value <= 0:
        raise SystemExit(
            "[pm] AGENT_BUS_DEFAULT_WORKFLOW_MAX_ACTIVE_ASSIGNMENTS must be "
            "a positive integer or 'unbounded'"
        )
    return value


DEFAULT_WORKFLOW_MAX_ACTIVE_ASSIGNMENTS = (
    _default_workflow_max_active_assignments()
)


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


def plan_next_emission(
    state: PMState,
    now: float,
    lease_seconds: float = WORKER_LEASE_SECONDS,
    default_workflow_max_active_assignments: object = _UNSET_POLICY_DEFAULT,
) -> Optional[dict]:
    """Return the next deterministic effect needed to reconcile derived state."""
    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if (
            task.status != "cancellation_requested"
            or task.cancel_request_event_id is None
        ):
            continue
        return {
            "topic": "task.cancelled",
            "payload": {
                "task_id": task.task_id,
                "cancel_request_event_id": task.cancel_request_event_id,
                "reason": task.cancel_reason or "cancellation requested",
                "last_assignment_id": (
                    task.assignment_id
                    if task.assignment_id is not None
                    else task.last_assignment_id
                ),
                "attempts": task.attempt,
            },
            "caused_by": task.cancel_request_event_id,
            "idempotency_key": (
                f"cancelled:task:{task.task_id}:"
                f"request:{task.cancel_request_event_id}"
            ),
        }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if (
            task.status != "supersession_requested"
            or task.supersession_request_event_id is None
            or task.superseded_by_task_id is None
        ):
            continue
        return {
            "topic": "task.superseded",
            "payload": {
                "task_id": task.task_id,
                "replacement_task_id": task.superseded_by_task_id,
                "replacement_created_event_id": task.supersession_request_event_id,
                "reason": task.supersession_reason or "task intent was replaced",
                "last_assignment_id": (
                    task.assignment_id
                    if task.assignment_id is not None
                    else task.last_assignment_id
                ),
                "attempts": task.attempt,
            },
            "caused_by": task.supersession_request_event_id,
            "idempotency_key": (
                f"superseded:task:{task.task_id}:"
                f"replacement:{task.superseded_by_task_id}"
            ),
        }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if (
            task.status in TASK_TERMINAL_STATUSES
            or task.status == "cancellation_requested"
            or task.deadline_at is None
            or now < task.deadline_at
        ):
            continue
        return {
            "topic": "task.deadline_exceeded",
            "payload": {
                "task_id": task.task_id,
                "deadline_at": task.deadline_at,
                "last_assignment_id": (
                    task.assignment_id
                    if task.assignment_id is not None
                    else task.last_assignment_id
                ),
                "attempts": task.attempt,
            },
            "caused_by": task.created_event_id,
            "idempotency_key": (
                f"deadline-exceeded:task:{task.task_id}:"
                f"created:{task.created_event_id}"
            ),
        }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if task.status != "pause_requested" or task.pause_request_event_id is None:
            continue
        return {
            "topic": "task.paused",
            "payload": {
                "task_id": task.task_id,
                "pause_request_event_id": task.pause_request_event_id,
                "reason": task.pause_reason or "pause requested",
                "last_assignment_id": (
                    task.assignment_id
                    if task.assignment_id is not None
                    else task.last_assignment_id
                ),
                "attempts": task.attempt,
                "resume_status": task.paused_from_status,
            },
            "caused_by": task.pause_request_event_id,
            "idempotency_key": (
                f"paused:task:{task.task_id}:request:{task.pause_request_event_id}"
            ),
        }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if task.status != "resume_requested" or task.resume_request_event_id is None:
            continue
        return {
            "topic": "task.resumed",
            "payload": {
                "task_id": task.task_id,
                "resume_request_event_id": task.resume_request_event_id,
                "reason": task.resume_reason or "resume requested",
                "resume_status": task.paused_from_status,
            },
            "caused_by": task.resume_request_event_id,
            "idempotency_key": (
                f"resumed:task:{task.task_id}:request:{task.resume_request_event_id}"
            ),
        }

    for correlation_id in sorted(state.workflows):
        control = state.workflows[correlation_id]
        if control.status == "pause_requested" and control.pause_request_event_id is not None:
            interrupted = [
                {"task_id": task_id, "assignment_id": assignment_id}
                for task_id, assignment_id in control.pause_assignment_snapshot
            ]
            return {
                "topic": "workflow.paused",
                "payload": {
                    "pause_request_event_id": control.pause_request_event_id,
                    "reason": control.pause_reason or "workflow pause requested",
                    "interrupted_assignments": interrupted,
                },
                "caused_by": control.pause_request_event_id,
                "correlation_id": correlation_id,
                "idempotency_key": (
                    f"paused:workflow:{correlation_id}:"
                    f"request:{control.pause_request_event_id}"
                ),
            }
        if control.status == "resume_requested" and control.resume_request_event_id is not None:
            return {
                "topic": "workflow.resumed",
                "payload": {
                    "resume_request_event_id": control.resume_request_event_id,
                    "reason": control.resume_reason or "workflow resume requested",
                },
                "caused_by": control.resume_request_event_id,
                "correlation_id": correlation_id,
                "idempotency_key": (
                    f"resumed:workflow:{correlation_id}:"
                    f"request:{control.resume_request_event_id}"
                ),
            }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if state.workflow_is_paused(task.correlation_id):
            continue
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
        if state.workflow_is_paused(task.correlation_id):
            continue
        if task.status != "open" or task.assignment_id is not None:
            continue
        for dependency_task_id in task.depends_on:
            dependency = state.tasks[dependency_task_id]
            if dependency.status not in DEPENDENCY_TERMINAL_STATUSES:
                continue
            dependency_event_id = dependency_terminal_event_id(dependency)
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
        if state.workflow_is_paused(task.correlation_id):
            continue
        if (
            task.status != "open"
            or task.last_failure_event_id is None
            or not (
                task.permanent_failure_pending
                or retry_budget_exhausted(task)
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
        if state.workflow_is_paused(task.correlation_id):
            continue
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

    if default_workflow_max_active_assignments is not _UNSET_POLICY_DEFAULT:
        workflows_needing_policy = set()
        for task in state.tasks.values():
            if (
                task.correlation_id is None
                or task.ownership_owner != "agent-bus"
                or task.status in TASK_TERMINAL_STATUSES
            ):
                continue
            control = state.workflow_control(task.correlation_id)
            if control is not None and control.policy_event_id is not None:
                continue
            workflows_needing_policy.add(task.correlation_id)
        policy_candidates = {
            correlation_id: min(
                (
                    task
                    for task in state.tasks.values()
                    if task.correlation_id == correlation_id
                ),
                key=lambda task: task.created_event_id or task.task_id,
            )
            for correlation_id in workflows_needing_policy
        }
        if policy_candidates:
            correlation_id, first_task = min(
                policy_candidates.items(),
                key=lambda item: (
                    item[1].created_event_id or item[1].task_id,
                    item[0],
                ),
            )
            limit = default_workflow_max_active_assignments
            if limit is not None and (
                not isinstance(limit, int)
                or isinstance(limit, bool)
                or limit <= 0
            ):
                raise ValueError(
                    "default workflow max active assignments must be null or positive"
                )
            return {
                "topic": "workflow.policy_set",
                "payload": {
                    "source": "default",
                    "reason": "materialized deployment default",
                    "max_active_assignments": limit,
                },
                "caused_by": first_task.created_event_id,
                "correlation_id": correlation_id,
                "idempotency_key": (
                    f"policy-default:workflow:{correlation_id}:"
                    f"task:{first_task.task_id}:created:{first_task.created_event_id}"
                ),
            }

    assignment_candidate = state.next_assignment_candidate(now, lease_seconds)
    if assignment_candidate is not None:
        task, worker = assignment_candidate
        attempt = task.last_assignment_attempt + 1
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
                "fairness": {
                    "policy": "workflow_round_robin_v1",
                    "previous_assignment_event_id": (
                        state.last_scheduling_assignment_event_id
                    ),
                },
                **(
                    {
                        "workflow_policy_event_id": state.workflow_control(
                            task.correlation_id
                        ).policy_event_id
                    }
                    if state.workflow_control(task.correlation_id) is not None
                    and state.workflow_control(task.correlation_id).policy_event_id
                    is not None
                    else {}
                ),
                **(
                    {"deadline_at": task.deadline_at}
                    if task.deadline_at is not None
                    else {}
                ),
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
    cursor: Optional[OrderedProjectionCursor] = None,
    default_workflow_max_active_assignments: object = _UNSET_POLICY_DEFAULT,
) -> list[dict]:
    """Publish effects until stable, consuming persisted order when available."""
    emitted: list[dict] = []
    for _ in range(10_000):
        current_time = now if now is not None else clock()
        planned = plan_next_emission(
            state,
            current_time,
            lease_seconds,
            default_workflow_max_active_assignments,
        )
        if planned is None:
            return emitted
        publish_options = {
            "caused_by": planned.get("caused_by"),
            "idempotency_key": planned["idempotency_key"],
        }
        if "correlation_id" in planned:
            publish_options["correlation_id"] = planned["correlation_id"]
        sent = bus.publish(
            planned["topic"],
            planned["payload"],
            **publish_options,
        )
        if cursor is None:
            if not apply_event(state, sent):
                raise RuntimeError(
                    f"PM emitted {sent.get('topic')}#{sent.get('id')} but could not apply it"
                )
            applied = True
        else:
            consumed = cursor.catch_up(bus)
            sent_id = sent.get("id")
            if not isinstance(sent_id, int) or cursor.last_event_id < sent_id:
                raise RuntimeError(
                    f"PM could not catch up through emitted event #{sent_id}"
                )
            applied = any(
                event.get("id") == sent_id and event_applied
                for event, event_applied in consumed
            )
        emitted.append(sent)
        print(
            (
                f"[pm] reconciled -> {sent['topic']}#{sent['id']} {sent['payload']}"
                if applied
                else f"[pm] stale plan -> {sent['topic']}#{sent['id']}"
            ),
            flush=True,
        )
    raise RuntimeError("reconciliation did not converge")


def main():
    with single_pm_lock():
        bus = BusClient(BUS_URL, actor="pm")
        state = PMState()
        cursor = OrderedProjectionCursor(state)

        history = bus.query_all(after_id=0, topics=list(PM_TOPICS))
        head = max((event["id"] for event in history), default=0)
        print(f"[pm] replaying log up to #{head}, then reconciling...", flush=True)
        cursor.consume(history)

        # This closes the prototype's crash window: state replay is followed by
        # deterministic effect reconciliation before waiting for another event.
        reconcile(
            state,
            bus,
            cursor=cursor,
            default_workflow_max_active_assignments=(
                DEFAULT_WORKFLOW_MAX_ACTIVE_ASSIGNMENTS
            ),
        )

        for event in bus.subscribe(
            from_id=cursor.last_event_id,
            topics=list(PM_TOPICS),
            on_idle=lambda: reconcile(
                state,
                bus,
                cursor=cursor,
                default_workflow_max_active_assignments=(
                    DEFAULT_WORKFLOW_MAX_ACTIVE_ASSIGNMENTS
                ),
            ),
        ):
            cursor.consume([event])
            reconcile(
                state,
                bus,
                cursor=cursor,
                default_workflow_max_active_assignments=(
                    DEFAULT_WORKFLOW_MAX_ACTIVE_ASSIGNMENTS
                ),
            )


if __name__ == "__main__":
    sys.exit(main())

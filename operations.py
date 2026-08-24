"""Deterministic, read-only operational views over agent-bus history."""

from __future__ import annotations

import math
from collections import Counter
from typing import Iterable, Optional

from projection import (
    ACTIVE_TASK_STATUSES,
    DEPENDENCY_TERMINAL_STATUSES,
    TASK_TERMINAL_STATUSES,
    CoordinationProjection,
    TaskRecord,
    replay_events,
)


class ProjectionLookupError(LookupError):
    """A requested task or workflow is absent from the replayed history."""


def build_projection(events: Iterable[dict]) -> CoordinationProjection:
    """Rebuild the coordination projection from scratch."""
    return replay_events(events)


def retries_remaining(task: TaskRecord) -> Optional[int]:
    """Return remaining automatic retries, or None for a legacy unbounded task."""
    if task.max_retries is None:
        return None
    return max(0, task.max_retries - task.retryable_failures)


def worker_views(
    state: CoordinationProjection,
    *,
    now: float,
    lease_seconds: float,
) -> list[dict]:
    """Describe the latest registered process for every worker name."""
    views = []
    for name in sorted(state.workers):
        worker = state.workers[name]
        lease_age = max(0.0, now - worker.last_seen)
        load = state.worker_load(worker)
        views.append(
            {
                "name": worker.name,
                "instance_id": worker.instance_id,
                "status": "healthy" if lease_age <= lease_seconds else "stale",
                "lease_age_seconds": round(lease_age, 3),
                "lease_seconds": lease_seconds,
                "capacity": worker.capacity,
                "load": load,
                "available_slots": max(0, worker.capacity - load),
                "capabilities": sorted(worker.capabilities),
                "last_event_id": worker.last_event_id,
            }
        )
    return views


def explain_task(
    state: CoordinationProjection,
    task_id: int,
    *,
    now: float,
    lease_seconds: float,
) -> dict:
    """Explain the concrete condition currently governing one task."""
    task = _get_task(state, task_id)
    evidence = [task.status_event_id]

    if task.status == "completed":
        return _explanation(
            "completed",
            f"Task completed successfully at event #{task.completion_event_id}.",
            evidence,
        )
    if task.status == "failed":
        reason = task.terminal_failure_reason or "the recorded terminal failure"
        code = task.terminal_failure_code or "task_failed"
        evidence.append(task.last_failure_event_id)
        return _explanation(
            "failed",
            f"Task is terminal after {code}: {reason}",
            evidence,
        )
    if task.status == "dependency_failed":
        evidence.append(task.dependency_failure_event_id)
        return _explanation(
            "dependency_failed",
            task.dependency_failure_reason
            or f"Dependency task {task.dependency_failure_task_id} ended terminally.",
            evidence,
            dependency_task_id=task.dependency_failure_task_id,
        )
    if task.status == "cancelled":
        evidence.append(task.cancel_request_event_id)
        return _explanation(
            "cancelled",
            f"Task was cancelled: {task.cancel_reason or 'no reason recorded'}",
            evidence,
        )
    if task.status == "deadline_exceeded":
        evidence.append(task.created_event_id)
        return _explanation(
            "deadline_exceeded",
            f"Task deadline {task.deadline_at} was reached before completion.",
            evidence,
        )
    if task.status == "cancellation_requested":
        evidence.append(task.cancel_request_event_id)
        return _explanation(
            "cancellation_pending",
            "Cancellation is recorded and is waiting for PM reconciliation.",
            evidence,
        )
    if task.status == "blocked":
        evidence.extend([task.block_event_id, task.decision_event_id])
        if task.decision_needed:
            return _explanation(
                "human_decision_required",
                task.block_reason or "The worker requires human input.",
                evidence,
                decision_id=task.decision_id,
            )
        return _explanation(
            "decision_request_pending",
            "The worker is blocked and the PM has not yet recorded decision.needed.",
            evidence,
            decision_id=task.decision_id,
        )
    if task.status in ACTIVE_TASK_STATUSES:
        evidence.append(task.assignment_event_id)
        worker = state.workers.get(task.assignee or "")
        if worker is None:
            return _explanation(
                "active_worker_missing",
                f"Attempt {task.assignment_id} is assigned, but worker {task.assignee!r} is no longer registered.",
                evidence,
            )
        evidence.append(worker.last_event_id)
        if worker.instance_id != task.worker_instance_id:
            return _explanation(
                "active_worker_replaced",
                f"Attempt {task.assignment_id} belongs to replaced process {task.worker_instance_id}; current process is {worker.instance_id}.",
                evidence,
            )
        lease_age = max(0.0, now - worker.last_seen)
        if lease_age > lease_seconds:
            return _explanation(
                "active_lease_expired",
                f"Attempt {task.assignment_id} has a stale worker lease ({lease_age:.1f}s old; limit {lease_seconds:.1f}s).",
                evidence,
                lease_age_seconds=round(lease_age, 3),
            )
        verb = "running on" if task.status == "started" else "assigned to"
        return _explanation(
            "active_lease_healthy",
            f"Attempt {task.assignment_id} is {verb} {task.assignee} with a healthy lease.",
            evidence,
            lease_age_seconds=round(lease_age, 3),
        )

    if task.status != "open":
        return _explanation(
            "unknown_state",
            f"Task is in unrecognized derived state {task.status!r}.",
            evidence,
        )

    if task.deadline_at is not None and now >= task.deadline_at:
        evidence.append(task.created_event_id)
        return _explanation(
            "deadline_reconciliation_pending",
            "The task deadline has passed and the PM has not yet recorded task.deadline_exceeded.",
            evidence,
        )
    if task.last_failure_event_id is not None and task.permanent_failure_pending:
        evidence.append(task.last_failure_event_id)
        return _explanation(
            "permanent_failure_reconciliation_pending",
            f"The last attempt failed permanently ({task.last_failure_code}); the PM has not yet recorded task.failed.",
            evidence,
        )
    if (
        task.last_failure_event_id is not None
        and task.max_retries is not None
        and task.retryable_failures > task.max_retries
    ):
        evidence.append(task.last_failure_event_id)
        return _explanation(
            "retry_exhaustion_reconciliation_pending",
            "The automatic retry budget is exhausted and the PM has not yet recorded task.failed.",
            evidence,
        )

    terminal_dependencies = []
    incomplete_dependencies = []
    for dependency_task_id in task.depends_on:
        dependency = state.tasks[dependency_task_id]
        if dependency.status in DEPENDENCY_TERMINAL_STATUSES:
            terminal_dependencies.append(dependency_task_id)
        elif dependency.status != "completed":
            incomplete_dependencies.append(dependency_task_id)
        evidence.append(dependency.status_event_id)
    if terminal_dependencies:
        return _explanation(
            "dependency_failure_reconciliation_pending",
            "A prerequisite ended terminally; the PM has not yet propagated task.dependency_failed.",
            evidence,
            dependency_task_ids=terminal_dependencies,
        )
    if incomplete_dependencies:
        labels = ", ".join(str(value) for value in incomplete_dependencies)
        return _explanation(
            "dependencies_incomplete",
            f"Waiting for prerequisite task(s) {labels} to complete.",
            evidence,
            dependency_task_ids=incomplete_dependencies,
        )

    active = state.active_workers(now, lease_seconds)
    if not active:
        evidence.extend(worker.last_event_id for worker in state.workers.values())
        return _explanation(
            "no_active_workers",
            "No worker currently has a healthy lease.",
            evidence,
        )
    capable = [
        worker
        for worker in active
        if task.required_capabilities.issubset(worker.capabilities)
    ]
    evidence.extend(worker.last_event_id for worker in active)
    if not capable:
        required = ", ".join(sorted(task.required_capabilities)) or "none"
        return _explanation(
            "capabilities_unavailable",
            f"No healthy worker provides all required capabilities: {required}.",
            evidence,
            required_capabilities=sorted(task.required_capabilities),
        )
    available = [
        worker for worker in capable if state.worker_load(worker) < worker.capacity
    ]
    if not available:
        return _explanation(
            "workers_at_capacity",
            "All healthy workers with the required capabilities are at capacity.",
            evidence,
        )
    return _explanation(
        "ready_for_assignment",
        "All dependencies and policies are satisfied; the task is waiting for PM assignment reconciliation.",
        evidence,
    )


def task_view(
    state: CoordinationProjection,
    task_id: int,
    *,
    now: float,
    lease_seconds: float,
) -> dict:
    """Build a JSON-safe operational view of one task."""
    task = _get_task(state, task_id)
    explanation = explain_task(
        state,
        task_id,
        now=now,
        lease_seconds=lease_seconds,
    )
    dependencies = []
    for dependency_task_id in task.depends_on:
        dependency = state.tasks[dependency_task_id]
        dependencies.append(
            {
                "task_id": dependency_task_id,
                "title": dependency.title,
                "status": dependency.status,
                "satisfied": dependency.status == "completed",
                "status_event_id": dependency.status_event_id,
            }
        )
    return {
        "task_id": task.task_id,
        "title": task.title,
        "correlation_id": task.correlation_id,
        "status": task.status,
        "status_event_id": task.status_event_id,
        "created_event_id": task.created_event_id,
        "attempt": task.attempt,
        "assignment_id": task.assignment_id,
        "assignment_event_id": task.assignment_event_id,
        "assignment_active": task.status in ACTIVE_TASK_STATUSES,
        "assignee": task.assignee,
        "worker_instance_id": task.worker_instance_id,
        "required_capabilities": sorted(task.required_capabilities),
        "dependencies": dependencies,
        "retry_policy": {
            "max_retries": task.max_retries,
            "retryable_failures": task.retryable_failures,
            "remaining": retries_remaining(task),
        },
        "deadline_at": task.deadline_at,
        "decision": {
            "needed": task.decision_needed,
            "decision_id": task.decision_id,
            "event_id": task.decision_event_id,
            "reason": task.block_reason,
        },
        "ownership": {
            "mode": task.ownership_mode,
            "owner": task.ownership_owner,
        },
        "completion_summary": task.completion_summary,
        "explanation": explanation,
    }


def workflow_view(
    state: CoordinationProjection,
    correlation_id: str,
    *,
    now: float,
    lease_seconds: float,
    telemetry_events: Iterable[dict] = (),
) -> dict:
    """Summarize one correlated DAG plus its separate telemetry stream."""
    tasks = [
        task
        for task in state.tasks.values()
        if task.correlation_id == correlation_id
    ]
    if not tasks:
        raise ProjectionLookupError(f"workflow {correlation_id!r} was not found")
    tasks.sort(key=lambda task: task.task_id)
    views = [
        task_view(state, task.task_id, now=now, lease_seconds=lease_seconds)
        for task in tasks
    ]
    counts = Counter(task.status for task in tasks)
    statuses = {task.status for task in tasks}
    if statuses == {"completed"}:
        status = "completed"
    elif statuses.issubset(TASK_TERMINAL_STATUSES):
        status = "ended_with_failures"
    elif "blocked" in statuses or "cancellation_requested" in statuses:
        status = "needs_attention"
    elif statuses.intersection(ACTIVE_TASK_STATUSES):
        status = "running"
    else:
        status = "waiting"
    edges = [
        {"from_task_id": dependency_id, "to_task_id": task.task_id}
        for task in tasks
        for dependency_id in task.depends_on
    ]
    relevant_telemetry = [
        event
        for event in telemetry_events
        if event.get("correlation_id") == correlation_id
    ]
    return {
        "correlation_id": correlation_id,
        "status": status,
        "task_count": len(tasks),
        "status_counts": dict(sorted(counts.items())),
        "tasks": views,
        "edges": edges,
        "telemetry": summarize_telemetry(relevant_telemetry),
        "event_ids": sorted(
            {
                event_id
                for task in tasks
                for event_id in (task.created_event_id, task.status_event_id)
                if event_id is not None
            }
        ),
    }


def workflow_mermaid(value: dict) -> str:
    """Render a derived workflow view as a read-only Mermaid flowchart."""
    tasks = value.get("tasks")
    edges = value.get("edges")
    if not isinstance(tasks, list) or not isinstance(edges, list):
        raise ValueError("workflow view must contain task and edge arrays")
    lines = ["flowchart LR"]
    known = set()
    for task in tasks:
        if not isinstance(task, dict):
            raise ValueError("workflow task view must be an object")
        task_id = task.get("task_id")
        if not isinstance(task_id, int) or isinstance(task_id, bool) or task_id <= 0:
            raise ValueError("workflow task id must be a positive integer")
        known.add(task_id)
        title = _mermaid_text(str(task.get("title", "Task")))
        status = _mermaid_text(str(task.get("status", "unknown")))
        lines.append(f'  T{task_id}["Task {task_id}: {title}<br/>{status}"]')
    for edge in edges:
        if not isinstance(edge, dict):
            raise ValueError("workflow edge must be an object")
        source = edge.get("from_task_id")
        target = edge.get("to_task_id")
        if source not in known or target not in known:
            raise ValueError("workflow edge references an unknown task")
        lines.append(f"  T{source} --> T{target}")
    return "\n".join(lines)


def _mermaid_text(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace('"', "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\n", " ")
    )


def summarize_telemetry(events: Iterable[dict]) -> dict:
    """Aggregate bounded usage once per terminal model/tool span."""
    model_started: dict[tuple[object, object], dict] = {}
    model_terminal: dict[tuple[object, object], dict] = {}
    tool_started: dict[tuple[object, object], dict] = {}
    tool_terminal: dict[tuple[object, object], dict] = {}
    for event in events:
        if not isinstance(event, dict):
            continue
        topic = event.get("topic")
        payload = event.get("payload")
        if not isinstance(topic, str) or not isinstance(payload, dict):
            continue
        if topic.startswith("telemetry.model."):
            key = (payload.get("assignment_id"), payload.get("invocation_id"))
            target = model_started if topic.endswith(".started") else model_terminal
        elif topic.startswith("telemetry.tool."):
            key = (payload.get("assignment_id"), payload.get("tool_call_id"))
            target = tool_started if topic.endswith(".started") else tool_terminal
        else:
            continue
        previous = target.get(key)
        if previous is None or _event_id(event) > _event_id(previous):
            target[key] = event

    usage_totals = {
        "input_tokens": 0.0,
        "output_tokens": 0.0,
        "total_tokens": 0.0,
        "cost_usd": 0.0,
        "duration_ms": 0.0,
    }
    usage_samples = 0
    cost_samples = 0
    by_model: dict[tuple[object, object], dict] = {}
    for event in model_terminal.values():
        payload = event["payload"]
        usage = payload.get("usage")
        usage = usage if isinstance(usage, dict) else {}
        input_tokens = _number(usage.get("input_tokens")) or 0.0
        output_tokens = _number(usage.get("output_tokens")) or 0.0
        total_tokens = _number(usage.get("total_tokens"))
        if total_tokens is None:
            total_tokens = input_tokens + output_tokens
        cost = _first_number(
            usage,
            "reported_cost_usd",
            "estimated_cost_usd",
            "cost_usd",
        )
        duration = _number(payload.get("duration_ms")) or 0.0
        usage_totals["input_tokens"] += input_tokens
        usage_totals["output_tokens"] += output_tokens
        usage_totals["total_tokens"] += total_tokens
        usage_totals["duration_ms"] += duration
        if usage:
            usage_samples += 1
        if cost is not None:
            usage_totals["cost_usd"] += cost
            cost_samples += 1
        model_key = (payload.get("provider"), payload.get("model"))
        group = by_model.setdefault(
            model_key,
            {
                "provider": payload.get("provider"),
                "model": payload.get("model"),
                "invocations": 0,
                "total_tokens": 0.0,
                "cost_usd": 0.0,
            },
        )
        group["invocations"] += 1
        group["total_tokens"] += total_tokens
        if cost is not None:
            group["cost_usd"] += cost

    terminal_model_topics = Counter(
        event.get("topic") for event in model_terminal.values()
    )
    terminal_tool_topics = Counter(
        event.get("topic") for event in tool_terminal.values()
    )
    return {
        "model": {
            "started": len(model_started),
            "completed": terminal_model_topics["telemetry.model.completed"],
            "failed": terminal_model_topics["telemetry.model.failed"],
            "open": len(set(model_started) - set(model_terminal)),
        },
        "tool": {
            "started": len(tool_started),
            "completed": terminal_tool_topics["telemetry.tool.completed"],
            "failed": terminal_tool_topics["telemetry.tool.failed"],
            "open": len(set(tool_started) - set(tool_terminal)),
        },
        "usage": {
            key: _clean_number(value) for key, value in usage_totals.items()
        },
        "usage_samples": usage_samples,
        "cost_samples": cost_samples,
        "by_model": [
            {
                **group,
                "total_tokens": _clean_number(group["total_tokens"]),
                "cost_usd": _clean_number(group["cost_usd"]),
            }
            for _, group in sorted(
                by_model.items(), key=lambda item: (str(item[0][0]), str(item[0][1]))
            )
        ],
        "event_ids": sorted(
            _event_id(event)
            for event in [
                *model_started.values(),
                *model_terminal.values(),
                *tool_started.values(),
                *tool_terminal.values(),
            ]
            if _event_id(event) > 0
        ),
    }


def _get_task(state: CoordinationProjection, task_id: int) -> TaskRecord:
    task = state.tasks.get(task_id)
    if task is None:
        raise ProjectionLookupError(f"task {task_id} was not found")
    return task


def _explanation(
    code: str,
    summary: str,
    event_ids: Iterable[Optional[int]],
    **details: object,
) -> dict:
    return {
        "code": code,
        "summary": summary,
        "event_ids": sorted({value for value in event_ids if isinstance(value, int)}),
        "details": details,
    }


def _event_id(event: dict) -> int:
    value = event.get("id")
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _number(value: object) -> Optional[float]:
    if (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    ):
        return float(value)
    return None


def _first_number(mapping: dict, *keys: str) -> Optional[float]:
    for key in keys:
        value = _number(mapping.get(key))
        if value is not None:
            return value
    return None


def _clean_number(value: float) -> int | float:
    return int(value) if value.is_integer() else round(value, 9)

"""Shared immutable scheduling policy for task publishers and projections."""

from __future__ import annotations

from typing import Optional


TASK_PRIORITY_CLASSES = ("low", "normal", "high", "urgent")
DEFAULT_TASK_PRIORITY = "normal"
_PRIORITY_RANK = {
    priority: rank for rank, priority in enumerate(TASK_PRIORITY_CLASSES)
}


def validate_task_priority(value: object) -> str:
    """Return a supported priority string or raise a stable public error."""
    if not isinstance(value, str) or value not in _PRIORITY_RANK:
        labels = ", ".join(TASK_PRIORITY_CLASSES)
        raise ValueError(f"priority must be one of: {labels}")
    return value


def task_schedule_key(
    priority: str,
    created_event_id: Optional[int],
    task_id: int,
) -> tuple[int, int, int]:
    """Sort urgent work first, then preserve immutable creation order."""
    validated = validate_task_priority(priority)
    creation_order = created_event_id if created_event_id is not None else task_id
    return (-_PRIORITY_RANK[validated], creation_order, task_id)

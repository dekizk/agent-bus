"""Canonical topic groups shared by the bus and its consumers."""

COORDINATION_TOPICS = frozenset(
    {
        "agent.registered",
        "agent.heartbeat",
        "task.created",
        "task.assigned",
        "task.started",
        "task.completed",
        "task.blocked",
        "task.attempt_failed",
        "task.assignment_expired",
        "task.failed",
        "task.retry_requested",
        "decision.needed",
        "decision.made",
    }
)

INTEGRATION_TOPICS = frozenset(
    {
        "integration.task_observed",
    }
)

KNOWN_TOPICS = COORDINATION_TOPICS | INTEGRATION_TOPICS

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
        "task.dependency_failed",
        "task.retry_requested",
        "task.cancel_requested",
        "task.cancelled",
        "task.deadline_exceeded",
        "decision.needed",
        "decision.made",
    }
)

INTEGRATION_TOPICS = frozenset(
    {
        "integration.task_observed",
    }
)

TELEMETRY_TOPICS = frozenset(
    {
        "telemetry.model.started",
        "telemetry.model.completed",
        "telemetry.model.failed",
        "telemetry.tool.started",
        "telemetry.tool.completed",
        "telemetry.tool.failed",
    }
)

KNOWN_TOPICS = COORDINATION_TOPICS | INTEGRATION_TOPICS | TELEMETRY_TOPICS

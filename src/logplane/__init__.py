"""Stable public integration SDK for logplane.

Application code should prefer imports from this package. The repository's
flat modules remain available for backward compatibility with pre-v0.9 users.
"""

from logplane.adoption import (
    AdoptionBridge,
    AdoptionMode,
    CanarySelector,
    ExecutionOwner,
    ExternalOrigin,
    OwnershipDecision,
    decide_ownership,
)
from logplane.artifacts import (
    ArtifactError,
    ArtifactIntegrityError,
    ArtifactStore,
    validate_artifact_ref,
)
from logplane.client import BusClient, BusProtocolError
from logplane.conformance import (
    ConformanceCheck,
    ConformanceReport,
    check_executor,
    probe_assignment,
)
from logplane.executor_protocol import (
    CURRENT_PROTOCOL_VERSION,
    PROTOCOL_NAME,
    SUPPORTED_PROTOCOL_VERSIONS,
    assignment_message,
    cancellation_message,
    outcome_message,
    parse_assignment_message,
    parse_outcome_message,
)
from logplane.executors import (
    AssignmentContext,
    Blocked,
    Completed,
    Executor,
    Outcome,
    PermanentFailure,
    RetryableFailure,
)
from logplane.integration import (
    CliAgentAdapter,
    CliAgentConfig,
    HttpAgentAdapter,
    HttpAgentConfig,
    IntegrationConfig,
    PythonAgentAdapter,
    load_python_target,
)
from logplane.runtime import WorkerRuntime
from logplane.scheduling import (
    DEFAULT_TASK_PRIORITY,
    TASK_PRIORITY_CLASSES,
    validate_task_priority,
)
from logplane.telemetry import BusTelemetrySink, ProducerIdentity, TelemetrySink


__all__ = [
    "AdoptionBridge",
    "AdoptionMode",
    "ArtifactError",
    "ArtifactIntegrityError",
    "ArtifactStore",
    "AssignmentContext",
    "Blocked",
    "BusClient",
    "BusProtocolError",
    "BusTelemetrySink",
    "CURRENT_PROTOCOL_VERSION",
    "CanarySelector",
    "CliAgentAdapter",
    "CliAgentConfig",
    "Completed",
    "ConformanceCheck",
    "ConformanceReport",
    "ExecutionOwner",
    "Executor",
    "ExternalOrigin",
    "HttpAgentAdapter",
    "HttpAgentConfig",
    "IntegrationConfig",
    "Outcome",
    "OwnershipDecision",
    "PROTOCOL_NAME",
    "PermanentFailure",
    "ProducerIdentity",
    "PythonAgentAdapter",
    "RetryableFailure",
    "SUPPORTED_PROTOCOL_VERSIONS",
    "DEFAULT_TASK_PRIORITY",
    "TASK_PRIORITY_CLASSES",
    "TelemetrySink",
    "WorkerRuntime",
    "assignment_message",
    "cancellation_message",
    "check_executor",
    "decide_ownership",
    "load_python_target",
    "outcome_message",
    "parse_assignment_message",
    "parse_outcome_message",
    "probe_assignment",
    "validate_task_priority",
    "validate_artifact_ref",
]

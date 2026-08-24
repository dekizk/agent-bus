"""Stable public integration SDK for agent-bus.

Application code should prefer imports from this package. The repository's
flat modules remain available for backward compatibility with pre-v0.9 users.
"""

from adoption import (
    AdoptionBridge,
    AdoptionMode,
    CanarySelector,
    ExecutionOwner,
    ExternalOrigin,
    OwnershipDecision,
    decide_ownership,
)
from artifacts import (
    ArtifactError,
    ArtifactIntegrityError,
    ArtifactStore,
    validate_artifact_ref,
)
from client import BusClient, BusProtocolError
from conformance import (
    ConformanceCheck,
    ConformanceReport,
    check_executor,
    probe_assignment,
)
from executor_protocol import (
    CURRENT_PROTOCOL_VERSION,
    PROTOCOL_NAME,
    SUPPORTED_PROTOCOL_VERSIONS,
    assignment_message,
    cancellation_message,
    outcome_message,
    parse_assignment_message,
    parse_outcome_message,
)
from executors import (
    AssignmentContext,
    Blocked,
    Completed,
    Executor,
    Outcome,
    PermanentFailure,
    RetryableFailure,
)
from integration import (
    CliAgentAdapter,
    CliAgentConfig,
    HttpAgentAdapter,
    HttpAgentConfig,
    IntegrationConfig,
    PythonAgentAdapter,
    load_python_target,
)
from runtime import WorkerRuntime
from telemetry import BusTelemetrySink, ProducerIdentity, TelemetrySink


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
    "validate_artifact_ref",
]

"""Framework-neutral, coordination-safe telemetry contracts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Protocol

from artifacts import ArtifactStore
from executors import AssignmentContext
from topics import TELEMETRY_TOPICS


@dataclass(frozen=True)
class ProducerIdentity:
    """Identity of the process implementation emitting telemetry."""

    implementation: str
    instance_id: str
    version: Optional[str] = None

    def __post_init__(self) -> None:
        for field in ("implementation", "instance_id"):
            value = getattr(self, field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"producer {field} must be a non-empty string")
        if self.version is not None and (
            not isinstance(self.version, str) or not self.version.strip()
        ):
            raise ValueError("producer version must be null or a non-empty string")

    def to_dict(self) -> dict[str, Optional[str]]:
        return {
            "implementation": self.implementation.strip(),
            "instance_id": self.instance_id.strip(),
            "version": self.version.strip() if self.version is not None else None,
        }


class TelemetrySink(Protocol):
    """Optional executor-facing observer; implementations must not own work."""

    def model_started(self, assignment: AssignmentContext, **fields: Any) -> dict: ...

    def model_completed(self, assignment: AssignmentContext, **fields: Any) -> dict: ...

    def model_failed(self, assignment: AssignmentContext, **fields: Any) -> dict: ...

    def tool_started(self, assignment: AssignmentContext, **fields: Any) -> dict: ...

    def tool_completed(self, assignment: AssignmentContext, **fields: Any) -> dict: ...

    def tool_failed(self, assignment: AssignmentContext, **fields: Any) -> dict: ...


class BusTelemetrySink:
    """Publish compact telemetry and optionally externalize captured content."""

    def __init__(
        self,
        bus,
        *,
        producer: ProducerIdentity,
        artifact_store: Optional[ArtifactStore] = None,
        capture_content: bool = False,
    ):
        if capture_content and artifact_store is None:
            raise ValueError("capture_content requires an artifact_store")
        self.bus = bus
        self.producer = producer
        self.artifact_store = artifact_store
        self.capture_content = bool(capture_content)

    def model_started(
        self,
        assignment: AssignmentContext,
        *,
        invocation_id: str,
        provider: str,
        model: str,
        attributes: Optional[Mapping[str, Any]] = None,
        input_content: object = None,
    ) -> dict:
        artifacts = self._capture(input_content, kind="model_input")
        return self._publish(
            "telemetry.model.started",
            assignment,
            {
                "invocation_id": invocation_id,
                "provider": provider,
                "model": model,
                "attributes": self._object(attributes),
                "artifacts": artifacts,
            },
            caused_by=assignment.assignment_event_id,
            idempotency_key=f"telemetry:model:{invocation_id}:started",
        )

    def model_completed(
        self,
        assignment: AssignmentContext,
        *,
        invocation_id: str,
        provider: str,
        model: str,
        duration_ms: float,
        usage: Optional[Mapping[str, Any]] = None,
        attributes: Optional[Mapping[str, Any]] = None,
        output_content: object = None,
        caused_by: Optional[int] = None,
    ) -> dict:
        return self._publish(
            "telemetry.model.completed",
            assignment,
            {
                "invocation_id": invocation_id,
                "provider": provider,
                "model": model,
                "duration_ms": duration_ms,
                "usage": self._object(usage),
                "attributes": self._object(attributes),
                "artifacts": self._capture(output_content, kind="model_output"),
            },
            caused_by=caused_by or assignment.assignment_event_id,
            idempotency_key=f"telemetry:model:{invocation_id}:completed",
        )

    def model_failed(
        self,
        assignment: AssignmentContext,
        *,
        invocation_id: str,
        provider: str,
        model: str,
        duration_ms: float,
        error_code: str,
        retryable: bool,
        usage: Optional[Mapping[str, Any]] = None,
        attributes: Optional[Mapping[str, Any]] = None,
        output_content: object = None,
        caused_by: Optional[int] = None,
    ) -> dict:
        return self._publish(
            "telemetry.model.failed",
            assignment,
            {
                "invocation_id": invocation_id,
                "provider": provider,
                "model": model,
                "duration_ms": duration_ms,
                "error_code": error_code,
                "retryable": retryable,
                "usage": self._object(usage),
                "attributes": self._object(attributes),
                "artifacts": self._capture(output_content, kind="model_output"),
            },
            caused_by=caused_by or assignment.assignment_event_id,
            idempotency_key=f"telemetry:model:{invocation_id}:failed",
        )

    def tool_started(
        self,
        assignment: AssignmentContext,
        *,
        tool_call_id: str,
        tool_name: str,
        invocation_id: Optional[str] = None,
        attributes: Optional[Mapping[str, Any]] = None,
        input_content: object = None,
        caused_by: Optional[int] = None,
    ) -> dict:
        fields = {
            "tool_call_id": tool_call_id,
            "tool_name": tool_name,
            "attributes": self._object(attributes),
            "artifacts": self._capture(input_content, kind="tool_input"),
        }
        if invocation_id is not None:
            fields["invocation_id"] = invocation_id
        return self._publish(
            "telemetry.tool.started",
            assignment,
            fields,
            caused_by=caused_by or assignment.assignment_event_id,
            idempotency_key=f"telemetry:tool:{tool_call_id}:started",
        )

    def tool_completed(
        self,
        assignment: AssignmentContext,
        *,
        tool_call_id: str,
        tool_name: str,
        duration_ms: float,
        invocation_id: Optional[str] = None,
        attributes: Optional[Mapping[str, Any]] = None,
        output_content: object = None,
        caused_by: Optional[int] = None,
    ) -> dict:
        fields = {
            "tool_call_id": tool_call_id,
            "tool_name": tool_name,
            "duration_ms": duration_ms,
            "attributes": self._object(attributes),
            "artifacts": self._capture(output_content, kind="tool_output"),
        }
        if invocation_id is not None:
            fields["invocation_id"] = invocation_id
        return self._publish(
            "telemetry.tool.completed",
            assignment,
            fields,
            caused_by=caused_by or assignment.assignment_event_id,
            idempotency_key=f"telemetry:tool:{tool_call_id}:completed",
        )

    def tool_failed(
        self,
        assignment: AssignmentContext,
        *,
        tool_call_id: str,
        tool_name: str,
        duration_ms: float,
        error_code: str,
        retryable: bool,
        invocation_id: Optional[str] = None,
        attributes: Optional[Mapping[str, Any]] = None,
        output_content: object = None,
        caused_by: Optional[int] = None,
    ) -> dict:
        fields = {
            "tool_call_id": tool_call_id,
            "tool_name": tool_name,
            "duration_ms": duration_ms,
            "error_code": error_code,
            "retryable": retryable,
            "attributes": self._object(attributes),
            "artifacts": self._capture(output_content, kind="tool_output"),
        }
        if invocation_id is not None:
            fields["invocation_id"] = invocation_id
        return self._publish(
            "telemetry.tool.failed",
            assignment,
            fields,
            caused_by=caused_by or assignment.assignment_event_id,
            idempotency_key=f"telemetry:tool:{tool_call_id}:failed",
        )

    def _publish(
        self,
        topic: str,
        assignment: AssignmentContext,
        fields: dict[str, Any],
        *,
        caused_by: int,
        idempotency_key: str,
    ) -> dict:
        if topic not in TELEMETRY_TOPICS:
            raise ValueError(f"unsupported telemetry topic: {topic}")
        return self.bus.publish(
            topic,
            {
                "task_id": assignment.task_id,
                "assignment_id": assignment.assignment_id,
                "worker_instance_id": assignment.worker_instance_id,
                **fields,
            },
            caused_by=caused_by,
            idempotency_key=idempotency_key,
            correlation_id=assignment.correlation_id,
            producer=self.producer.to_dict(),
        )

    def _capture(self, content: object, *, kind: str) -> list[dict[str, object]]:
        if not self.capture_content or content is None or self.artifact_store is None:
            return []
        if isinstance(content, bytes):
            reference = self.artifact_store.put_bytes(
                content,
                media_type="application/octet-stream",
                kind=kind,
            )
        elif isinstance(content, str):
            reference = self.artifact_store.put_text(content, kind=kind)
        else:
            reference = self.artifact_store.put_json(content, kind=kind)
        return [reference]

    @staticmethod
    def _object(value: Optional[Mapping[str, Any]]) -> dict[str, Any]:
        if value is None:
            return {}
        if not isinstance(value, Mapping):
            raise TypeError("telemetry metadata must be a JSON object")
        try:
            return json.loads(
                json.dumps(dict(value), allow_nan=False, separators=(",", ":"))
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("telemetry metadata must be JSON-compatible") from exc

"""Versioned wire contract for subprocess and HTTP agent adapters."""

from __future__ import annotations

from typing import Mapping


PROTOCOL_NAME = "agent-bus.executor"
LEGACY_PROTOCOL_VERSION = 0
CURRENT_PROTOCOL_VERSION = 1
SUPPORTED_PROTOCOL_VERSIONS = (LEGACY_PROTOCOL_VERSION, CURRENT_PROTOCOL_VERSION)
VERSIONED_PROTOCOL_VERSIONS = (CURRENT_PROTOCOL_VERSION,)


def validate_protocol_version(value: object, *, allow_legacy: bool = True) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError("protocol_version must be an integer")
    supported = SUPPORTED_PROTOCOL_VERSIONS if allow_legacy else (CURRENT_PROTOCOL_VERSION,)
    if value not in supported:
        labels = ", ".join(str(item) for item in supported)
        raise ValueError(f"unsupported protocol_version {value}; supported: {labels}")
    return value


def assignment_message(assignment: Mapping[str, object], protocol_version: int) -> dict:
    """Build the JSON request for one selected protocol version."""
    version = validate_protocol_version(protocol_version)
    if version == LEGACY_PROTOCOL_VERSION:
        return dict(assignment)
    return {
        "protocol": {
            "name": PROTOCOL_NAME,
            "version": version,
            "supported_versions": list(VERSIONED_PROTOCOL_VERSIONS),
        },
        "assignment": dict(assignment),
    }


def outcome_message(outcome: Mapping[str, object], protocol_version: int) -> dict:
    """Build a response envelope for adapter implementations and examples."""
    version = validate_protocol_version(protocol_version)
    if version == LEGACY_PROTOCOL_VERSION:
        return dict(outcome)
    return {
        "protocol": {"name": PROTOCOL_NAME, "version": version},
        "outcome": dict(outcome),
    }


def parse_assignment_message(value: object, protocol_version: int) -> dict:
    """Validate and unwrap one adapter request."""
    version = validate_protocol_version(protocol_version)
    if not isinstance(value, dict):
        raise ValueError("adapter request must be a JSON object")
    if version == LEGACY_PROTOCOL_VERSION:
        return value
    if set(value) != {"protocol", "assignment"}:
        raise ValueError("versioned adapter request has an invalid shape")
    _validate_protocol_header(value["protocol"], version, request=True)
    assignment = value["assignment"]
    if not isinstance(assignment, dict):
        raise ValueError("adapter assignment must be a JSON object")
    return assignment


def parse_outcome_message(value: object, protocol_version: int) -> dict:
    """Validate and unwrap one adapter response."""
    version = validate_protocol_version(protocol_version)
    if not isinstance(value, dict):
        raise ValueError("adapter response must be a JSON object")
    if version == LEGACY_PROTOCOL_VERSION:
        return value
    if set(value) != {"protocol", "outcome"}:
        raise ValueError("versioned adapter response has an invalid shape")
    _validate_protocol_header(value["protocol"], version, request=False)
    outcome = value["outcome"]
    if not isinstance(outcome, dict):
        raise ValueError("adapter outcome must be a JSON object")
    return outcome


def cancellation_message(assignment_id: str) -> dict:
    if not isinstance(assignment_id, str) or not assignment_id.strip():
        raise ValueError("assignment_id must be a non-empty string")
    return {
        "protocol": {"name": PROTOCOL_NAME, "version": CURRENT_PROTOCOL_VERSION},
        "cancellation": {"assignment_id": assignment_id},
    }


def _validate_protocol_header(value: object, version: int, *, request: bool) -> None:
    if not isinstance(value, dict):
        raise ValueError("protocol header must be a JSON object")
    expected = {"name", "version", "supported_versions"} if request else {"name", "version"}
    if set(value) != expected:
        raise ValueError("protocol header has an invalid shape")
    if value.get("name") != PROTOCOL_NAME:
        raise ValueError("adapter protocol name does not match agent-bus")
    if value.get("version") != version:
        raise ValueError(
            f"adapter selected protocol version {value.get('version')!r}; expected {version}"
        )
    if request:
        supported = value.get("supported_versions")
        if (
            not isinstance(supported, list)
            or not supported
            or any(
                not isinstance(item, int)
                or isinstance(item, bool)
                or item <= 0
                for item in supported
            )
            or len(set(supported)) != len(supported)
            or version not in supported
        ):
            raise ValueError("adapter supported_versions negotiation is invalid")

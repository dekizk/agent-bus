"""Safe rollout helpers for controlled, shadow, and canary adoption."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Mapping, Optional

from client import BusClient


class AdoptionMode(str, Enum):
    CONTROLLED = "controlled"
    SHADOW = "shadow"
    CANARY = "canary"


class ExecutionOwner(str, Enum):
    AGENT_BUS = "agent-bus"
    EXTERNAL = "external"


@dataclass(frozen=True)
class ExternalOrigin:
    system: str
    task_ref: str

    def __post_init__(self) -> None:
        for field_name, value in (
            ("system", self.system),
            ("task_ref", self.task_ref),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
            if len(value) > 128:
                raise ValueError(f"{field_name} must not exceed 128 characters")

    @property
    def key(self) -> str:
        return json.dumps(
            [self.system, self.task_ref],
            ensure_ascii=False,
            separators=(",", ":"),
        )

    def to_dict(self) -> dict:
        return {"system": self.system, "task_ref": self.task_ref}


@dataclass(frozen=True)
class OwnershipDecision:
    mode: AdoptionMode
    owner: ExecutionOwner

    def to_dict(self) -> dict:
        return {"mode": self.mode.value, "owner": self.owner.value}


class CanarySelector:
    """Deterministically select external tasks using a stable hash."""

    def __init__(
        self,
        percentage: float,
        *,
        namespace: str = "default",
        include_refs: Iterable[str] = (),
    ):
        if (
            not isinstance(percentage, (int, float))
            or isinstance(percentage, bool)
            or not 0 <= percentage <= 100
        ):
            raise ValueError("percentage must be between 0 and 100")
        if not isinstance(namespace, str) or not namespace.strip():
            raise ValueError("namespace must be a non-empty string")
        self.basis_points = round(float(percentage) * 100)
        self.namespace = namespace
        if isinstance(include_refs, (str, bytes)):
            raise ValueError("include_refs must be an iterable of task references")
        self.include_refs = frozenset(include_refs)
        if not all(isinstance(item, str) and item for item in self.include_refs):
            raise ValueError("include_refs must contain non-empty strings")

    def selects(self, origin: ExternalOrigin) -> bool:
        if origin.task_ref in self.include_refs:
            return True
        digest = hashlib.sha256(
            json.dumps(
                [self.namespace, origin.system, origin.task_ref],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        ).digest()
        bucket = int.from_bytes(digest[:8], "big") % 10_000
        return bucket < self.basis_points


def decide_ownership(
    mode: AdoptionMode,
    origin: ExternalOrigin,
    selector: Optional[CanarySelector] = None,
) -> OwnershipDecision:
    if not isinstance(mode, AdoptionMode):
        mode = AdoptionMode(mode)
    if mode is AdoptionMode.CONTROLLED:
        return OwnershipDecision(mode, ExecutionOwner.AGENT_BUS)
    if mode is AdoptionMode.SHADOW:
        return OwnershipDecision(mode, ExecutionOwner.EXTERNAL)
    if selector is None:
        raise ValueError("canary mode requires a CanarySelector")
    owner = (
        ExecutionOwner.AGENT_BUS
        if selector.selects(origin)
        else ExecutionOwner.EXTERNAL
    )
    return OwnershipDecision(mode, owner)


class AdoptionBridge:
    """Record one immutable ownership decision for external work.

    The same actor/idempotency key is used whether the decision is bus-owned
    or external. Re-evaluating an origin to a different owner therefore causes
    an idempotency conflict instead of silently creating dual ownership.
    """

    def __init__(self, bus: BusClient):
        self.bus = bus

    def adopt(
        self,
        *,
        origin: ExternalOrigin,
        title: str,
        mode: AdoptionMode,
        selector: Optional[CanarySelector] = None,
        context: Optional[Mapping[str, Any]] = None,
        required_capabilities: Iterable[str] = (),
        max_retries: Optional[int] = None,
        correlation_id: Optional[str] = None,
    ) -> dict:
        if not isinstance(title, str) or not title.strip():
            raise ValueError("title must be a non-empty string")
        normalized_context = self._json_object(
            context if context is not None else {},
            "context",
        )
        capabilities = tuple(dict.fromkeys(required_capabilities))
        if not all(isinstance(item, str) and item.strip() for item in capabilities):
            raise ValueError("required_capabilities must contain non-empty strings")
        if max_retries is not None and (
            not isinstance(max_retries, int)
            or isinstance(max_retries, bool)
            or max_retries < 0
        ):
            raise ValueError("max_retries must be a non-negative integer")

        decision = decide_ownership(mode, origin, selector)
        payload = {
            "title": title,
            "context": normalized_context,
            "required_capabilities": list(capabilities),
            "external_origin": origin.to_dict(),
            "ownership": decision.to_dict(),
        }
        if max_retries is not None:
            payload["retry_policy"] = {"max_retries": max_retries}

        digest = hashlib.sha256(origin.key.encode("utf-8")).hexdigest()
        idempotency_key = f"adopt:{digest}"
        if decision.owner is ExecutionOwner.AGENT_BUS:
            topic = "task.created"
            effective_correlation = correlation_id
        else:
            topic = "integration.task_observed"
            effective_correlation = correlation_id or f"external:{digest[:24]}"
        return self.bus.publish(
            topic,
            payload,
            idempotency_key=idempotency_key,
            correlation_id=effective_correlation,
        )

    @staticmethod
    def _json_object(value: object, field_name: str) -> dict:
        if not isinstance(value, Mapping):
            raise ValueError(f"{field_name} must be a JSON object")
        try:
            return json.loads(json.dumps(dict(value), allow_nan=False))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"{field_name} must contain JSON-compatible values"
            ) from exc

"""Small loopback HTTP agent used to demonstrate the public adapter contract.

The in-memory delivery and effect records make retry and cancellation behavior
easy to inspect. They are deliberately a teaching aid, not durable production
idempotency storage.
"""

from __future__ import annotations

import threading
from collections import Counter
from typing import Mapping

from fastapi import FastAPI, Header, HTTPException

from executor_protocol import (
    CURRENT_PROTOCOL_VERSION,
    PROTOCOL_NAME,
    outcome_message,
    parse_assignment_message,
)


app = FastAPI(title="agent-bus HTTP agent example")


class _ExampleState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._deliveries: list[dict] = []
        self._effect_executions: Counter[str] = Counter()
        self._cancel_events: dict[str, threading.Event] = {}
        self._cancellations: list[str] = []

    def reset(self) -> None:
        with self._lock:
            events = tuple(self._cancel_events.values())
            self._deliveries.clear()
            self._effect_executions.clear()
            self._cancel_events.clear()
            self._cancellations.clear()
        for event in events:
            event.set()

    def record_delivery(
        self,
        assignment: Mapping[str, object],
        *,
        mode: str,
        effect_key: str,
    ) -> tuple[dict, threading.Event]:
        assignment_id = str(assignment["assignment_id"])
        effect_scope = str(assignment["effect_scope"])
        with self._lock:
            delivery_number = 1 + sum(
                item["effect_scope"] == effect_scope for item in self._deliveries
            )
            observation = {
                "assignment_id": assignment_id,
                "effect_scope": effect_scope,
                "effect_key": effect_key,
                "attempt": assignment.get("attempt"),
                "task_id": assignment.get("task_id"),
                "mode": mode,
                "delivery_number": delivery_number,
            }
            self._deliveries.append(observation)
            cancel_event = self._cancel_events.setdefault(
                assignment_id,
                threading.Event(),
            )
            return dict(observation), cancel_event

    def apply_effect_once(self, effect_key: str) -> int:
        with self._lock:
            if not self._effect_executions[effect_key]:
                self._effect_executions[effect_key] = 1
            return self._effect_executions[effect_key]

    def cancel(self, assignment_id: str) -> None:
        with self._lock:
            event = self._cancel_events.setdefault(assignment_id, threading.Event())
            if assignment_id not in self._cancellations:
                self._cancellations.append(assignment_id)
        event.set()

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "deliveries": [dict(item) for item in self._deliveries],
                "effect_executions": dict(self._effect_executions),
                "cancellations": list(self._cancellations),
            }


_state = _ExampleState()


def reset_state() -> None:
    """Reset demonstration observations between tests or local trials."""
    _state.reset()


def snapshot_state() -> dict:
    """Return a stable copy of the current demonstration observations."""
    return _state.snapshot()


def _completed(summary: str, result: Mapping[str, object]) -> dict:
    return outcome_message(
        {"status": "completed", "summary": summary, "result": dict(result)},
        CURRENT_PROTOCOL_VERSION,
    )


def _assignment_from_request(
    body: object,
    *,
    idempotency_key: str | None,
    assignment_header: str | None,
    effect_scope_header: str | None,
    protocol_header: str | None,
) -> dict:
    try:
        assignment = parse_assignment_message(body, CURRENT_PROTOCOL_VERSION)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    assignment_id = assignment.get("assignment_id")
    effect_scope = assignment.get("effect_scope")
    if not isinstance(assignment_id, str) or not assignment_id:
        raise HTTPException(status_code=400, detail="assignment_id is required")
    if not isinstance(effect_scope, str) or not effect_scope:
        raise HTTPException(status_code=400, detail="effect_scope is required")
    if idempotency_key != assignment_id or assignment_header != assignment_id:
        raise HTTPException(status_code=400, detail="assignment identity headers disagree")
    if effect_scope_header != effect_scope:
        raise HTTPException(status_code=400, detail="effect-scope header disagrees")
    if protocol_header != str(CURRENT_PROTOCOL_VERSION):
        raise HTTPException(status_code=400, detail="protocol-version header disagrees")
    return assignment


@app.get("/health")
def health() -> dict:
    return {"ok": True, "protocol_version": CURRENT_PROTOCOL_VERSION}


@app.get("/observations")
def observations() -> dict:
    return snapshot_state()


@app.post("/execute")
def execute(
    body: dict,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    assignment_header: str | None = Header(
        default=None,
        alias="X-Agent-Bus-Assignment-Id",
    ),
    effect_scope_header: str | None = Header(
        default=None,
        alias="X-Agent-Bus-Effect-Scope",
    ),
    protocol_header: str | None = Header(
        default=None,
        alias="X-Agent-Bus-Protocol-Version",
    ),
) -> dict:
    assignment = _assignment_from_request(
        body,
        idempotency_key=idempotency_key,
        assignment_header=assignment_header,
        effect_scope_header=effect_scope_header,
        protocol_header=protocol_header,
    )
    context = assignment.get("context")
    if not isinstance(context, dict):
        context = {}

    # Conformance must prove the transport contract without performing effects.
    if context.get("agent_bus_conformance_probe"):
        return _completed("HTTP conformance probe passed", {"probe": True})

    mode = context.get("trial_mode", "normal")
    if mode not in {"normal", "retry_once", "cancel"}:
        raise HTTPException(status_code=400, detail="unknown trial_mode")
    assignment_id = str(assignment["assignment_id"])
    effect_key = f"{assignment['effect_scope']}:apply-trial-effect"
    delivery, cancel_event = _state.record_delivery(
        assignment,
        mode=mode,
        effect_key=effect_key,
    )

    if mode == "cancel":
        if not cancel_event.wait(timeout=60):
            raise HTTPException(status_code=503, detail="cancellation was not received")
        return _completed(
            "HTTP agent observed cancellation",
            {"assignment_id": assignment_id, "cancelled": True},
        )

    executions = _state.apply_effect_once(effect_key)
    if mode == "retry_once" and delivery["delivery_number"] == 1:
        raise HTTPException(status_code=503, detail="controlled retry trial")

    summary = (
        "HTTP retry trial completed without duplicating the logical effect"
        if mode == "retry_once"
        else "HTTP agent completed the assignment"
    )
    return _completed(
        summary,
        {
            "assignment_id": assignment_id,
            "delivery_number": delivery["delivery_number"],
            "effect_scope": assignment["effect_scope"],
            "logical_effect_executions": executions,
        },
    )


@app.post("/cancel")
def cancel(
    body: dict,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    assignment_header: str | None = Header(
        default=None,
        alias="X-Agent-Bus-Assignment-Id",
    ),
) -> dict:
    expected_keys = {"protocol", "cancellation"}
    if set(body) != expected_keys:
        raise HTTPException(status_code=400, detail="cancellation envelope is invalid")
    protocol = body.get("protocol")
    cancellation = body.get("cancellation")
    if protocol != {"name": PROTOCOL_NAME, "version": CURRENT_PROTOCOL_VERSION}:
        raise HTTPException(status_code=400, detail="cancellation protocol is invalid")
    if not isinstance(cancellation, dict) or set(cancellation) != {"assignment_id"}:
        raise HTTPException(status_code=400, detail="cancellation payload is invalid")
    assignment_id = cancellation.get("assignment_id")
    if not isinstance(assignment_id, str) or not assignment_id:
        raise HTTPException(status_code=400, detail="assignment_id is required")
    if assignment_header != assignment_id or idempotency_key != f"cancel:{assignment_id}":
        raise HTTPException(status_code=400, detail="cancellation identity headers disagree")
    _state.cancel(assignment_id)
    return {"accepted": True, "assignment_id": assignment_id}

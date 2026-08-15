"""
agent-bus: an append-only event bus for local multi-agent orchestration.

Events are immutable SQLite rows. Consumers rebuild state by replaying the log,
then follow the same ordered stream over SSE. Versioned contracts protect known
orchestration events while unknown topics remain available to extensions.

Run: uvicorn bus:app --port 8765
"""

import asyncio
import contextlib
import json
import math
import os
import secrets
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from limits import (
    MAX_ARTIFACT_BYTES,
    MAX_TELEMETRY_ARTIFACT_REFS,
    MAX_TELEMETRY_OBJECT_BYTES,
    MAX_EXTERNAL_ID_LENGTH,
    MAX_INLINE_CONTEXT_BYTES,
    MAX_INLINE_RESULT_BYTES,
    MAX_TASK_DEPENDENCIES,
)
from topics import KNOWN_TOPICS, TELEMETRY_TOPICS

DB_PATH = Path(__file__).parent / "events.db"
CURRENT_SCHEMA_VERSION = 2
MAX_CORRELATION_ID_LENGTH = 128
MAX_PRODUCER_FIELD_LENGTH = 128


def _read_default_max_retries() -> int:
    raw_value = os.environ.get("AGENT_BUS_MAX_RETRIES", "2")
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise RuntimeError(
            "AGENT_BUS_MAX_RETRIES must be a non-negative integer"
        ) from exc
    if value < 0:
        raise RuntimeError("AGENT_BUS_MAX_RETRIES must be a non-negative integer")
    return value


DEFAULT_MAX_RETRIES = _read_default_max_retries()

# Optional perimeter auth: when AGENT_BUS_TOKEN is set, every data route
# requires "Authorization: Bearer <token>". Actor strings are still
# self-reported — this authenticates clients, not identities.
API_TOKEN = os.environ.get("AGENT_BUS_TOKEN")


async def require_token(request: Request) -> None:
    if not API_TOKEN:
        return
    supplied = request.headers.get("authorization", "")
    if not secrets.compare_digest(supplied, f"Bearer {API_TOKEN}"):
        raise HTTPException(status_code=401, detail="invalid or missing bearer token")

class EventValidationError(ValueError):
    """A known event does not satisfy its versioned contract."""


class IdempotencyConflict(ValueError):
    """An idempotency key was reused for a different logical request."""


def _is_positive_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _is_nonnegative_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _require_string(payload: dict, field: str) -> None:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise EventValidationError(f"payload.{field} must be a non-empty string")


def _require_task_id(payload: dict) -> None:
    if not _is_positive_int(payload.get("task_id")):
        raise EventValidationError("payload.task_id must be a positive integer")


def _validate_json_object(
    payload: dict,
    field: str,
    *,
    max_bytes: int,
) -> None:
    value = payload.get(field, {})
    if not isinstance(value, dict):
        raise EventValidationError(f"payload.{field} must be a JSON object")
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise EventValidationError(
            f"payload.{field} must contain JSON-compatible values"
        ) from exc
    if len(encoded) > max_bytes:
        raise EventValidationError(
            f"payload.{field} must not exceed {max_bytes} encoded bytes"
        )


def _validate_decisions(payload: dict) -> None:
    decisions = payload.get("decisions", [])
    if not isinstance(decisions, list):
        raise EventValidationError("payload.decisions must be a JSON array")
    try:
        json.dumps(decisions, allow_nan=False, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise EventValidationError(
            "payload.decisions must contain JSON-compatible values"
        ) from exc
    expected_fields = {
        "event_id",
        "actor",
        "assignment_id",
        "decision_id",
        "decision",
    }
    for index, decision in enumerate(decisions):
        if not isinstance(decision, dict) or set(decision) != expected_fields:
            raise EventValidationError(
                f"payload.decisions[{index}] has an invalid shape"
            )
        if not _is_positive_int(decision["event_id"]):
            raise EventValidationError(
                f"payload.decisions[{index}].event_id must be a positive integer"
            )
        for field in ("actor", "assignment_id", "decision_id"):
            value = decision[field]
            if not isinstance(value, str) or not value.strip():
                raise EventValidationError(
                    f"payload.decisions[{index}].{field} must be a non-empty string"
                )


def _validate_task_dependencies(payload: dict) -> None:
    depends_on = payload.get("depends_on", [])
    if not isinstance(depends_on, list):
        raise EventValidationError("payload.depends_on must be a JSON array")
    if len(depends_on) > MAX_TASK_DEPENDENCIES:
        raise EventValidationError(
            f"payload.depends_on must contain at most {MAX_TASK_DEPENDENCIES} tasks"
        )
    if not all(_is_positive_int(task_id) for task_id in depends_on):
        raise EventValidationError(
            "payload.depends_on must contain only positive task ids"
        )
    if len(depends_on) != len(set(depends_on)):
        raise EventValidationError("payload.depends_on must not contain duplicates")


def _validate_dependency_refs(payload: dict) -> None:
    refs = payload.get("dependency_refs", [])
    if not isinstance(refs, list):
        raise EventValidationError("payload.dependency_refs must be a JSON array")
    if len(refs) > MAX_TASK_DEPENDENCIES:
        raise EventValidationError(
            "payload.dependency_refs must contain at most "
            f"{MAX_TASK_DEPENDENCIES} references"
        )
    seen: set[int] = set()
    for index, ref in enumerate(refs):
        if not isinstance(ref, dict) or set(ref) != {
            "task_id",
            "completion_event_id",
        }:
            raise EventValidationError(
                f"payload.dependency_refs[{index}] has an invalid shape"
            )
        if not _is_positive_int(ref["task_id"]):
            raise EventValidationError(
                f"payload.dependency_refs[{index}].task_id must be a positive integer"
            )
        if not _is_positive_int(ref["completion_event_id"]):
            raise EventValidationError(
                "payload.dependency_refs"
                f"[{index}].completion_event_id must be a positive integer"
            )
        if ref["task_id"] in seen:
            raise EventValidationError(
                "payload.dependency_refs must not contain duplicate task ids"
            )
        seen.add(ref["task_id"])


def _validate_capabilities(payload: dict) -> None:
    required = payload.get("required_capabilities", [])
    if not isinstance(required, list) or not all(
        isinstance(item, str) and item.strip() for item in required
    ):
        raise EventValidationError(
            "payload.required_capabilities must be a list of strings"
        )


def _validate_retry_policy(payload: dict, *, allow_null: bool = False) -> None:
    if "retry_policy" not in payload:
        return
    retry_policy = payload["retry_policy"]
    max_retries = (
        retry_policy.get("max_retries")
        if isinstance(retry_policy, dict)
        else None
    )
    if (
        not isinstance(retry_policy, dict)
        or set(retry_policy) != {"max_retries"}
        or (
            max_retries is None
            and not allow_null
        )
        or (
            max_retries is not None
            and not _is_nonnegative_int(max_retries)
        )
    ):
        nullable = "null or " if allow_null else ""
        raise EventValidationError(
            "payload.retry_policy must contain only a "
            f"{nullable}non-negative integer max_retries"
        )


def _validate_external_origin(payload: dict, *, required: bool = False) -> None:
    origin = payload.get("external_origin")
    if origin is None and not required:
        return
    if not isinstance(origin, dict) or set(origin) != {"system", "task_ref"}:
        raise EventValidationError(
            "payload.external_origin must contain only system and task_ref"
        )
    for field in ("system", "task_ref"):
        value = origin.get(field)
        if (
            not isinstance(value, str)
            or not value.strip()
            or len(value) > MAX_EXTERNAL_ID_LENGTH
        ):
            raise EventValidationError(
                f"payload.external_origin.{field} must be a non-empty string "
                f"of at most {MAX_EXTERNAL_ID_LENGTH} characters"
            )


def _validate_ownership(payload: dict, *, observed: bool = False) -> None:
    ownership = payload.get("ownership")
    if "ownership" not in payload and not observed:
        return
    if not isinstance(ownership, dict) or set(ownership) != {"mode", "owner"}:
        raise EventValidationError(
            "payload.ownership must contain only mode and owner"
        )
    pair = (ownership.get("mode"), ownership.get("owner"))
    allowed = (
        {("shadow", "external"), ("canary", "external")}
        if observed
        else {("controlled", "agent-bus"), ("canary", "agent-bus")}
    )
    if pair not in allowed:
        raise EventValidationError(
            "payload.ownership mode and owner are not valid for this topic"
        )
    if ownership.get("mode") == "canary":
        _validate_external_origin(payload, required=True)


def _validate_correlation_id(correlation_id: Optional[str]) -> None:
    if correlation_id is None:
        return
    if (
        not isinstance(correlation_id, str)
        or not correlation_id.strip()
        or correlation_id != correlation_id.strip()
        or len(correlation_id) > MAX_CORRELATION_ID_LENGTH
    ):
        raise EventValidationError(
            "correlation_id must be null or a trimmed, non-empty string "
            f"of at most {MAX_CORRELATION_ID_LENGTH} characters"
        )


def _validate_query_correlation_id(correlation_id: Optional[str]) -> None:
    try:
        _validate_correlation_id(correlation_id)
    except EventValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _validate_producer(producer: Optional[dict]) -> None:
    if producer is None:
        return
    if not isinstance(producer, dict) or set(producer) != {
        "implementation",
        "instance_id",
        "version",
    }:
        raise EventValidationError(
            "producer must contain only implementation, instance_id, and version"
        )
    for field in ("implementation", "instance_id"):
        value = producer.get(field)
        if (
            not isinstance(value, str)
            or not value.strip()
            or value != value.strip()
            or len(value) > MAX_PRODUCER_FIELD_LENGTH
        ):
            raise EventValidationError(
                f"producer.{field} must be a trimmed, non-empty string of at most "
                f"{MAX_PRODUCER_FIELD_LENGTH} characters"
            )
    version = producer.get("version")
    if version is not None and (
        not isinstance(version, str)
        or not version.strip()
        or version != version.strip()
        or len(version) > MAX_PRODUCER_FIELD_LENGTH
    ):
        raise EventValidationError(
            "producer.version must be null or a trimmed, non-empty string of at "
            f"most {MAX_PRODUCER_FIELD_LENGTH} characters"
        )


def _validate_artifact_refs(payload: dict) -> None:
    refs = payload.get("artifacts", [])
    if not isinstance(refs, list):
        raise EventValidationError("payload.artifacts must be a JSON array")
    if len(refs) > MAX_TELEMETRY_ARTIFACT_REFS:
        raise EventValidationError(
            "payload.artifacts must contain at most "
            f"{MAX_TELEMETRY_ARTIFACT_REFS} references"
        )
    for index, ref in enumerate(refs):
        if not isinstance(ref, dict) or set(ref) != {
            "sha256",
            "size_bytes",
            "media_type",
            "kind",
        }:
            raise EventValidationError(
                f"payload.artifacts[{index}] has an invalid shape"
            )
        digest = ref.get("sha256")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise EventValidationError(
                f"payload.artifacts[{index}].sha256 must be a lowercase SHA-256 digest"
            )
        if not _is_nonnegative_int(ref.get("size_bytes")):
            raise EventValidationError(
                f"payload.artifacts[{index}].size_bytes must be a non-negative integer"
            )
        if ref["size_bytes"] > MAX_ARTIFACT_BYTES:
            raise EventValidationError(
                f"payload.artifacts[{index}].size_bytes exceeds the artifact limit"
            )
        for field in ("media_type", "kind"):
            value = ref.get(field)
            if (
                not isinstance(value, str)
                or not value.strip()
                or value != value.strip()
                or len(value) > MAX_PRODUCER_FIELD_LENGTH
            ):
                raise EventValidationError(
                    f"payload.artifacts[{index}].{field} must be a non-empty string"
                )


def _validate_telemetry(payload: dict, topic: str) -> None:
    common_fields = {
        "task_id",
        "assignment_id",
        "worker_instance_id",
        "attributes",
        "artifacts",
    }
    is_model = topic.startswith("telemetry.model.")
    identity_fields = (
        {"invocation_id", "provider", "model"}
        if is_model
        else {"tool_call_id", "tool_name"}
    )
    status_fields: set[str] = set()
    if topic.endswith(".completed"):
        status_fields.add("duration_ms")
        if is_model:
            status_fields.add("usage")
    elif topic.endswith(".failed"):
        status_fields.update({"duration_ms", "error_code", "retryable"})
        if is_model:
            status_fields.add("usage")
    allowed_fields = common_fields | identity_fields | status_fields
    if not is_model:
        allowed_fields.add("invocation_id")
    unexpected_fields = set(payload) - allowed_fields
    if unexpected_fields:
        names = ", ".join(sorted(unexpected_fields))
        raise EventValidationError(f"telemetry payload has unexpected fields: {names}")

    _require_task_id(payload)
    for field in ("assignment_id", "worker_instance_id"):
        _require_telemetry_string(payload, field)
    _validate_artifact_refs(payload)
    _validate_json_object(
        payload,
        "attributes",
        max_bytes=MAX_TELEMETRY_OBJECT_BYTES,
    )

    if is_model:
        for field in ("invocation_id", "provider", "model"):
            _require_telemetry_string(payload, field)
    else:
        for field in ("tool_call_id", "tool_name"):
            _require_telemetry_string(payload, field)
        if "invocation_id" in payload:
            _require_telemetry_string(payload, "invocation_id")

    if topic.endswith(".started"):
        return
    duration_ms = payload.get("duration_ms")
    if (
        not isinstance(duration_ms, (int, float))
        or isinstance(duration_ms, bool)
        or not math.isfinite(duration_ms)
        or duration_ms < 0
    ):
        raise EventValidationError("payload.duration_ms must be a non-negative number")
    if topic.endswith(".completed"):
        if is_model:
            _validate_json_object(
                payload,
                "usage",
                max_bytes=MAX_TELEMETRY_OBJECT_BYTES,
            )
        return
    _require_telemetry_string(payload, "error_code")
    if not isinstance(payload.get("retryable"), bool):
        raise EventValidationError("payload.retryable must be a boolean")
    if is_model:
        _validate_json_object(
            payload,
            "usage",
            max_bytes=MAX_TELEMETRY_OBJECT_BYTES,
        )


def _require_telemetry_string(payload: dict, field: str) -> None:
    _require_string(payload, field)
    value = payload[field]
    if value != value.strip() or len(value) > MAX_PRODUCER_FIELD_LENGTH:
        raise EventValidationError(
            f"payload.{field} must be trimmed and at most "
            f"{MAX_PRODUCER_FIELD_LENGTH} characters"
        )


def validate_event(
    topic: str,
    actor: str,
    payload: dict,
    caused_by: Optional[int],
    idempotency_key: Optional[str],
    schema_version: int,
    correlation_id: Optional[str] = None,
    producer: Optional[dict] = None,
) -> None:
    """Validate common fields and v2 contracts for built-in event topics.

    Version 1 remains replayable for databases created by the prototype. New
    publishes default to v2. Unknown topics are deliberately allowed so the
    bus stays an extensible coordination substrate rather than a task-board API.
    """
    if not isinstance(topic, str) or not topic.strip():
        raise EventValidationError("topic must be a non-empty string")
    if not isinstance(actor, str) or not actor.strip():
        raise EventValidationError("actor must be a non-empty string")
    if not _is_positive_int(schema_version):
        raise EventValidationError("schema_version must be a positive integer")
    if idempotency_key is not None and (
        not isinstance(idempotency_key, str) or not idempotency_key.strip()
    ):
        raise EventValidationError("idempotency_key must be null or a non-empty string")
    if caused_by is not None and not _is_positive_int(caused_by):
        raise EventValidationError("caused_by must be null or a positive event id")
    _validate_correlation_id(correlation_id)
    _validate_producer(producer)

    if topic not in KNOWN_TOPICS:
        return
    if schema_version != CURRENT_SCHEMA_VERSION:
        raise EventValidationError(
            f"unsupported schema_version {schema_version} for known topic {topic!r}"
        )

    if topic in TELEMETRY_TOPICS:
        if producer is None:
            raise EventValidationError("known telemetry events require producer identity")
        _validate_telemetry(payload, topic)
        return

    if topic == "task.created":
        if "task_id" in payload:
            _require_task_id(payload)
        _require_string(payload, "title")
        _validate_capabilities(payload)
        _validate_json_object(
            payload,
            "context",
            max_bytes=MAX_INLINE_CONTEXT_BYTES,
        )
        _validate_retry_policy(payload)
        _validate_task_dependencies(payload)
        _validate_external_origin(payload)
        _validate_ownership(payload)
        return

    if topic == "integration.task_observed":
        _require_string(payload, "title")
        _validate_capabilities(payload)
        _validate_json_object(
            payload,
            "context",
            max_bytes=MAX_INLINE_CONTEXT_BYTES,
        )
        _validate_retry_policy(payload)
        _validate_external_origin(payload, required=True)
        _validate_ownership(payload, observed=True)
        return

    if topic in {"agent.registered", "agent.heartbeat"}:
        _require_string(payload, "name")
        _require_string(payload, "instance_id")
        if payload["name"] != actor:
            raise EventValidationError("agent event actor must match payload.name")
        if topic == "agent.registered":
            capacity = payload.get("capacity", 1)
            if not _is_positive_int(capacity):
                raise EventValidationError("payload.capacity must be a positive integer")
            capabilities = payload.get("capabilities", [])
            if not isinstance(capabilities, list) or not all(
                isinstance(item, str) and item.strip() for item in capabilities
            ):
                raise EventValidationError("payload.capabilities must be a list of strings")
        return

    _require_task_id(payload)

    if topic == "task.assigned":
        for field in ("assignment_id", "assignee", "worker_instance_id"):
            _require_string(payload, field)
        if not _is_positive_int(payload.get("attempt")):
            raise EventValidationError("payload.attempt must be a positive integer")
        _validate_capabilities(payload)
        _validate_json_object(
            payload,
            "context",
            max_bytes=MAX_INLINE_CONTEXT_BYTES,
        )
        _validate_decisions(payload)
        _validate_dependency_refs(payload)
        _validate_retry_policy(payload, allow_null=True)
        if not _is_nonnegative_int(payload.get("retryable_failures", 0)):
            raise EventValidationError(
                "payload.retryable_failures must be a non-negative integer"
            )
        _validate_external_origin(payload)
        _validate_ownership(payload)
    elif topic in {"task.started", "task.completed", "task.blocked"}:
        _require_string(payload, "assignment_id")
        _require_string(payload, "worker_instance_id")
        if topic == "task.blocked":
            _require_string(payload, "reason")
        if topic == "task.completed" and "result" in payload:
            _validate_json_object(
                payload,
                "result",
                max_bytes=MAX_INLINE_RESULT_BYTES,
            )
    elif topic == "task.attempt_failed":
        for field in (
            "assignment_id",
            "worker_instance_id",
            "failure_code",
            "reason",
        ):
            _require_string(payload, field)
        if not isinstance(payload.get("retryable"), bool):
            raise EventValidationError("payload.retryable must be a boolean")
    elif topic == "task.assignment_expired":
        for field in ("assignment_id", "assignee", "worker_instance_id", "reason"):
            _require_string(payload, field)
    elif topic == "task.failed":
        for field in ("reason_code", "reason", "last_assignment_id"):
            _require_string(payload, field)
        if not _is_positive_int(payload.get("attempts")):
            raise EventValidationError("payload.attempts must be a positive integer")
        if not _is_nonnegative_int(payload.get("retryable_failures")):
            raise EventValidationError(
                "payload.retryable_failures must be a non-negative integer"
            )
        max_retries = payload.get("max_retries")
        if max_retries is not None and not _is_nonnegative_int(max_retries):
            raise EventValidationError(
                "payload.max_retries must be null or a non-negative integer"
            )
    elif topic == "task.dependency_failed":
        if actor != "pm":
            raise EventValidationError("task.dependency_failed must be emitted by pm")
        if not _is_positive_int(payload.get("dependency_task_id")):
            raise EventValidationError(
                "payload.dependency_task_id must be a positive integer"
            )
        if not _is_positive_int(payload.get("dependency_event_id")):
            raise EventValidationError(
                "payload.dependency_event_id must be a positive integer"
            )
        _require_string(payload, "reason")
    elif topic == "task.retry_requested":
        if not _is_positive_int(payload.get("additional_retries")):
            raise EventValidationError(
                "payload.additional_retries must be a positive integer"
            )
        _require_string(payload, "reason")
    elif topic == "decision.needed":
        for field in ("assignment_id", "decision_id", "reason"):
            _require_string(payload, field)
    elif topic == "decision.made":
        for field in ("assignment_id", "decision_id"):
            _require_string(payload, field)
        if "decision" not in payload:
            raise EventValidationError("payload.decision is required")
        try:
            json.dumps(payload["decision"], allow_nan=False, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            raise EventValidationError(
                "payload.decision must be JSON-compatible"
            ) from exc


# ---------------------------------------------------------------- storage


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


@contextlib.contextmanager
def db():
    """Connection that both commits the transaction AND closes the handle.

    sqlite3's own `with conn:` only commits/rolls back — it does NOT close,
    so using `with _connect() as conn:` alone would leak a connection (and
    keep the WAL/-shm files open) on every call.
    """
    conn = _connect()
    try:
        with conn:  # transaction scope (commit/rollback)
            yield conn
    finally:
        conn.close()  # lifetime scope


def init_db() -> None:
    with db() as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                ts        REAL    NOT NULL,
                topic     TEXT    NOT NULL,
                actor     TEXT    NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                idempotency_key TEXT,
                caused_by INTEGER,
                correlation_id TEXT,
                producer  TEXT,
                payload   TEXT    NOT NULL
            )
            """
        )
        columns = {r["name"] for r in conn.execute("PRAGMA table_info(events)")}
        if "idempotency_key" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN idempotency_key TEXT")
        if "schema_version" not in columns:
            conn.execute(
                "ALTER TABLE events ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1"
            )
        if "correlation_id" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN correlation_id TEXT")
        if "producer" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN producer TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS counters (
                name  TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            )
            """
        )
        max_task_id = 0
        for row in conn.execute("SELECT payload FROM events WHERE topic = 'task.created'"):
            try:
                task_id = json.loads(row["payload"]).get("task_id")
            except (json.JSONDecodeError, TypeError):
                continue
            if _is_positive_int(task_id):
                max_task_id = max(max_task_id, task_id)
        conn.execute(
            "INSERT OR IGNORE INTO counters (name, value) VALUES ('task_id', ?)",
            (max_task_id,),
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_topic ON events(topic)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_topic_id ON events(topic, id)")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_events_correlation_id
            ON events(correlation_id, id)
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_events_actor_idempotency
            ON events(actor, idempotency_key)
            WHERE idempotency_key IS NOT NULL
            """
        )


def next_task_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "UPDATE counters SET value = value + 1 WHERE name = 'task_id' RETURNING value"
    ).fetchone()
    if row is None:
        conn.execute("INSERT INTO counters (name, value) VALUES ('task_id', 1)")
        return 1
    return int(row["value"])


def row_to_dict(row: sqlite3.Row) -> dict:
    event = dict(row)
    event["payload"] = json.loads(event["payload"])
    if event.get("producer") is not None:
        event["producer"] = json.loads(event["producer"])
    event.setdefault("schema_version", 1)
    event.setdefault("correlation_id", None)
    event.setdefault("producer", None)
    return event


def _find_task_created(
    conn: sqlite3.Connection,
    task_id: int,
) -> Optional[sqlite3.Row]:
    # v0.5 deliberately derives this local-scale lookup from the immutable log.
    # It is O(task.created rows) and runs inside the append transaction. Before
    # high-volume use, replace it with a transactionally maintained
    # task_id -> created_event_id projection table or an indexed stored column.
    for row in conn.execute(
        "SELECT * FROM events WHERE topic = 'task.created' ORDER BY id"
    ):
        try:
            if json.loads(row["payload"]).get("task_id") == task_id:
                return row
        except (json.JSONDecodeError, TypeError):
            continue
    return None


def _resolve_correlation_id(
    conn: sqlite3.Connection,
    *,
    topic: str,
    caused_by: Optional[int],
    correlation_id: Optional[str],
    payload: dict,
) -> Optional[str]:
    """Resolve a new event's workflow identity inside its append transaction."""
    resolved = correlation_id
    if caused_by is not None:
        parent = conn.execute(
            "SELECT correlation_id FROM events WHERE id = ?",
            (caused_by,),
        ).fetchone()
        if parent is None:
            raise EventValidationError(f"caused_by event {caused_by} does not exist")
        parent_correlation_id = parent["correlation_id"]
        if (
            parent_correlation_id is not None
            and resolved is not None
            and resolved != parent_correlation_id
        ):
            raise EventValidationError(
                "correlation_id conflicts with the caused_by event"
            )
        resolved = parent_correlation_id or resolved

    if topic == "task.created":
        for dependency_task_id in payload.get("depends_on", []):
            dependency = _find_task_created(conn, dependency_task_id)
            if dependency is None:
                raise EventValidationError(
                    f"dependency task {dependency_task_id} does not exist"
                )
            dependency_correlation_id = dependency["correlation_id"]
            if dependency_correlation_id is None:
                raise EventValidationError(
                    f"dependency task {dependency_task_id} has no workflow identity"
                )
            if resolved is not None and resolved != dependency_correlation_id:
                raise EventValidationError(
                    "all dependencies must belong to the same correlation_id"
                )
            resolved = dependency_correlation_id

    if resolved is not None:
        return resolved
    if topic == "task.created":
        return uuid.uuid4().hex
    return None


def _assert_idempotent_match(
    row: sqlite3.Row,
    *,
    topic: str,
    payload: dict,
    caused_by: Optional[int],
    schema_version: int,
    correlation_id: Optional[str],
    producer: Optional[dict],
) -> None:
    existing = row_to_dict(row)
    stored_payload = existing["payload"]
    if topic == "task.created":
        stored_payload = dict(stored_payload)
        if "task_id" not in payload:
            # task_id is a server-generated response field, not part of the
            # caller's logical idempotent request.
            stored_payload.pop("task_id", None)
        if "retry_policy" not in payload:
            # The server materializes its current default into new task events.
            # Omitting it remains the same logical request on a retry.
            stored_payload.pop("retry_policy", None)
        if "ownership" not in payload:
            # Controlled ownership is a server-materialized default.
            stored_payload.pop("ownership", None)
        if "depends_on" not in payload:
            stored_payload.pop("depends_on", None)
    # Omitting correlation_id delegates generation/inheritance to the server.
    # On a retry, the stored value is therefore the effective requested value.
    effective_correlation_id = (
        existing["correlation_id"] if correlation_id is None else correlation_id
    )
    requested = (
        topic,
        payload,
        caused_by,
        schema_version,
        effective_correlation_id,
        producer,
    )
    stored = (
        existing["topic"],
        stored_payload,
        existing["caused_by"],
        existing["schema_version"],
        existing["correlation_id"],
        existing["producer"],
    )
    if stored != requested:
        raise IdempotencyConflict(
            "idempotency key is already attached to a different event"
        )


def append_event(
    topic: str,
    actor: str,
    payload: dict,
    caused_by: Optional[int] = None,
    idempotency_key: Optional[str] = None,
    schema_version: int = CURRENT_SCHEMA_VERSION,
    correlation_id: Optional[str] = None,
    producer: Optional[dict] = None,
) -> dict:
    validate_event(
        topic,
        actor,
        payload,
        caused_by,
        idempotency_key,
        schema_version,
        correlation_id,
        producer,
    )
    requested_payload = dict(payload)
    with db() as conn:
        if idempotency_key is not None:
            row = conn.execute(
                "SELECT * FROM events WHERE actor = ? AND idempotency_key = ?",
                (actor, idempotency_key),
            ).fetchone()
            if row is not None:
                _assert_idempotent_match(
                    row,
                    topic=topic,
                    payload=requested_payload,
                    caused_by=caused_by,
                    schema_version=schema_version,
                    correlation_id=correlation_id,
                    producer=producer,
                )
                return row_to_dict(row)

        resolved_correlation_id = _resolve_correlation_id(
            conn,
            topic=topic,
            caused_by=caused_by,
            correlation_id=correlation_id,
            payload=requested_payload,
        )
        payload = dict(requested_payload)
        if topic == "task.created":
            payload.setdefault(
                "retry_policy",
                {"max_retries": DEFAULT_MAX_RETRIES},
            )
            payload.setdefault(
                "ownership",
                {"mode": "controlled", "owner": "agent-bus"},
            )
            payload.setdefault("depends_on", [])
            if "task_id" not in payload:
                payload["task_id"] = next_task_id(conn)
            elif _is_positive_int(payload["task_id"]):
                if _find_task_created(conn, payload["task_id"]) is not None:
                    raise EventValidationError(
                        f"task_id {payload['task_id']} already exists"
                    )
                conn.execute(
                    "UPDATE counters SET value = max(value, ?) WHERE name = 'task_id'",
                    (payload["task_id"],),
                )

        try:
            cur = conn.execute(
                """
                INSERT INTO events
                    (ts, topic, actor, schema_version, idempotency_key, caused_by,
                     correlation_id, producer, payload)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    time.time(),
                    topic,
                    actor,
                    schema_version,
                    idempotency_key,
                    caused_by,
                    resolved_correlation_id,
                    json.dumps(producer, sort_keys=True, separators=(",", ":"))
                    if producer is not None
                    else None,
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                ),
            )
        except sqlite3.IntegrityError:
            if idempotency_key is None:
                raise
            # Undo any counter increment performed before a racing idempotent
            # insert lost the unique-index race.
            conn.rollback()
            row = conn.execute(
                "SELECT * FROM events WHERE actor = ? AND idempotency_key = ?",
                (actor, idempotency_key),
            ).fetchone()
            if row is None:
                raise
            _assert_idempotent_match(
                row,
                topic=topic,
                payload=requested_payload,
                caused_by=caused_by,
                schema_version=schema_version,
                correlation_id=correlation_id,
                producer=producer,
            )
            return row_to_dict(row)
        row = conn.execute("SELECT * FROM events WHERE id = ?", (cur.lastrowid,)).fetchone()
    return row_to_dict(row)


def fetch_after(
    after_id: int,
    topics: Optional[list[str]],
    limit: int = 1000,
    correlation_id: Optional[str] = None,
) -> list[dict]:
    q = "SELECT * FROM events WHERE id > ?"
    args: list = [after_id]
    if topics:
        q += f" AND topic IN ({','.join('?' * len(topics))})"
        args += topics
    if correlation_id is not None:
        q += " AND correlation_id = ?"
        args.append(correlation_id)
    q += " ORDER BY id LIMIT ?"
    args.append(limit)
    with db() as conn:
        return [row_to_dict(r) for r in conn.execute(q, args).fetchall()]


def fetch_event(event_id: int) -> Optional[dict]:
    with db() as conn:
        row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
    return row_to_dict(row) if row is not None else None


def fetch_stream_window(
    after_id: int,
    topics: Optional[list[str]],
    limit: int = 500,
    correlation_id: Optional[str] = None,
) -> tuple[list[dict], int, bool]:
    """Read a bounded matching-id window without decoding unrelated events."""
    q = "SELECT * FROM events WHERE id > ?"
    args: list = [after_id]
    if topics:
        q += f" AND topic IN ({','.join('?' * len(topics))})"
        args += topics
    if correlation_id is not None:
        q += " AND correlation_id = ?"
        args.append(correlation_id)
    q += " ORDER BY id LIMIT ?"
    args.append(limit)
    with db() as conn:
        rows = conn.execute(q, args).fetchall()
    events = [row_to_dict(row) for row in rows]
    scanned_to = events[-1]["id"] if events else after_id
    return events, scanned_to, len(events) == limit


# ---------------------------------------------------------------- app

new_event = asyncio.Condition()
event_generation = 0


@asynccontextmanager
async def lifespan(app: FastAPI):
    await run_in_threadpool(init_db)
    yield


app = FastAPI(title="agent-bus", version="0.6.0", lifespan=lifespan)


class PublishRequest(BaseModel):
    topic: str = Field(..., examples=["task.created"])
    actor: str = Field(..., examples=["pm"])
    payload: dict = Field(default_factory=dict)
    caused_by: Optional[int] = None
    idempotency_key: Optional[str] = None
    schema_version: int = CURRENT_SCHEMA_VERSION
    correlation_id: Optional[str] = None
    producer: Optional[dict] = None


@app.post("/events", dependencies=[Depends(require_token)])
async def publish(req: PublishRequest) -> dict:
    global event_generation
    try:
        event = await run_in_threadpool(
            append_event,
            req.topic,
            req.actor,
            req.payload,
            req.caused_by,
            req.idempotency_key,
            req.schema_version,
            req.correlation_id,
            req.producer,
        )
    except EventValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    async with new_event:
        event_generation += 1
        new_event.notify_all()
    return event


@app.get("/events", dependencies=[Depends(require_token)])
async def query(
    response: Response,
    after_id: int = 0,
    topics: Optional[str] = Query(None),
    limit: int = Query(1000, ge=1, le=10_000),
    correlation_id: Optional[str] = Query(
        None, min_length=1, max_length=MAX_CORRELATION_ID_LENGTH
    ),
) -> list[dict]:
    _validate_query_correlation_id(correlation_id)
    topic_list = [topic.strip() for topic in topics.split(",") if topic.strip()] if topics else None
    events = await run_in_threadpool(
        fetch_after, after_id, topic_list, limit, correlation_id
    )
    # A full page means more events may exist; callers that want everything
    # should paginate (see BusClient.query_all) instead of trusting one page.
    response.headers["X-Page-Full"] = "1" if len(events) == limit else "0"
    return events


@app.get("/events/stream", dependencies=[Depends(require_token)])
async def stream(
    request: Request,
    from_id: int = 0,
    topics: Optional[str] = Query(None),
    correlation_id: Optional[str] = Query(
        None, min_length=1, max_length=MAX_CORRELATION_ID_LENGTH
    ),
):
    """Replay history after from_id in bounded windows, then follow live."""
    _validate_query_correlation_id(correlation_id)
    topic_list = [topic.strip() for topic in topics.split(",") if topic.strip()] if topics else None

    async def gen():
        last_id = from_id
        observed_generation = event_generation
        while True:
            if await request.is_disconnected():
                return
            events, scanned_to, full_window = await run_in_threadpool(
                fetch_stream_window, last_id, topic_list, 500, correlation_id
            )
            last_id = scanned_to
            for event in events:
                yield f"id: {event['id']}\ndata: {json.dumps(event)}\n\n"
            if full_window:
                continue
            try:
                async with new_event:
                    if event_generation == observed_generation:
                        await asyncio.wait_for(new_event.wait(), timeout=15.0)
                    observed_generation = event_generation
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/events/{event_id}", dependencies=[Depends(require_token)])
async def get_event(event_id: int) -> dict:
    if event_id <= 0:
        raise HTTPException(status_code=422, detail="event_id must be positive")
    event = await run_in_threadpool(fetch_event, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    return event


@app.get("/health")
async def health() -> dict:
    return {"ok": True, "schema_version": CURRENT_SCHEMA_VERSION}

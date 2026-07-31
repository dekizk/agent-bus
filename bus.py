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

DB_PATH = Path(__file__).parent / "events.db"
CURRENT_SCHEMA_VERSION = 2
MAX_CORRELATION_ID_LENGTH = 128


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

KNOWN_TOPICS = {
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


def validate_event(
    topic: str,
    actor: str,
    payload: dict,
    caused_by: Optional[int],
    idempotency_key: Optional[str],
    schema_version: int,
    correlation_id: Optional[str] = None,
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

    if topic not in KNOWN_TOPICS:
        return
    if schema_version != CURRENT_SCHEMA_VERSION:
        raise EventValidationError(
            f"unsupported schema_version {schema_version} for known topic {topic!r}"
        )

    if topic == "task.created":
        if "task_id" in payload:
            _require_task_id(payload)
        if "title" in payload and not isinstance(payload["title"], str):
            raise EventValidationError("payload.title must be a string")
        required = payload.get("required_capabilities", [])
        if not isinstance(required, list) or not all(
            isinstance(item, str) and item.strip() for item in required
        ):
            raise EventValidationError(
                "payload.required_capabilities must be a list of strings"
            )
        if "retry_policy" in payload:
            retry_policy = payload["retry_policy"]
            if (
                not isinstance(retry_policy, dict)
                or set(retry_policy) != {"max_retries"}
                or not _is_nonnegative_int(retry_policy.get("max_retries"))
            ):
                raise EventValidationError(
                    "payload.retry_policy must contain only a non-negative "
                    "integer max_retries"
                )
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
    elif topic in {"task.started", "task.completed", "task.blocked"}:
        _require_string(payload, "assignment_id")
        _require_string(payload, "worker_instance_id")
        if topic == "task.blocked":
            _require_string(payload, "reason")
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
    event.setdefault("schema_version", 1)
    event.setdefault("correlation_id", None)
    return event


def _resolve_correlation_id(
    conn: sqlite3.Connection,
    *,
    topic: str,
    caused_by: Optional[int],
    correlation_id: Optional[str],
) -> Optional[str]:
    """Resolve a new event's workflow identity inside its append transaction."""
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
            and correlation_id is not None
            and correlation_id != parent_correlation_id
        ):
            raise EventValidationError(
                "correlation_id conflicts with the caused_by event"
            )
        return parent_correlation_id or correlation_id

    if correlation_id is not None:
        return correlation_id
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
    )
    stored = (
        existing["topic"],
        stored_payload,
        existing["caused_by"],
        existing["schema_version"],
        existing["correlation_id"],
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
) -> dict:
    validate_event(
        topic,
        actor,
        payload,
        caused_by,
        idempotency_key,
        schema_version,
        correlation_id,
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
                )
                return row_to_dict(row)

        resolved_correlation_id = _resolve_correlation_id(
            conn,
            topic=topic,
            caused_by=caused_by,
            correlation_id=correlation_id,
        )
        payload = dict(requested_payload)
        if topic == "task.created":
            payload.setdefault(
                "retry_policy",
                {"max_retries": DEFAULT_MAX_RETRIES},
            )
            if "task_id" not in payload:
                payload["task_id"] = next_task_id(conn)
            elif _is_positive_int(payload["task_id"]):
                conn.execute(
                    "UPDATE counters SET value = max(value, ?) WHERE name = 'task_id'",
                    (payload["task_id"],),
                )

        try:
            cur = conn.execute(
                """
                INSERT INTO events
                    (ts, topic, actor, schema_version, idempotency_key, caused_by,
                     correlation_id, payload)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    time.time(),
                    topic,
                    actor,
                    schema_version,
                    idempotency_key,
                    caused_by,
                    resolved_correlation_id,
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


app = FastAPI(title="agent-bus", version="0.3.0", lifespan=lifespan)


class PublishRequest(BaseModel):
    topic: str = Field(..., examples=["task.created"])
    actor: str = Field(..., examples=["pm"])
    payload: dict = Field(default_factory=dict)
    caused_by: Optional[int] = None
    idempotency_key: Optional[str] = None
    schema_version: int = CURRENT_SCHEMA_VERSION
    correlation_id: Optional[str] = None


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


@app.get("/health")
async def health() -> dict:
    return {"ok": True, "schema_version": CURRENT_SCHEMA_VERSION}

"""
agent-bus: a minimal append-only event bus for multi-agent coordination.

Design:
  - Events are immutable rows in SQLite (the log IS the source of truth).
  - Consumers track their own offset (last event id seen) -> crash recovery
    is just "resume from offset".
  - Subscribers get history-then-live via a single SSE stream:
      GET /events/stream?from_id=0&topics=task.assigned,task.completed
  - Publishing is a POST; an asyncio.Condition wakes all live subscribers.

Run:  uvicorn bus:app --port 8765
"""

import asyncio
import contextlib
import json
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

DB_PATH = Path(__file__).parent / "events.db"

# ---------------------------------------------------------------- storage


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


@contextlib.contextmanager
def db():
    """Connection that both commits the transaction AND closes the handle.

    Note: sqlite3's own `with conn:` only commits/rolls back — it does NOT
    close, so using `with _connect() as conn:` leaks a connection (and keeps
    the WAL/-shm files open) on every call.
    """
    conn = _connect()
    try:
        with conn:  # transaction scope (commit/rollback)
            yield conn
    finally:
        conn.close()  # lifetime scope


def init_db() -> None:
    with db() as conn:
        # WAL is a persistent database property — set it once here rather
        # than on every connection.
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                ts        REAL    NOT NULL,
                topic     TEXT    NOT NULL,
                actor     TEXT    NOT NULL,
                idempotency_key TEXT,
                caused_by INTEGER,           -- causal parent event id (audit trail)
                payload   TEXT    NOT NULL   -- JSON
            )
            """
        )
        columns = {r["name"] for r in conn.execute("PRAGMA table_info(events)")}
        if "idempotency_key" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN idempotency_key TEXT")
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
            except json.JSONDecodeError:
                continue
            if isinstance(task_id, int):
                max_task_id = max(max_task_id, task_id)
        conn.execute(
            "INSERT OR IGNORE INTO counters (name, value) VALUES ('task_id', ?)",
            (max_task_id,),
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_topic ON events(topic)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_topic_id ON events(topic, id)")
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_events_actor_idempotency
            ON events(actor, idempotency_key)
            WHERE idempotency_key IS NOT NULL
            """
        )


def next_task_id(conn: sqlite3.Connection) -> int:
    # Atomic increment-and-read in one statement (RETURNING needs SQLite
    # 3.35+, 2021). The separate UPDATE-then-SELECT it replaces was only
    # safe because SQLite serializes writers; RETURNING stays correct even
    # if connections are ever pooled or the backend changes.
    row = conn.execute(
        "UPDATE counters SET value = value + 1 WHERE name = 'task_id' RETURNING value"
    ).fetchone()
    if row is None:  # counter row missing (e.g. manually deleted)
        conn.execute("INSERT INTO counters (name, value) VALUES ('task_id', 1)")
        return 1
    return int(row["value"])


def append_event(
    topic: str,
    actor: str,
    payload: dict,
    caused_by: Optional[int],
    idempotency_key: Optional[str],
) -> dict:
    with db() as conn:
        if idempotency_key:
            row = conn.execute(
                "SELECT * FROM events WHERE actor = ? AND idempotency_key = ?",
                (actor, idempotency_key),
            ).fetchone()
            if row is not None:
                return row_to_dict(row)

        payload = dict(payload)
        if topic == "task.created":
            if "task_id" not in payload:
                payload["task_id"] = next_task_id(conn)
            elif isinstance(payload["task_id"], int):
                conn.execute(
                    "UPDATE counters SET value = max(value, ?) WHERE name = 'task_id'",
                    (payload["task_id"],),
                )

        try:
            cur = conn.execute(
                """
                INSERT INTO events (ts, topic, actor, idempotency_key, caused_by, payload)
                VALUES (?,?,?,?,?,?)
                """,
                (time.time(), topic, actor, idempotency_key, caused_by, json.dumps(payload)),
            )
        except sqlite3.IntegrityError:
            if not idempotency_key:
                raise
            row = conn.execute(
                "SELECT * FROM events WHERE actor = ? AND idempotency_key = ?",
                (actor, idempotency_key),
            ).fetchone()
            if row is None:
                raise
            return row_to_dict(row)
        row = conn.execute("SELECT * FROM events WHERE id = ?", (cur.lastrowid,)).fetchone()
    return row_to_dict(row)


def fetch_after(after_id: int, topics: Optional[list[str]]) -> list[dict]:
    q = "SELECT * FROM events WHERE id > ?"
    args: list = [after_id]
    if topics:
        q += f" AND topic IN ({','.join('?' * len(topics))})"
        args += topics
    q += " ORDER BY id"
    with db() as conn:
        return [row_to_dict(r) for r in conn.execute(q, args).fetchall()]


def row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["payload"] = json.loads(d["payload"])
    return d


# ---------------------------------------------------------------- app

new_event = asyncio.Condition()  # wakes live subscribers on every append


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="agent-bus", lifespan=lifespan)


class PublishRequest(BaseModel):
    topic: str = Field(..., examples=["task.created"])
    actor: str = Field(..., examples=["pm"])
    payload: dict = Field(default_factory=dict)
    caused_by: Optional[int] = None
    idempotency_key: Optional[str] = None


@app.post("/events")
async def publish(req: PublishRequest) -> dict:
    event = append_event(req.topic, req.actor, req.payload, req.caused_by, req.idempotency_key)
    async with new_event:
        new_event.notify_all()
    return event


@app.get("/events")
async def query(after_id: int = 0, topics: Optional[str] = Query(None)) -> list[dict]:
    topic_list = topics.split(",") if topics else None
    return fetch_after(after_id, topic_list)


@app.get("/events/stream")
async def stream(request: Request, from_id: int = 0, topics: Optional[str] = Query(None)):
    """SSE stream: replays history after from_id, then follows live events."""
    topic_list = topics.split(",") if topics else None

    async def gen():
        last_id = from_id
        while True:
            if await request.is_disconnected():
                return
            for ev in fetch_after(last_id, topic_list):
                last_id = ev["id"]
                yield f"id: {ev['id']}\ndata: {json.dumps(ev)}\n\n"
            # wait for a new append (or timeout to send a keepalive + check disconnect)
            try:
                async with new_event:
                    await asyncio.wait_for(new_event.wait(), timeout=15.0)
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/health")
async def health() -> dict:
    return {"ok": True}

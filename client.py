"""Blocking client library for agent-bus."""

import fcntl
import hashlib
import json
import os
import re
import tempfile
import threading
import time
from pathlib import Path
from typing import Callable, Iterator, Optional

import httpx

CURRENT_SCHEMA_VERSION = 2


class BusProtocolError(RuntimeError):
    """The bus returned a malformed event stream frame."""


class BusClient:
    def __init__(
        self,
        base_url: str,
        actor: str,
        offset_dir: Path = Path(".offsets"),
        offset_name: Optional[str] = None,
        token: Optional[str] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.actor = actor
        token = token if token is not None else os.environ.get("AGENT_BUS_TOKEN")
        self._headers = {"Authorization": f"Bearer {token}"} if token else {}
        offset_dir.mkdir(parents=True, exist_ok=True)
        consumer_name = offset_name or actor
        if re.fullmatch(r"[A-Za-z0-9_.-]+", consumer_name) and consumer_name not in {".", ".."}:
            safe_name = consumer_name
        else:
            prefix = re.sub(r"[^A-Za-z0-9_-]+", "_", consumer_name).strip("_")[:40]
            digest = hashlib.sha256(consumer_name.encode()).hexdigest()[:12]
            safe_name = f"{prefix or 'consumer'}-{digest}"
        self.offset_file = offset_dir / f"{safe_name}.offset"
        self.offset_lock_file = offset_dir / f"{safe_name}.offset.lock"

    def publish(
        self,
        topic: str,
        payload: dict,
        caused_by: Optional[int] = None,
        idempotency_key: Optional[str] = None,
        schema_version: int = CURRENT_SCHEMA_VERSION,
        correlation_id: Optional[str] = None,
        producer: Optional[dict] = None,
    ) -> dict:
        body = {
            "topic": topic,
            "actor": self.actor,
            "payload": payload,
            "caused_by": caused_by,
            "schema_version": schema_version,
        }
        if idempotency_key is not None:
            body["idempotency_key"] = idempotency_key
        if correlation_id is not None:
            body["correlation_id"] = correlation_id
        if producer is not None:
            body["producer"] = producer
        response = httpx.post(
            f"{self.base_url}/events", json=body, headers=self._headers, timeout=10
        )
        response.raise_for_status()
        return response.json()

    def cancel_task(
        self,
        task_id: int,
        *,
        reason: str = "cancelled by requester",
        idempotency_key: Optional[str] = None,
    ) -> dict:
        if not isinstance(task_id, int) or isinstance(task_id, bool) or task_id <= 0:
            raise ValueError("task_id must be a positive integer")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        return self.publish(
            "task.cancel_requested",
            {"task_id": task_id, "reason": reason.strip()},
            idempotency_key=idempotency_key or f"cancel:task:{task_id}",
        )

    def subscribe(
        self,
        topics: Optional[list[str]] = None,
        from_id: int = 0,
        on_idle: Optional[Callable[[], None]] = None,
        correlation_id: Optional[str] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> Iterator[dict]:
        """Follow SSE with reconnect, resuming after the last yielded event.

        on_idle runs on server keepalives. Coordinators can use it for
        time-based reconciliation without introducing a second state-mutating
        thread.
        """
        last_id = from_id
        backoff = 1.0
        while stop_event is None or not stop_event.is_set():
            yielded = False
            try:
                for event in self._stream_once(
                    topics,
                    last_id,
                    on_idle,
                    correlation_id,
                    stop_event,
                ):
                    yielded = True
                    last_id = event["id"]
                    backoff = 1.0
                    yield event
                # A graceful server close is still a disconnect. Avoid a tight
                # reconnect loop if a proxy repeatedly returns an empty stream.
                if not yielded:
                    if stop_event is not None:
                        if stop_event.wait(backoff):
                            return
                    else:
                        time.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
            except httpx.HTTPError as exc:
                print(
                    f"[{self.actor}] bus connection lost ({exc.__class__.__name__}); "
                    f"reconnecting from #{last_id} in {backoff:.0f}s",
                    flush=True,
                )
                if stop_event is not None:
                    if stop_event.wait(backoff):
                        return
                else:
                    time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    def _stream_once(
        self,
        topics: Optional[list[str]],
        from_id: int,
        on_idle: Optional[Callable[[], None]] = None,
        correlation_id: Optional[str] = None,
        stop_event: Optional[threading.Event] = None,
    ) -> Iterator[dict]:
        params: dict = {"from_id": from_id}
        if topics:
            params["topics"] = ",".join(topics)
        if correlation_id is not None:
            params["correlation_id"] = correlation_id
        with httpx.stream(
            "GET",
            f"{self.base_url}/events/stream",
            params=params,
            headers=self._headers,
            timeout=None,
        ) as response:
            response.raise_for_status()
            for line in response.iter_lines():
                if stop_event is not None and stop_event.is_set():
                    return
                if line.startswith("data: "):
                    raw = line[len("data: ") :]
                    try:
                        event = json.loads(raw)
                    except json.JSONDecodeError as exc:
                        raise BusProtocolError(
                            "bus stream contained invalid JSON"
                        ) from exc
                    if not isinstance(event, dict):
                        raise BusProtocolError("bus stream event must be an object")
                    event_id = event.get("id")
                    if (
                        not isinstance(event_id, int)
                        or isinstance(event_id, bool)
                        or event_id <= 0
                    ):
                        raise BusProtocolError(
                            "bus stream event must contain a positive integer id"
                        )
                    yield event
                elif line.startswith(":") and on_idle is not None:
                    on_idle()

    def query(
        self,
        after_id: int = 0,
        topics: Optional[list[str]] = None,
        limit: int = 1000,
        correlation_id: Optional[str] = None,
    ) -> list[dict]:
        params: dict = {"after_id": after_id, "limit": limit}
        if topics:
            params["topics"] = ",".join(topics)
        if correlation_id is not None:
            params["correlation_id"] = correlation_id
        response = httpx.get(
            f"{self.base_url}/events", params=params, headers=self._headers, timeout=10
        )
        response.raise_for_status()
        return response.json()

    def get_event(self, event_id: int) -> dict:
        if not isinstance(event_id, int) or isinstance(event_id, bool) or event_id <= 0:
            raise ValueError("event_id must be a positive integer")
        response = httpx.get(
            f"{self.base_url}/events/{event_id}",
            headers=self._headers,
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    def query_all(
        self,
        after_id: int = 0,
        topics: Optional[list[str]] = None,
        page_size: int = 1000,
        correlation_id: Optional[str] = None,
    ) -> list[dict]:
        """Read a complete snapshot using bounded pages."""
        events: list[dict] = []
        cursor = after_id
        while True:
            page = self.query(
                after_id=cursor,
                topics=topics,
                limit=page_size,
                correlation_id=correlation_id,
            )
            if not page:
                return events
            events.extend(page)
            cursor = page[-1]["id"]
            if len(page) < page_size:
                return events

    def load_offset(self) -> int:
        try:
            return int(self.offset_file.read_text())
        except (FileNotFoundError, ValueError):
            return 0

    def save_offset(self, event_id: int) -> None:
        """Atomically and monotonically persist a consumer resume point."""
        if not isinstance(event_id, int) or isinstance(event_id, bool) or event_id < 0:
            raise ValueError("event_id must be a non-negative integer")

        with self.offset_lock_file.open("a+") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            if event_id <= self.load_offset():
                return

            fd, temp_name = tempfile.mkstemp(
                prefix=f".{self.offset_file.name}.",
                dir=self.offset_file.parent,
                text=True,
            )
            try:
                with os.fdopen(fd, "w") as temp_file:
                    temp_file.write(str(event_id))
                    temp_file.flush()
                    os.fsync(temp_file.fileno())
                os.replace(temp_name, self.offset_file)
            finally:
                try:
                    os.unlink(temp_name)
                except FileNotFoundError:
                    pass

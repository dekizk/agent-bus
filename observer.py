"""GET-only client used by operational projections and CLI commands."""

from __future__ import annotations

import json
import time
from typing import Iterator, Optional

import httpx


class ObserverClient:
    """Read agent-bus history without exposing any state-mutating method."""

    def __init__(
        self,
        base_url: str,
        *,
        token: Optional[str] = None,
        client: Optional[httpx.Client] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self._headers = {"Authorization": f"Bearer {token}"} if token else {}
        self._owns_client = client is None
        self._client = client or httpx.Client(headers=self._headers, timeout=10)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def health(self) -> dict:
        response = self._client.get(f"{self.base_url}/health", headers=self._headers)
        response.raise_for_status()
        value = response.json()
        if not isinstance(value, dict):
            raise ValueError("health response was not a JSON object")
        return value

    def query(
        self,
        *,
        after_id: int = 0,
        topics: Optional[list[str]] = None,
        limit: int = 1000,
        correlation_id: Optional[str] = None,
    ) -> list[dict]:
        params: dict[str, object] = {"after_id": after_id, "limit": limit}
        if topics:
            params["topics"] = ",".join(topics)
        if correlation_id is not None:
            params["correlation_id"] = correlation_id
        response = self._client.get(
            f"{self.base_url}/events",
            params=params,
            headers=self._headers,
        )
        response.raise_for_status()
        value = response.json()
        if not isinstance(value, list) or not all(
            isinstance(event, dict) for event in value
        ):
            raise ValueError("events response was not a JSON event list")
        return value

    def query_all(
        self,
        *,
        after_id: int = 0,
        topics: Optional[list[str]] = None,
        page_size: int = 1000,
        correlation_id: Optional[str] = None,
    ) -> list[dict]:
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
            event_id = page[-1].get("id")
            if not isinstance(event_id, int) or event_id <= cursor:
                raise ValueError("events response did not advance its cursor")
            events.extend(page)
            cursor = event_id
            if len(page) < page_size:
                return events

    def iter_events(self, *, through_id: int, after_id: int = 0,
                    topics: Optional[list[str]] = None, page_size: int = 1000):
        """Stream a fixed prefix in bounded pages, without offsets or writes."""
        if type(through_id) is not int or type(after_id) is not int or not 0 <= after_id <= through_id:
            raise ValueError("history bounds must be ordered nonnegative integers")
        if type(page_size) is not int or not 1 <= page_size <= 10000:
            raise ValueError("invalid history page size")
        cursor = after_id
        while cursor < through_id:
            page = self.query(after_id=cursor, topics=topics, limit=page_size)
            if not isinstance(page, list) or len(page) > page_size:
                raise ValueError("history response exceeds its page limit")
            if not page:
                return
            for event in page:
                if not isinstance(event, dict):
                    raise ValueError("history contains a non-event")
                event_id = event.get("id")
                if type(event_id) is not int or event_id <= cursor:
                    raise ValueError("history is not strictly ordered")
                if event_id > through_id:
                    return
                if not isinstance(event.get("payload"), dict) or (topics and event.get("topic") not in topics):
                    raise ValueError("history contains a malformed or incorrectly filtered event")
                cursor = event_id
                yield event
            if len(page) < page_size:
                return

    def subscribe(
        self,
        *,
        from_id: int = 0,
        topics: Optional[list[str]] = None,
        correlation_id: Optional[str] = None,
    ) -> Iterator[dict]:
        """Follow a read-only stream and resume after disconnects."""
        last_id = from_id
        backoff = 1.0
        connected_once = False
        while True:
            params: dict[str, object] = {"from_id": last_id}
            if topics:
                params["topics"] = ",".join(topics)
            if correlation_id is not None:
                params["correlation_id"] = correlation_id
            try:
                with self._client.stream(
                    "GET",
                    f"{self.base_url}/events/stream",
                    params=params,
                    headers=self._headers,
                    timeout=None,
                ) as response:
                    response.raise_for_status()
                    connected_once = True
                    backoff = 1.0
                    for line in response.iter_lines():
                        if not line.startswith("data: "):
                            continue
                        event = json.loads(line[len("data: ") :])
                        if not isinstance(event, dict):
                            continue
                        event_id = event.get("id")
                        if isinstance(event_id, int) and event_id > last_id:
                            last_id = event_id
                            yield event
                # A graceful close is still a disconnect. Avoid a tight loop
                # when a proxy repeatedly returns a short or empty stream.
                time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
            except httpx.HTTPStatusError:
                raise
            except httpx.HTTPError:
                if not connected_once:
                    raise
                time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

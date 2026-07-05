"""
Tiny client library for agent-bus. Agents use this to publish and subscribe.

  bus = BusClient("http://127.0.0.1:8765", actor="worker-1")
  bus.publish("task.completed", {"task_id": 3}, caused_by=17)
  for ev in bus.subscribe(topics=["task.assigned"], from_id=bus.load_offset()):
      handle(ev)
      bus.save_offset(ev["id"])   # idempotent resume point
"""

import json
import time
from pathlib import Path
from typing import Iterator, Optional

import httpx


class BusClient:
    def __init__(self, base_url: str, actor: str, offset_dir: Path = Path(".offsets")):
        self.base_url = base_url.rstrip("/")
        self.actor = actor
        self.offset_file = offset_dir / f"{actor}.offset"
        offset_dir.mkdir(exist_ok=True)

    # -- publish -----------------------------------------------------------

    def publish(
        self,
        topic: str,
        payload: dict,
        caused_by: Optional[int] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        body = {
            "topic": topic,
            "actor": self.actor,
            "payload": payload,
            "caused_by": caused_by,
        }
        if idempotency_key:
            body["idempotency_key"] = idempotency_key
        r = httpx.post(
            f"{self.base_url}/events",
            json=body,
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    # -- subscribe (blocking generator over SSE) ---------------------------

    def subscribe(self, topics: Optional[list[str]] = None, from_id: int = 0) -> Iterator[dict]:
        """Blocking generator over SSE, with automatic reconnect.

        If the bus restarts or the connection drops, we back off and
        reconnect, resuming from the last event id we actually yielded —
        so consumers never miss or re-see events across a reconnect.
        """
        last_id = from_id
        backoff = 1.0
        while True:
            try:
                for ev in self._stream_once(topics, last_id):
                    last_id = ev["id"]
                    backoff = 1.0  # healthy stream -> reset backoff
                    yield ev
            except (httpx.HTTPError, httpx.StreamError) as exc:
                print(f"[{self.actor}] bus connection lost ({exc.__class__.__name__}); "
                      f"reconnecting from #{last_id} in {backoff:.0f}s", flush=True)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    def _stream_once(self, topics: Optional[list[str]], from_id: int) -> Iterator[dict]:
        """One SSE connection; raises httpx errors on disconnect."""
        params: dict = {"from_id": from_id}
        if topics:
            params["topics"] = ",".join(topics)
        with httpx.stream(
            "GET", f"{self.base_url}/events/stream", params=params, timeout=None
        ) as resp:
            resp.raise_for_status()
            for line in resp.iter_lines():
                if line.startswith("data: "):
                    yield json.loads(line[len("data: "):])

    # -- query (non-blocking history read) ---------------------------------

    def query(self, after_id: int = 0, topics: Optional[list[str]] = None) -> list[dict]:
        params: dict = {"after_id": after_id}
        if topics:
            params["topics"] = ",".join(topics)
        r = httpx.get(f"{self.base_url}/events", params=params, timeout=10)
        r.raise_for_status()
        return r.json()

    # -- offset persistence (crash recovery) --------------------------------

    def load_offset(self) -> int:
        try:
            return int(self.offset_file.read_text())
        except (FileNotFoundError, ValueError):
            return 0

    def save_offset(self, event_id: int) -> None:
        self.offset_file.write_text(str(event_id))

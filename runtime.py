"""Reusable leased worker runtime for framework-neutral executors."""

from __future__ import annotations

import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable, Iterable, Optional

import httpx

from client import BusClient
from executors import (
    AssignmentContext,
    Blocked,
    Completed,
    Executor,
    PermanentFailure,
    RetryableFailure,
    ensure_outcome,
    json_size,
    mutable_json,
)
from limits import MAX_INLINE_RESULT_BYTES

DEFAULT_HEARTBEAT_SECONDS = 5.0
DEFAULT_PUBLISH_RETRY_SECONDS = 1.0
RUNTIME_TOPICS = (
    "agent.registered",
    "task.assigned",
    "task.assignment_expired",
    "task.failed",
)


class WorkerRuntime:
    """Own bus plumbing while delegating actual work to an Executor.

    Capacity is enforced by a bounded thread pool. Ownership loss cancels
    cancellable adapters and suppresses their eventual lifecycle result. The
    PM reducer remains the final race-safe authority for stale events.
    """

    def __init__(
        self,
        bus: BusClient,
        *,
        name: str,
        executor: Executor,
        capacity: int = 1,
        capabilities: Iterable[str] = (),
        instance_id: Optional[str] = None,
        heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
        publish_retry_seconds: float = DEFAULT_PUBLISH_RETRY_SECONDS,
        unexpected_exceptions_retryable: bool = False,
        log: Callable[[str], None] = print,
    ):
        if not isinstance(name, str) or not name.strip():
            raise ValueError("name must be a non-empty string")
        if not isinstance(capacity, int) or isinstance(capacity, bool) or capacity < 1:
            raise ValueError("capacity must be a positive integer")
        if heartbeat_seconds <= 0 or publish_retry_seconds <= 0:
            raise ValueError("heartbeat and publish retry intervals must be positive")
        normalized_capabilities = tuple(dict.fromkeys(capabilities))
        if not all(isinstance(item, str) and item.strip() for item in normalized_capabilities):
            raise ValueError("capabilities must contain non-empty strings")
        if getattr(bus, "actor", name) != name:
            raise ValueError("bus actor must match worker name")

        self.bus = bus
        self.name = name
        self.executor = executor
        self.capacity = capacity
        self.capabilities = normalized_capabilities
        self.instance_id = instance_id or uuid.uuid4().hex
        self.heartbeat_seconds = heartbeat_seconds
        self.publish_retry_seconds = publish_retry_seconds
        self.unexpected_exceptions_retryable = unexpected_exceptions_retryable
        self.log = log

        self.stop_event = threading.Event()
        self._heartbeat_stop = threading.Event()
        self._lock = threading.Lock()
        self._owned: set[str] = set()
        self._futures: dict[str, Future] = {}
        self._accepting = False
        self._closed = False
        self._pool: Optional[ThreadPoolExecutor] = None
        self._heartbeat: Optional[threading.Thread] = None

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._owned)

    def run(self, events: Optional[Iterable[dict]] = None) -> None:
        """Register and consume events until stopped.

        Passing a finite event iterable is useful for embedding and tests; a
        normally exhausted iterable drains accepted work before shutdown.
        """
        registered = self.bus.publish(
            "agent.registered",
            {
                "name": self.name,
                "instance_id": self.instance_id,
                "capacity": self.capacity,
                "capabilities": list(self.capabilities),
            },
            idempotency_key=f"registered:{self.instance_id}",
        )
        self.log(
            f"[{self.name}] registered instance {self.instance_id[:8]}, "
            "waiting for work"
        )
        with self._lock:
            self._accepting = True
        self._pool = ThreadPoolExecutor(
            max_workers=self.capacity,
            thread_name_prefix=f"{self.name}-assignment",
        )
        self._heartbeat = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"{self.name}-heartbeat",
        )
        self._heartbeat.start()

        completed_normally = False
        try:
            stream = events
            if stream is None:
                stream = self.bus.subscribe(
                    topics=list(RUNTIME_TOPICS),
                    from_id=registered["id"],
                    stop_event=self.stop_event,
                )
            for event in stream:
                if self.stop_event.is_set():
                    break
                self.process_event(event)
            completed_normally = not self.stop_event.is_set()
        finally:
            self.shutdown(drain=completed_normally)

    def process_event(self, event: dict) -> None:
        if not isinstance(event, dict):
            return
        topic = event.get("topic")
        payload = event.get("payload")
        if not isinstance(payload, dict):
            return

        if topic == "agent.registered":
            if (
                payload.get("name") == self.name
                and payload.get("instance_id") != self.instance_id
            ):
                self.log(f"[{self.name}] replacement instance registered; stopping")
                self.stop()
            return

        if topic == "task.assignment_expired":
            if payload.get("worker_instance_id") == self.instance_id:
                self._revoke(payload.get("assignment_id"))
            return

        if topic == "task.failed":
            self._revoke(payload.get("last_assignment_id"))
            return

        if topic != "task.assigned":
            return
        if (
            payload.get("assignee") != self.name
            or payload.get("worker_instance_id") != self.instance_id
        ):
            return
        try:
            assignment = AssignmentContext.from_event(event)
        except (TypeError, ValueError) as exc:
            self.log(f"[{self.name}] ignored malformed assignment: {exc}")
            return

        with self._lock:
            if (
                not self._accepting
                or assignment.assignment_id in self._owned
                or self._pool is None
            ):
                return
            self._owned.add(assignment.assignment_id)
            future = self._pool.submit(self._execute_assignment, assignment)
            self._futures[assignment.assignment_id] = future

    def stop(self) -> None:
        self.stop_event.set()
        with self._lock:
            assignment_ids = tuple(self._owned)
            self._accepting = False
        for assignment_id in assignment_ids:
            self._revoke(assignment_id)

    def shutdown(self, *, drain: bool = False) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._accepting = False
            assignment_ids = tuple(self._owned)
        if not drain:
            self.stop_event.set()
            for assignment_id in assignment_ids:
                self._revoke(assignment_id)
            close = getattr(self.executor, "close", None)
            if callable(close):
                close()
        if self._pool is not None:
            self._pool.shutdown(wait=True, cancel_futures=not drain)
        if drain:
            close = getattr(self.executor, "close", None)
            if callable(close):
                close()
        self.stop_event.set()
        self._heartbeat_stop.set()
        if self._heartbeat is not None:
            self._heartbeat.join(timeout=max(1.0, self.heartbeat_seconds + 0.5))

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(self.heartbeat_seconds):
            try:
                self.bus.publish(
                    "agent.heartbeat",
                    {"name": self.name, "instance_id": self.instance_id},
                )
            except httpx.HTTPError as exc:
                self.log(
                    f"[{self.name}] heartbeat failed ({exc.__class__.__name__})"
                )

    def _execute_assignment(self, assignment: AssignmentContext) -> None:
        assignment_id = assignment.assignment_id
        lifecycle = {
            "task_id": assignment.task_id,
            "assignment_id": assignment_id,
            "worker_instance_id": self.instance_id,
        }
        try:
            started = self._publish_while_owned(
                assignment_id,
                "task.started",
                lifecycle,
                caused_by=assignment.assignment_event_id,
                idempotency_key=f"started:{assignment_id}",
            )
            if started is None:
                return
            self.log(
                f"[{self.name}] executing task {assignment.task_id} "
                f"attempt {assignment.attempt}: {assignment.goal}"
            )
            try:
                outcome = ensure_outcome(self.executor.execute(assignment))
            except Exception as exc:
                reason = f"executor raised {exc.__class__.__name__}: {exc}"
                if self.unexpected_exceptions_retryable:
                    outcome = RetryableFailure("executor_exception", reason)
                else:
                    outcome = PermanentFailure("executor_exception", reason)

            if isinstance(outcome, Completed):
                if json_size(outcome.result) > MAX_INLINE_RESULT_BYTES:
                    outcome = PermanentFailure(
                        "result_too_large",
                        "executor result exceeds the inline coordination limit",
                    )
                else:
                    payload = {**lifecycle, "summary": outcome.summary}
                    if outcome.result:
                        payload["result"] = mutable_json(outcome.result)
                    self._publish_while_owned(
                        assignment_id,
                        "task.completed",
                        payload,
                        caused_by=assignment.assignment_event_id,
                        idempotency_key=f"completed:{assignment_id}",
                    )
                    return

            if isinstance(outcome, Blocked):
                self._publish_while_owned(
                    assignment_id,
                    "task.blocked",
                    {**lifecycle, "reason": outcome.reason},
                    caused_by=assignment.assignment_event_id,
                    idempotency_key=f"blocked:{assignment_id}",
                )
                return

            retryable = isinstance(outcome, RetryableFailure)
            self._publish_while_owned(
                assignment_id,
                "task.attempt_failed",
                {
                    **lifecycle,
                    "failure_code": outcome.code,
                    "reason": outcome.reason,
                    "retryable": retryable,
                },
                caused_by=assignment.assignment_event_id,
                idempotency_key=f"attempt-failed:{assignment_id}",
            )
        finally:
            with self._lock:
                self._owned.discard(assignment_id)
                self._futures.pop(assignment_id, None)

    def _publish_while_owned(
        self,
        assignment_id: str,
        topic: str,
        payload: dict,
        *,
        caused_by: int,
        idempotency_key: str,
    ) -> Optional[dict]:
        while self._owns(assignment_id) and not self.stop_event.is_set():
            try:
                return self.bus.publish(
                    topic,
                    payload,
                    caused_by=caused_by,
                    idempotency_key=idempotency_key,
                )
            except httpx.HTTPStatusError as exc:
                if 400 <= exc.response.status_code < 500:
                    self.log(
                        f"[{self.name}] bus rejected {topic} with "
                        f"HTTP {exc.response.status_code}; stopping this instance"
                    )
                    self.stop_event.set()
                    return None
                self.log(
                    f"[{self.name}] publish failed ({exc.__class__.__name__}); "
                    f"retrying {topic}"
                )
                if self.stop_event.wait(self.publish_retry_seconds):
                    break
            except httpx.HTTPError as exc:
                self.log(
                    f"[{self.name}] publish failed ({exc.__class__.__name__}); "
                    f"retrying {topic}"
                )
                if self.stop_event.wait(self.publish_retry_seconds):
                    break
        return None

    def _owns(self, assignment_id: str) -> bool:
        with self._lock:
            return assignment_id in self._owned

    def _revoke(self, assignment_id: object) -> None:
        if not isinstance(assignment_id, str):
            return
        with self._lock:
            if assignment_id not in self._owned:
                return
            self._owned.discard(assignment_id)
            future = self._futures.get(assignment_id)
        cancelled_before_start = False
        if future is not None:
            cancelled_before_start = future.cancel()
            if cancelled_before_start:
                with self._lock:
                    self._futures.pop(assignment_id, None)
        if not cancelled_before_start:
            cancel = getattr(self.executor, "cancel", None)
            if callable(cancel):
                cancel(assignment_id)

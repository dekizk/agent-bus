"""Framework-neutral executor contract and reference adapters."""

from __future__ import annotations

import json
import subprocess
import threading
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Callable, Mapping, Optional, Protocol, Sequence, Union

DEFAULT_MAX_PROTOCOL_BYTES = 64 * 1024


def _positive_int(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _nonnegative_int(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _nonempty_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def mutable_json(value: Any) -> Any:
    """Return a JSON-serializable mutable copy of an immutable JSON value."""
    return _thaw(value)


def _immutable_json_object(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a JSON object")
    try:
        normalized = json.loads(
            json.dumps(dict(value), allow_nan=False, separators=(",", ":"))
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must contain JSON-compatible values") from exc
    return _freeze(normalized)


def _immutable_decisions(
    value: object,
) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, (list, tuple)):
        raise ValueError("decisions must be a JSON array")
    try:
        normalized = json.loads(
            json.dumps(list(value), allow_nan=False, separators=(",", ":"))
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("decisions must contain JSON-compatible values") from exc
    for index, decision in enumerate(normalized):
        if not isinstance(decision, dict):
            raise ValueError(f"decisions[{index}] must be an object")
        if set(decision) != {
            "event_id",
            "actor",
            "assignment_id",
            "decision_id",
            "decision",
        }:
            raise ValueError(f"decisions[{index}] has an invalid shape")
        _positive_int(decision["event_id"], f"decisions[{index}].event_id")
        for field_name in ("actor", "assignment_id", "decision_id"):
            _nonempty_string(
                decision[field_name],
                f"decisions[{index}].{field_name}",
            )
    return _freeze(normalized)


def json_size(value: object) -> int:
    return len(
        json.dumps(
            _thaw(value),
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )


@dataclass(frozen=True)
class AssignmentContext:
    """Immutable input presented to every executor implementation."""

    correlation_id: Optional[str]
    task_id: int
    assignment_id: str
    assignment_event_id: int
    attempt: int
    goal: str
    context: Mapping[str, Any] = field(default_factory=dict)
    required_capabilities: tuple[str, ...] = ()
    max_retries: Optional[int] = None
    retryable_failures: int = 0
    assignee: str = ""
    worker_instance_id: str = ""
    ownership_mode: str = "controlled"
    ownership_owner: str = "agent-bus"
    external_origin: Optional[Mapping[str, Any]] = None
    decisions: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        _positive_int(self.task_id, "task_id")
        _positive_int(self.assignment_event_id, "assignment_event_id")
        _positive_int(self.attempt, "attempt")
        _nonempty_string(self.assignment_id, "assignment_id")
        _nonempty_string(self.goal, "goal")
        _nonempty_string(self.assignee, "assignee")
        _nonempty_string(self.worker_instance_id, "worker_instance_id")
        if self.correlation_id is not None:
            _nonempty_string(self.correlation_id, "correlation_id")
        if self.max_retries is not None:
            _nonnegative_int(self.max_retries, "max_retries")
        _nonnegative_int(self.retryable_failures, "retryable_failures")
        if not isinstance(self.required_capabilities, tuple) or not all(
            isinstance(item, str) and item.strip()
            for item in self.required_capabilities
        ):
            raise ValueError("required_capabilities must be a tuple of strings")
        if self.ownership_mode not in {"controlled", "canary"}:
            raise ValueError("assigned work must use controlled or canary mode")
        if self.ownership_owner != "agent-bus":
            raise ValueError("assigned work must be owned by agent-bus")
        object.__setattr__(
            self,
            "context",
            _immutable_json_object(self.context, "context"),
        )
        object.__setattr__(self, "decisions", _immutable_decisions(self.decisions))
        if self.external_origin is not None:
            object.__setattr__(
                self,
                "external_origin",
                _immutable_json_object(self.external_origin, "external_origin"),
            )

    @classmethod
    def from_event(cls, event: Mapping[str, Any]) -> "AssignmentContext":
        payload = event.get("payload")
        if not isinstance(payload, Mapping):
            raise ValueError("assignment event payload must be an object")
        retry_policy = payload.get("retry_policy", {})
        if not isinstance(retry_policy, Mapping):
            raise ValueError("assignment retry_policy must be an object")
        ownership = payload.get(
            "ownership",
            {"mode": "controlled", "owner": "agent-bus"},
        )
        if not isinstance(ownership, Mapping):
            raise ValueError("assignment ownership must be an object")
        raw_capabilities = payload.get("required_capabilities", [])
        if not isinstance(raw_capabilities, list):
            raise ValueError("required_capabilities must be a list")
        return cls(
            correlation_id=event.get("correlation_id"),
            task_id=payload.get("task_id"),
            assignment_id=payload.get("assignment_id"),
            assignment_event_id=event.get("id"),
            attempt=payload.get("attempt"),
            goal=payload.get("goal") or payload.get("title"),
            context=payload.get("context", {}),
            decisions=payload.get("decisions", []),
            required_capabilities=tuple(raw_capabilities),
            max_retries=retry_policy.get("max_retries"),
            retryable_failures=payload.get("retryable_failures", 0),
            assignee=payload.get("assignee"),
            worker_instance_id=payload.get("worker_instance_id"),
            ownership_mode=ownership.get("mode"),
            ownership_owner=ownership.get("owner"),
            external_origin=payload.get("external_origin"),
        )

    def to_dict(self) -> dict:
        result = {
            "correlation_id": self.correlation_id,
            "task_id": self.task_id,
            "assignment_id": self.assignment_id,
            "assignment_event_id": self.assignment_event_id,
            "attempt": self.attempt,
            "goal": self.goal,
            "context": _thaw(self.context),
            "decisions": _thaw(self.decisions),
            "required_capabilities": list(self.required_capabilities),
            "retry_policy": {"max_retries": self.max_retries},
            "retryable_failures": self.retryable_failures,
            "assignee": self.assignee,
            "worker_instance_id": self.worker_instance_id,
            "ownership": {
                "mode": self.ownership_mode,
                "owner": self.ownership_owner,
            },
        }
        if self.external_origin is not None:
            result["external_origin"] = _thaw(self.external_origin)
        return result


@dataclass(frozen=True)
class Completed:
    summary: str
    result: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _nonempty_string(self.summary, "summary")
        object.__setattr__(
            self,
            "result",
            _immutable_json_object(self.result, "result"),
        )


@dataclass(frozen=True)
class Blocked:
    reason: str

    def __post_init__(self) -> None:
        _nonempty_string(self.reason, "reason")


@dataclass(frozen=True)
class RetryableFailure:
    code: str
    reason: str

    def __post_init__(self) -> None:
        _nonempty_string(self.code, "code")
        _nonempty_string(self.reason, "reason")


@dataclass(frozen=True)
class PermanentFailure:
    code: str
    reason: str

    def __post_init__(self) -> None:
        _nonempty_string(self.code, "code")
        _nonempty_string(self.reason, "reason")


Outcome = Union[Completed, Blocked, RetryableFailure, PermanentFailure]
OUTCOME_TYPES = (Completed, Blocked, RetryableFailure, PermanentFailure)


class Executor(Protocol):
    def execute(self, assignment: AssignmentContext) -> Outcome:
        """Perform one assignment and return one explicit lifecycle outcome."""


def ensure_outcome(value: object) -> Outcome:
    if not isinstance(value, OUTCOME_TYPES):
        raise TypeError(
            "executor must return Completed, Blocked, RetryableFailure, "
            "or PermanentFailure"
        )
    return value


def outcome_from_dict(value: object) -> Outcome:
    if not isinstance(value, dict):
        raise ValueError("executor output must be a JSON object")
    status = value.get("status")
    if status == "completed":
        return Completed(
            summary=value.get("summary"),
            result=value.get("result", {}),
        )
    if status == "blocked":
        return Blocked(reason=value.get("reason"))
    if status == "retryable_failure":
        return RetryableFailure(
            code=value.get("code"),
            reason=value.get("reason"),
        )
    if status == "permanent_failure":
        return PermanentFailure(
            code=value.get("code"),
            reason=value.get("reason"),
        )
    raise ValueError(f"unknown executor outcome status: {status!r}")


class InProcessExecutor:
    """Adapt a Python callable or an object exposing ``run(assignment)``."""

    def __init__(self, target: object):
        run_method = getattr(target, "run", None)
        if callable(run_method):
            self._call: Callable[[AssignmentContext], object] = run_method
        elif callable(target):
            self._call = target
        else:
            raise TypeError("target must be callable or expose a callable run method")

    def execute(self, assignment: AssignmentContext) -> Outcome:
        return ensure_outcome(self._call(assignment))


class SubprocessExecutor:
    """Run a CLI executor using bounded JSON input and output contracts.

    Exit code 0 must produce one JSON outcome on stdout. Exit code 75 is a
    retryable process failure; other non-zero exits are permanent failures.
    Shell execution is deliberately disabled.
    """

    def __init__(
        self,
        command: Sequence[str],
        *,
        timeout: Optional[float] = None,
        max_protocol_bytes: int = DEFAULT_MAX_PROTOCOL_BYTES,
    ):
        if (
            not isinstance(command, Sequence)
            or isinstance(command, (str, bytes))
            or not command
            or not all(isinstance(item, str) and item for item in command)
        ):
            raise ValueError("command must be a non-empty sequence of strings")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        _positive_int(max_protocol_bytes, "max_protocol_bytes")
        self.command = tuple(command)
        self.timeout = timeout
        self.max_protocol_bytes = max_protocol_bytes
        self._lock = threading.Lock()
        self._processes: dict[str, subprocess.Popen] = {}
        self._cancelled: set[str] = set()
        self._closed = False

    def execute(self, assignment: AssignmentContext) -> Outcome:
        input_bytes = json.dumps(
            assignment.to_dict(),
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(input_bytes) > self.max_protocol_bytes:
            return PermanentFailure(
                "assignment_too_large",
                "serialized assignment exceeds the subprocess protocol limit",
            )

        with self._lock:
            if self._closed:
                return PermanentFailure(
                    "executor_closed",
                    "subprocess executor is closed",
                )

        try:
            process = subprocess.Popen(
                self.command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
            )
        except OSError as exc:
            return PermanentFailure(
                "subprocess_start_failed",
                f"could not start subprocess executor: {exc}",
            )

        with self._lock:
            if self._closed or assignment.assignment_id in self._cancelled:
                self._terminate(process)
                self._cancelled.discard(assignment.assignment_id)
                return RetryableFailure(
                    "subprocess_cancelled",
                    "subprocess execution lost assignment ownership",
                )
            self._processes[assignment.assignment_id] = process

        stdout = bytearray()
        stderr = bytearray()
        overflow = threading.Event()
        readers = (
            threading.Thread(
                target=self._read_bounded_pipe,
                args=(process.stdout, stdout, overflow, process),
                daemon=True,
            ),
            threading.Thread(
                target=self._read_bounded_pipe,
                args=(process.stderr, stderr, overflow, process),
                daemon=True,
            ),
        )
        for reader in readers:
            reader.start()
        timed_out = False
        io_error: Optional[OSError] = None
        try:
            try:
                if process.stdin is not None:
                    process.stdin.write(input_bytes)
                    process.stdin.close()
            except BrokenPipeError:
                pass
            try:
                process.wait(timeout=self.timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                self._terminate(process)
        except OSError as exc:
            io_error = exc
            self._terminate(process)
        finally:
            for reader in readers:
                reader.join(timeout=1)
            with self._lock:
                self._processes.pop(assignment.assignment_id, None)

        with self._lock:
            cancelled = assignment.assignment_id in self._cancelled
            self._cancelled.discard(assignment.assignment_id)
        if cancelled:
            return RetryableFailure(
                "subprocess_cancelled",
                "subprocess execution lost assignment ownership",
            )
        if io_error is not None:
            return RetryableFailure(
                "subprocess_io_failed",
                f"subprocess I/O failed after start: {io_error}",
            )
        if timed_out:
            return RetryableFailure(
                "subprocess_timeout",
                "subprocess executor exceeded its timeout",
            )
        if overflow.is_set():
            return PermanentFailure(
                "protocol_output_too_large",
                "subprocess output exceeds the protocol limit",
            )

        stderr_text = bytes(stderr).decode("utf-8", errors="replace").strip()
        if process.returncode != 0:
            reason = stderr_text or f"subprocess exited with {process.returncode}"
            if process.returncode == 75:
                return RetryableFailure("subprocess_exit", reason)
            return PermanentFailure("subprocess_exit", reason)
        try:
            decoded = json.loads(bytes(stdout).decode("utf-8"))
            return outcome_from_dict(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            return PermanentFailure(
                "invalid_executor_output",
                f"subprocess returned invalid JSON outcome: {exc}",
            )

    def cancel(self, assignment_id: str) -> None:
        with self._lock:
            process = self._processes.get(assignment_id)
            self._cancelled.add(assignment_id)
        if process is not None:
            self._terminate(process)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            processes = list(self._processes.items())
            self._cancelled.update(self._processes)
        for _, process in processes:
            self._terminate(process)

    def _read_bounded_pipe(
        self,
        pipe,
        output: bytearray,
        overflow: threading.Event,
        process: subprocess.Popen,
    ) -> None:
        if pipe is None:
            return
        try:
            while True:
                chunk = pipe.read(4096)
                if not chunk:
                    return
                remaining = self.max_protocol_bytes + 1 - len(output)
                if remaining > 0:
                    output.extend(chunk[:remaining])
                if len(output) > self.max_protocol_bytes or len(chunk) > remaining:
                    overflow.set()
                    self._terminate(process)
                    return
        finally:
            pipe.close()

    @staticmethod
    def _terminate(process: subprocess.Popen) -> None:
        if process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=1)

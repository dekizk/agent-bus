"""Local onboarding configuration shared by agent-bus process commands."""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional


LOCAL_CONFIG_VERSION = 1
DEFAULT_LOCAL_CONFIG = "agent-bus.local.json"


@dataclass(frozen=True)
class LocalConfig:
    path: Path
    host: str
    port: int
    database_path: Path
    worker_lease_seconds: float
    default_workflow_max_active_assignments: Optional[int] = None
    default_agent_max_active_assignments: Optional[int] = None
    default_workflow_max_total_tokens: Optional[int] = None
    default_workflow_max_total_cost_usd: Optional[float] = None
    default_workflow_max_attempts: Optional[int] = None
    default_workflow_max_wall_clock_seconds: Optional[float] = None
    default_workflow_reserve_tokens_per_assignment: int = 0
    default_workflow_reserve_cost_usd_per_assignment: float = 0.0

    @property
    def bus_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @classmethod
    def from_file(cls, path: str | Path = DEFAULT_LOCAL_CONFIG) -> "LocalConfig":
        config_path = Path(path).expanduser().resolve()
        try:
            value = json.loads(config_path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise ValueError(
                f"local config not found: {config_path}; run agent-bus init first"
            ) from exc
        except json.JSONDecodeError as exc:
            raise ValueError(f"local config is not valid JSON: {exc}") from exc
        required_keys = {"schema_version", "bus", "worker_lease_seconds"}
        allowed_keys = required_keys | {
            "default_workflow_max_active_assignments",
            "default_agent_max_active_assignments",
            "default_workflow_max_total_tokens",
            "default_workflow_max_total_cost_usd",
            "default_workflow_max_attempts",
            "default_workflow_max_wall_clock_seconds",
            "default_workflow_reserve_tokens_per_assignment",
            "default_workflow_reserve_cost_usd_per_assignment",
        }
        if (
            not isinstance(value, Mapping)
            or not required_keys.issubset(value)
            or not set(value).issubset(allowed_keys)
        ):
            raise ValueError("local config has an invalid shape")
        if value["schema_version"] != LOCAL_CONFIG_VERSION:
            raise ValueError(
                f"unsupported local config schema_version {value['schema_version']!r}"
            )
        bus = value["bus"]
        if not isinstance(bus, Mapping) or set(bus) != {"host", "port", "database_path"}:
            raise ValueError("local config bus must contain host, port, and database_path")
        host = bus["host"]
        if host not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("local onboarding config must bind to a loopback host")
        port = bus["port"]
        if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
            raise ValueError("local config port must be between 1 and 65535")
        raw_database = bus["database_path"]
        if not isinstance(raw_database, str) or not raw_database.strip():
            raise ValueError("local config database_path must be a non-empty string")
        database_path = Path(raw_database).expanduser()
        if not database_path.is_absolute():
            database_path = config_path.parent / database_path
        lease = value["worker_lease_seconds"]
        if (
            not isinstance(lease, (int, float))
            or isinstance(lease, bool)
            or not math.isfinite(lease)
            or lease <= 0
        ):
            raise ValueError("worker_lease_seconds must be positive")
        workflow_limit = value.get("default_workflow_max_active_assignments")
        if workflow_limit is not None and (
            not isinstance(workflow_limit, int)
            or isinstance(workflow_limit, bool)
            or workflow_limit <= 0
        ):
            raise ValueError(
                "default_workflow_max_active_assignments must be null or positive"
            )
        agent_limit = value.get("default_agent_max_active_assignments")
        token_limit = value.get("default_workflow_max_total_tokens")
        attempt_limit = value.get("default_workflow_max_attempts")
        for field_name, field_value in (
            ("default_agent_max_active_assignments", agent_limit),
            ("default_workflow_max_total_tokens", token_limit),
            ("default_workflow_max_attempts", attempt_limit),
        ):
            if field_value is not None and (
                not isinstance(field_value, int)
                or isinstance(field_value, bool)
                or field_value <= 0
            ):
                raise ValueError(f"{field_name} must be null or positive")
        cost_limit = value.get("default_workflow_max_total_cost_usd")
        wall_limit = value.get("default_workflow_max_wall_clock_seconds")
        for field_name, field_value in (
            ("default_workflow_max_total_cost_usd", cost_limit),
            ("default_workflow_max_wall_clock_seconds", wall_limit),
        ):
            if field_value is not None and (
                not isinstance(field_value, (int, float))
                or isinstance(field_value, bool)
                or not math.isfinite(field_value)
                or field_value <= 0
            ):
                raise ValueError(f"{field_name} must be null or positive")
        reserve_tokens = value.get(
            "default_workflow_reserve_tokens_per_assignment", 0
        )
        if (
            not isinstance(reserve_tokens, int)
            or isinstance(reserve_tokens, bool)
            or reserve_tokens < 0
        ):
            raise ValueError(
                "default_workflow_reserve_tokens_per_assignment must be non-negative"
            )
        reserve_cost = value.get(
            "default_workflow_reserve_cost_usd_per_assignment", 0.0
        )
        if (
            not isinstance(reserve_cost, (int, float))
            or isinstance(reserve_cost, bool)
            or not math.isfinite(reserve_cost)
            or reserve_cost < 0
        ):
            raise ValueError(
                "default_workflow_reserve_cost_usd_per_assignment must be non-negative"
            )
        if token_limit is not None and not 0 < reserve_tokens <= token_limit:
            raise ValueError(
                "a finite default token budget requires a positive reservation no larger than the budget"
            )
        if cost_limit is not None and not 0 < reserve_cost <= cost_limit:
            raise ValueError(
                "a finite default cost budget requires a positive reservation no larger than the budget"
            )
        return cls(
            path=config_path,
            host=host,
            port=port,
            database_path=database_path.resolve(),
            worker_lease_seconds=float(lease),
            default_workflow_max_active_assignments=workflow_limit,
            default_agent_max_active_assignments=agent_limit,
            default_workflow_max_total_tokens=token_limit,
            default_workflow_max_total_cost_usd=(
                float(cost_limit) if cost_limit is not None else None
            ),
            default_workflow_max_attempts=attempt_limit,
            default_workflow_max_wall_clock_seconds=(
                float(wall_limit) if wall_limit is not None else None
            ),
            default_workflow_reserve_tokens_per_assignment=reserve_tokens,
            default_workflow_reserve_cost_usd_per_assignment=float(reserve_cost),
        )

    def apply_environment(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        os.environ["AGENT_BUS_URL"] = self.bus_url
        os.environ["AGENT_BUS_DB_PATH"] = str(self.database_path)
        os.environ["AGENT_BUS_WORKER_LEASE_SECONDS"] = str(self.worker_lease_seconds)
        os.environ["AGENT_BUS_DEFAULT_WORKFLOW_MAX_ACTIVE_ASSIGNMENTS"] = (
            str(self.default_workflow_max_active_assignments)
            if self.default_workflow_max_active_assignments is not None
            else "unbounded"
        )
        optional_values = {
            "AGENT_BUS_DEFAULT_AGENT_MAX_ACTIVE_ASSIGNMENTS": (
                self.default_agent_max_active_assignments
            ),
            "AGENT_BUS_DEFAULT_WORKFLOW_MAX_TOTAL_TOKENS": (
                self.default_workflow_max_total_tokens
            ),
            "AGENT_BUS_DEFAULT_WORKFLOW_MAX_TOTAL_COST_USD": (
                self.default_workflow_max_total_cost_usd
            ),
            "AGENT_BUS_DEFAULT_WORKFLOW_MAX_ATTEMPTS": (
                self.default_workflow_max_attempts
            ),
            "AGENT_BUS_DEFAULT_WORKFLOW_MAX_WALL_CLOCK_SECONDS": (
                self.default_workflow_max_wall_clock_seconds
            ),
        }
        for name, configured in optional_values.items():
            os.environ[name] = (
                str(configured) if configured is not None else "unbounded"
            )
        os.environ[
            "AGENT_BUS_DEFAULT_WORKFLOW_RESERVE_TOKENS_PER_ASSIGNMENT"
        ] = str(self.default_workflow_reserve_tokens_per_assignment)
        os.environ[
            "AGENT_BUS_DEFAULT_WORKFLOW_RESERVE_COST_USD_PER_ASSIGNMENT"
        ] = str(self.default_workflow_reserve_cost_usd_per_assignment)


def initialize_local_config(directory: str | Path) -> Path:
    root = Path(directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    path = root / DEFAULT_LOCAL_CONFIG
    value = {
        "schema_version": LOCAL_CONFIG_VERSION,
        "bus": {
            "host": "127.0.0.1",
            "port": 8765,
            "database_path": ".agent-bus/events.db",
        },
        "worker_lease_seconds": 20,
        "default_workflow_max_active_assignments": 4,
        "default_agent_max_active_assignments": None,
        "default_workflow_max_total_tokens": None,
        "default_workflow_max_total_cost_usd": None,
        "default_workflow_max_attempts": None,
        "default_workflow_max_wall_clock_seconds": None,
        "default_workflow_reserve_tokens_per_assignment": 0,
        "default_workflow_reserve_cost_usd_per_assignment": 0.0,
    }
    encoded = json.dumps(value, indent=2, sort_keys=True) + "\n"
    try:
        with path.open("x", encoding="utf-8") as target:
            target.write(encoded)
    except FileExistsError as exc:
        raise ValueError(f"refusing to overwrite existing local config: {path}") from exc
    (root / ".agent-bus").mkdir(exist_ok=True)
    return path

"""Local onboarding configuration shared by agent-bus process commands."""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


LOCAL_CONFIG_VERSION = 1
DEFAULT_LOCAL_CONFIG = "agent-bus.local.json"


@dataclass(frozen=True)
class LocalConfig:
    path: Path
    host: str
    port: int
    database_path: Path
    worker_lease_seconds: float

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
        if not isinstance(value, Mapping) or set(value) != {
            "schema_version",
            "bus",
            "worker_lease_seconds",
        }:
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
        return cls(
            path=config_path,
            host=host,
            port=port,
            database_path=database_path.resolve(),
            worker_lease_seconds=float(lease),
        )

    def apply_environment(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        os.environ["AGENT_BUS_URL"] = self.bus_url
        os.environ["AGENT_BUS_DB_PATH"] = str(self.database_path)
        os.environ["AGENT_BUS_WORKER_LEASE_SECONDS"] = str(self.worker_lease_seconds)


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
    }
    encoded = json.dumps(value, indent=2, sort_keys=True) + "\n"
    try:
        with path.open("x", encoding="utf-8") as target:
            target.write(encoded)
    except FileExistsError as exc:
        raise ValueError(f"refusing to overwrite existing local config: {path}") from exc
    (root / ".agent-bus").mkdir(exist_ok=True)
    return path

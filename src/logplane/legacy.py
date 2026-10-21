"""Clear failures for settings left over from the pre-0.13 ``agent-bus`` name.

logplane 0.13 renamed every environment variable from ``AGENT_BUS_*`` to
``LOGPLANE_*`` and the local config from ``agent-bus.local.json`` to
``logplane.local.json``. Old names are rejected rather than ignored: a
silently ignored ``AGENT_BUS_TOKEN`` would start an unauthenticated bus.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping, Optional

LEGACY_ENV_PREFIX = "AGENT_BUS_"
LEGACY_LOCAL_CONFIG = "agent-bus.local.json"


def legacy_environment_error(environ: Optional[Mapping[str, str]] = None) -> Optional[str]:
    environ = os.environ if environ is None else environ
    names = sorted(name for name in environ if name.startswith(LEGACY_ENV_PREFIX))
    if not names:
        return None
    renamed = ", ".join(f"{name} -> LOGPLANE_{name[len(LEGACY_ENV_PREFIX):]}" for name in names)
    return (
        "found pre-0.13 environment variables that logplane no longer reads; "
        f"rename them and unset the old names: {renamed}"
    )


def check_legacy_environment(environ: Optional[Mapping[str, str]] = None) -> None:
    message = legacy_environment_error(environ)
    if message:
        raise RuntimeError(message)


def legacy_config_hint(path: Path) -> Optional[str]:
    """Explain how to migrate when only the old config file is present."""
    legacy = path.with_name(LEGACY_LOCAL_CONFIG)
    if path.exists() or not legacy.exists():
        return None
    return (
        f"found {legacy}, the pre-0.13 config name; rename it to {path.name}. "
        "Its database_path may still point at .agent-bus/, which keeps working, "
        "or move that directory to .logplane/ and update the path"
    )

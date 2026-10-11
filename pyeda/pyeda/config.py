"""
pyeda.config — Configuration loading and validation.

Reads from a YAML file (``pyeda.yml`` by default) or from a plain dict.
All keys are optional; sensible defaults are applied.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from pyeda.exceptions import ConfigError


# ---------------------------------------------------------------------------
# Sub-configs
# ---------------------------------------------------------------------------

@dataclass
class HandlerConfig:
    concurrency:     int   = 3
    retry_attempts:  int   = 3
    retry_delay_s:   float = 1.0
    retry_backoff:   Literal["fixed", "exponential"] = "exponential"
    timeout_s:       Optional[float] = None


@dataclass
class BusConfig:
    backend:      Literal["memory", "sqlite"] = "memory"
    queue_size:   int  = 1_000
    journal_path: str  = ".pyeda/journal.db"
    # How long stop() waits for queued and running handlers; None = wait forever
    shutdown_timeout_s: Optional[float] = 30.0


@dataclass
class UIConfig:
    enabled: bool = True
    host:    str  = "127.0.0.1"
    port:    int  = 7373


@dataclass
class PyEDAConfig:
    bus:      BusConfig                   = field(default_factory=BusConfig)
    ui:       UIConfig                    = field(default_factory=UIConfig)
    defaults: HandlerConfig               = field(default_factory=HandlerConfig)
    middleware: List[str]                 = field(default_factory=lambda: ["logging", "retry"])
    # Per-handler overrides — only the keys that were explicitly set
    handlers:   Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def for_handler(self, name: str) -> HandlerConfig:
        """Return handler-specific config merged over defaults."""
        override = self.handlers.get(name)
        if not override:
            return self.defaults
        if isinstance(override, HandlerConfig):
            return override
        return replace(self.defaults, **override)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

_DEFAULT_PATHS = ["pyeda.yml", "pyeda.yaml", ".pyeda.yml"]


def load_config(source: Optional[str | Path | Dict[str, Any]] = None) -> PyEDAConfig:
    """
    Load configuration from a YAML file path, a dict, or auto-discover.

    Priority:
      1. Explicit ``source`` argument.
      2. ``PYEDA_CONFIG`` environment variable.
      3. Auto-discover: ``pyeda.yml``, ``pyeda.yaml``, ``.pyeda.yml``.
      4. All defaults.
    """
    if source is None:
        env = os.environ.get("PYEDA_CONFIG")
        if env:
            source = Path(env)
        else:
            for p in _DEFAULT_PATHS:
                if Path(p).exists():
                    source = Path(p)
                    break

    raw: Dict[str, Any] = {}

    if isinstance(source, (str, Path)):
        raw = _load_yaml(Path(source))
    elif isinstance(source, dict):
        raw = source
    # else: all defaults

    return _parse(raw)


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
    except ImportError as e:
        raise ImportError(
            "pyyaml is required to load YAML configs: pip install pyyaml"
        ) from e

    with path.open() as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected a YAML mapping at the top level of {path}.")
    return data


def _parse(raw: Dict[str, Any]) -> PyEDAConfig:
    # --- bus ---
    bus_raw = raw.get("bus", {})
    bus = BusConfig(
        backend      = bus_raw.get("backend",    "memory"),
        queue_size   = bus_raw.get("queue_size", 1_000),
        journal_path = bus_raw.get("journal_path", ".pyeda/journal.db"),
        shutdown_timeout_s = bus_raw.get("shutdown_timeout_s", 30.0),
    )

    # --- ui ---
    ui_raw = raw.get("ui", {})
    ui = UIConfig(
        enabled = ui_raw.get("enabled", True),
        host    = ui_raw.get("host", "127.0.0.1"),
        port    = ui_raw.get("port", 7373),
    )

    # --- defaults ---
    def_raw = raw.get("defaults", {})
    defaults = _parse_handler_cfg(def_raw)

    # --- middleware ---
    middleware = raw.get("middleware", ["logging", "retry"])

    # --- per-handler overrides ---
    known = {f.name for f in fields(HandlerConfig)}
    handlers: Dict[str, Dict[str, Any]] = {}
    for name, cfg in raw.get("handlers", {}).items():
        override = dict(cfg or {})
        unknown = set(override) - known
        if unknown:
            raise ConfigError(
                f"Unknown option(s) for handler '{name}': {sorted(unknown)}"
            )
        handlers[name] = override

    return PyEDAConfig(
        bus=bus,
        ui=ui,
        defaults=defaults,
        middleware=middleware,
        handlers=handlers,
    )


def _parse_handler_cfg(raw: Dict[str, Any]) -> HandlerConfig:
    return HandlerConfig(
        concurrency    = raw.get("concurrency",    3),
        retry_attempts = raw.get("retry_attempts", 3),
        retry_delay_s  = raw.get("retry_delay_s",  1.0),
        retry_backoff  = raw.get("retry_backoff",  "exponential"),
        timeout_s      = raw.get("timeout_s",      None),
    )

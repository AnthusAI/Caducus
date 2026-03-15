"""Caducus configuration: YAML merge, env, and CLI overrides."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Merge override into base recursively. Override wins for scalars."""
    out = dict(base)
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _set_dotted(root: dict[str, Any], key: str, value: Any) -> None:
    """Set a dotted key (e.g. biblicus.reinforcement_memory.data_dir) into root."""
    parts = key.split(".")
    cur = root
    for i, part in enumerate(parts[:-1]):
        if part not in cur:
            cur[part] = {}
        cur = cur[part]
    cur[parts[-1]] = value


def _env_substitute_string(s: str) -> str:
    """Replace {{ VAR }} and {{ VAR|default }} in s with env or default."""
    if not isinstance(s, str):
        return s

    def repl(match: re.Match[str]) -> str:
        var = match.group(1).strip()
        pipe = match.group(2)
        default = match.group(3).strip() if pipe else ""
        return os.environ.get(var, default)

    return re.sub(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*(\|\s*([^}]*))?\s*\}\}", repl, s)


def _env_substitute(obj: Any) -> Any:
    """Recursively substitute {{ VAR }} in string values."""
    if isinstance(obj, dict):
        return {k: _env_substitute(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_env_substitute(v) for v in obj]
    if isinstance(obj, str):
        return _env_substitute_string(obj)
    return obj


def load_yaml(path: Path) -> dict[str, Any]:
    """Load a single YAML file. Returns {} if file missing or invalid."""
    import yaml

    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return dict(data) if data else {}
    except Exception:
        return {}


def load_config(
    config_file_paths: list[str] | None = None,
    overrides: list[str] | None = None,
) -> dict[str, Any]:
    """
    Build merged config from YAML files, env, and dotted overrides.

    - config_file_paths: paths to YAML files (later override earlier).
    - overrides: list of "key=value" (dotted keys, e.g. biblicus.reinforcement_memory.data_dir=/path).

    CADUCUS_DATA_DIR and BIBLICUS_* env vars are not auto-injected here;
    CLI can apply them as overrides before calling this, or callers can merge.
    """
    merged: dict[str, Any] = {}
    if config_file_paths:
        for p in config_file_paths:
            data = load_yaml(Path(p))
            merged = _deep_merge(merged, data)
    merged = _env_substitute(merged)
    if overrides:
        for o in overrides:
            if "=" not in o:
                continue
            key, _, value = o.partition("=")
            key = key.strip().strip(".")
            _set_dotted(merged, key, value)
    return merged


def get_data_dir(config: dict[str, Any], cli_data_dir: str | None) -> str:
    """Resolve data_dir: CLI wins, then config, then default."""
    if cli_data_dir:
        return cli_data_dir
    return config.get("data_dir") or os.environ.get("CADUCUS_DATA_DIR") or "./caducus-data"

"""Read a config mapping from JSON or YAML."""

from __future__ import annotations

import json
import os
from typing import Any, Dict


def load_mapping(path: str) -> Dict[str, Any]:
    """Load a mapping from `path`: `.json` with the standard library, else YAML."""
    with open(path, encoding="utf-8") as handle:
        text = handle.read()
    if path.endswith(".json"):
        data = json.loads(text)
    else:
        import yaml

        data = yaml.safe_load(text)
    if not isinstance(data, dict):
        raise ValueError("%s must hold a mapping" % os.path.basename(path))
    return data


def config_path(name: str, directory: str = "") -> str:
    """Resolve a config file name, preferring JSON when the directory has it."""
    directory = directory or os.environ.get("CADUCUS_CONFIG_DIR", "config")
    stem = name.rsplit(".", 1)[0]
    as_json = os.path.join(directory, stem + ".json")
    return as_json if os.path.exists(as_json) else os.path.join(directory, name)

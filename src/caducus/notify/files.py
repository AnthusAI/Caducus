"""Read config mappings: JSON with the standard library, YAML only when PyYAML is installed."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))


def load_mapping(path: str) -> Dict[str, Any]:
    """Load a mapping from `path`: `.json` with the standard library, else YAML."""
    with open(path, encoding="utf-8") as handle:
        text = handle.read()
    if path.endswith(".json"):
        data = json.loads(text)
    else:
        import yaml  # only needed for YAML files; the notify extra does not install it

        data = yaml.safe_load(text)
    if not isinstance(data, dict):
        raise ValueError("%s must hold a mapping" % os.path.basename(path))
    return data


def find_config(name: str, directory: str = "") -> Optional[str]:
    """Find a host config file by name in `directory` (default `CADUCUS_CONFIG_DIR`).

    The JSON form is preferred when both exist. Returns None when there is no such file.
    """
    directory = directory or os.environ.get("CADUCUS_CONFIG_DIR", "")
    if not directory:
        return None
    stem = name.rsplit(".", 1)[0]
    for candidate in (stem + ".json", name):
        path = os.path.join(directory, candidate)
        if os.path.exists(path):
            return path
    return None

"""Assemble the Lambda bundle: the notify package plus its config as JSON.

The Lambda runtime has no PyYAML, so the YAML in `config/` is converted to JSON
here, at synth time, and the code reads the JSON.
"""

from __future__ import annotations

import json
import os
import shutil
from typing import List

import yaml

CONFIG_FILES = ("notification-kinds.yaml", "alarm-catalog.yaml")


def build_bundle(repo_root: str, out_dir: str) -> str:
    """Write `out_dir` afresh and return it."""
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    package = os.path.join(out_dir, "caducus")
    shutil.copytree(
        os.path.join(repo_root, "src", "caducus", "notify"),
        os.path.join(package, "notify"),
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "fakes.py"),
    )
    with open(os.path.join(package, "__init__.py"), "w", encoding="utf-8") as handle:
        handle.write('"""Caducus notifications Lambda bundle."""\n')
    config_dir = os.path.join(out_dir, "config")
    os.makedirs(config_dir)
    for name in CONFIG_FILES:
        with open(os.path.join(repo_root, "config", name), encoding="utf-8") as src:
            data = yaml.safe_load(src)
        target = os.path.join(config_dir, name.rsplit(".", 1)[0] + ".json")
        with open(target, "w", encoding="utf-8") as dst:
            json.dump(data, dst, indent=2, sort_keys=True)
    return out_dir


def files_in(directory: str) -> List[str]:
    found = []
    for root, _, names in os.walk(directory):
        for name in names:
            found.append(os.path.relpath(os.path.join(root, name), directory))
    return sorted(found)

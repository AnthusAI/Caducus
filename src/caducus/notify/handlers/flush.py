"""Run each minute: clearing messages that are due, and groups whose window ended."""

from __future__ import annotations

from typing import Any, Dict


def handler(event: Any = None, context: Any = None) -> Dict[str, int]:
    from ..wiring import runtime

    return {"sent": runtime().router.tick()}

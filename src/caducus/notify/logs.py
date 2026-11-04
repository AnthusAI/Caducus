"""Structured log lines that carry ids and statuses, never addresses."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

ADDRESS_SHAPE = re.compile(r"[^\s@<>\"']+@[^\s@<>\"']+\.[A-Za-z]{2,}")
_MASKED = re.compile(r"\*{3}@")


def contains_address(text: str) -> bool:
    """True when `text` holds something shaped like an email address."""
    return bool(ADDRESS_SHAPE.search(text))


def emit(logger: logging.Logger, message: str, **fields: Any) -> None:
    """Log one JSON line. Refuses a field that looks like an address."""
    line = json.dumps({"message": message, **fields}, sort_keys=True, default=str)
    if contains_address(line):
        raise ValueError("refusing to log an address-shaped value")
    logger.info(line)

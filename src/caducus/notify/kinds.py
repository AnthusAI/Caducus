"""The registry of notification kinds."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple

from .files import PACKAGE_DIR, find_config, load_mapping

CLASSES = ("operator", "admin", "account", "public")
ACK_MODES = ("none", "requested")
AUDIENCES = ("class", "probe")
GROUPING_FIELDS = ("product", "site")


@dataclass(frozen=True)
class Grouping:
    """Events that share `by` and arrive within `window_seconds` become one message."""

    by: Tuple[str, ...]
    window_seconds: int


@dataclass(frozen=True)
class Kind:
    name: str
    klass: str
    default_on: bool
    channels: Tuple[str, ...]
    ack: str = "none"
    grouping: Optional[Grouping] = None
    lifecycle: Optional[str] = None
    audience: str = "class"  # "probe": only the directory's probe mailbox, for heartbeats


class KindRegistry:
    """Looks kinds up by name; an unknown name returns None."""

    def __init__(self, kinds: Mapping[str, Kind]):
        self._kinds = dict(kinds)

    def get(self, name: str) -> Optional[Kind]:
        return self._kinds.get(name)

    def names(self) -> Tuple[str, ...]:
        return tuple(sorted(self._kinds))


def _kind(name: str, raw: Mapping[str, Any]) -> Kind:
    klass = raw.get("class")
    if klass not in CLASSES:
        raise ValueError("kind %s: class must be one of %s" % (name, ", ".join(CLASSES)))
    ack = raw.get("ack", "none")
    if ack not in ACK_MODES:
        raise ValueError("kind %s: ack must be one of %s" % (name, ", ".join(ACK_MODES)))
    audience = raw.get("audience", "class")
    if audience not in AUDIENCES:
        raise ValueError("kind %s: audience must be one of %s" % (name, ", ".join(AUDIENCES)))
    channels = tuple(raw.get("channels") or ())
    if not channels:
        raise ValueError("kind %s: needs at least one channel" % name)
    grouping = None
    if raw.get("grouping"):
        by = tuple(raw["grouping"].get("by") or ())
        unknown = [field for field in by if field not in GROUPING_FIELDS]
        if unknown:
            raise ValueError("kind %s: cannot group by %s" % (name, ", ".join(unknown)))
        grouping = Grouping(by=by, window_seconds=int(raw["grouping"]["windowSeconds"]))
    return Kind(
        name=name,
        klass=klass,
        default_on=bool(raw.get("defaultOn", False)),
        channels=channels,
        ack=ack,
        grouping=grouping,
        lifecycle=raw.get("lifecycle"),
        audience=audience,
    )


def registry_from_mapping(data: Mapping[str, Any]) -> KindRegistry:
    kinds: Dict[str, Kind] = {
        name: _kind(name, raw) for name, raw in (data.get("kinds") or {}).items()
    }
    return KindRegistry(kinds)


BASE_KINDS = os.path.join(PACKAGE_DIR, "data", "notification-kinds.json")


def load_registry(directory: str = "") -> KindRegistry:
    """The package's base kinds, plus any the host adds.

    A host adds or overrides kinds with `notification-kinds.json` (or `.yaml`) in
    `directory`, which defaults to `CADUCUS_CONFIG_DIR`.
    """
    data = load_mapping(BASE_KINDS)
    extra = find_config("notification-kinds.yaml", directory)
    if extra:
        data = {"kinds": {**data["kinds"], **(load_mapping(extra).get("kinds") or {})}}
    return registry_from_mapping(data)

"""Email addresses, networks and the keyed hashes that stand for them."""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import re

MAXIMUM_ADDRESS_CHARACTERS = 254
MAXIMUM_LOCAL_CHARACTERS = 64

_LABEL = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
_LOCAL = r"[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*"
_ADDRESS = re.compile(r"%s@%s(?:\.%s)+" % (_LOCAL, _LABEL, _LABEL))


def normalize(address: str) -> str:
    """Trim and lowercase, so one mailbox has one recipient key."""
    return address.strip().lower()


def is_valid(address: object) -> bool:
    """A plain, normalized address: local part, at sign, a domain with a dot."""
    if not isinstance(address, str) or not 0 < len(address) <= MAXIMUM_ADDRESS_CHARACTERS:
        return False
    if address != normalize(address):
        return False
    local, _, _ = address.partition("@")
    return len(local) <= MAXIMUM_LOCAL_CHARACTERS and _ADDRESS.fullmatch(address) is not None


def domain_of(address: str) -> str:
    return address.rpartition("@")[2]


def network_of(ip: str) -> str:
    """The visitor's network: an IPv4 /24 or an IPv6 /48. Unparseable input is its own network."""
    try:
        parsed = ipaddress.ip_address(ip.strip())
    except ValueError:
        return "unknown"
    prefix = 24 if parsed.version == 4 else 48
    return str(ipaddress.ip_network("%s/%d" % (parsed, prefix), strict=False))


def keyed_hash(key: bytes, value: str) -> str:
    return hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest()[:32]


def mask(address: str) -> str:
    local, _, domain = address.partition("@")
    return "%s***@%s" % (local[:1], domain)

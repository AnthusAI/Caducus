"""Signed, URL-safe tokens: ``v1.<base64url(json)>.<base64url(hmac-sha256)>``.

Claims: ``k`` recipient key, ``p`` purpose (``confirm`` or ``manage``), ``g`` the
profile's generation, ``n`` nonce, ``e`` expiry (epoch seconds). A confirm token
expires with the host's confirm window and is single use (its nonce must be the
profile's current one). A manage token never expires: it dies with the record,
because a new record after an erasure has a new generation.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Optional

VERSION = "v1"
PURPOSES = ("confirm", "manage")
MAXIMUM_TOKEN_CHARACTERS = 2048


class InvalidToken(ValueError):
    """Malformed, tampered, expired, or for another purpose."""


@dataclass(frozen=True)
class Claims:
    key: str
    purpose: str
    generation: str
    nonce: Optional[str] = None
    expires_at: Optional[int] = None


def hkdf_sha256(secret: bytes, info: bytes, salt: bytes = b"caducus", length: int = 32) -> bytes:
    """RFC 5869 extract and expand with SHA-256."""
    prk = hmac.new(salt, secret, hashlib.sha256).digest()
    output, block, counter = b"", b"", 1
    while len(output) < length:
        block = hmac.new(prk, block + info + bytes([counter]), hashlib.sha256).digest()
        output += block
        counter += 1
    return output[:length]


def _encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _decode(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _signature(key: bytes, payload: str) -> bytes:
    return hmac.new(key, ("%s.%s" % (VERSION, payload)).encode("ascii"), hashlib.sha256).digest()


def issue(key: bytes, claims: Claims) -> str:
    if claims.purpose not in PURPOSES:
        raise ValueError("unknown token purpose %r" % claims.purpose)
    body = {"k": claims.key, "p": claims.purpose, "g": claims.generation}
    if claims.nonce is not None:
        body["n"] = claims.nonce
    if claims.expires_at is not None:
        body["e"] = int(claims.expires_at)
    payload = _encode(json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    return "%s.%s.%s" % (VERSION, payload, _encode(_signature(key, payload)))


def verify(key: bytes, token: object, purpose: str, now_seconds: float) -> Claims:
    """The claims of a valid token, else `InvalidToken`. The signature is checked first, in constant time."""
    if not isinstance(token, str) or not 0 < len(token) <= MAXIMUM_TOKEN_CHARACTERS:
        raise InvalidToken("malformed")
    parts = token.split(".")
    if len(parts) != 3 or parts[0] != VERSION:
        raise InvalidToken("malformed")
    try:
        presented = _decode(parts[2])
        body = json.loads(_decode(parts[1]))
    except (ValueError, UnicodeDecodeError) as error:
        raise InvalidToken("malformed") from error
    if not hmac.compare_digest(presented, _signature(key, parts[1])):
        raise InvalidToken("signature")
    if (
        not isinstance(body, dict)
        or body.get("p") != purpose
        or not isinstance(body.get("k"), str)
        or not isinstance(body.get("g"), str)
    ):
        raise InvalidToken("purpose")
    nonce, expires_at = body.get("n"), body.get("e")
    if (nonce is not None and not isinstance(nonce, str)) or (
        expires_at is not None and (isinstance(expires_at, bool) or not isinstance(expires_at, int))
    ):
        raise InvalidToken("claims")
    if expires_at is not None and now_seconds > expires_at:
        raise InvalidToken("expired")
    return Claims(body["k"], purpose, body["g"], nonce, expires_at)

"""HMAC-SHA256 sender authentication (enabled when WEBHOOK_SECRETS is set).

Signature: ``X-Signature: sha256=<hex>`` over ``f"{timestamp}.{raw_body}"``
with the secret of ``X-Source``. The timestamp bounds replay to the
tolerance window; inside the window a replay is just a duplicate and is
absorbed by idempotency.
"""

import hashlib
import hmac
import math
from collections.abc import Mapping


class AuthError(Exception):
    """Request rejected; the message is safe to return to the client."""


def same_secret(expected: str, given: str) -> bool:
    """Constant-time comparison that accepts any header text.

    ``hmac.compare_digest`` raises on non-ASCII ``str``, which would turn a
    bad credential into a 500; comparing bytes avoids that.
    """
    return hmac.compare_digest(expected.encode(), given.encode())


def sign(secret: bytes, timestamp: str, body: bytes) -> str:
    message = timestamp.encode() + b"." + body
    return "sha256=" + hmac.new(secret, message, hashlib.sha256).hexdigest()


def verify(
    *,
    secrets: Mapping[str, bytes],
    source: str | None,
    timestamp: str | None,
    signature: str | None,
    body: bytes,
    now: float,
    tolerance: float,
) -> str:
    """Return the authenticated source or raise :class:`AuthError`."""
    if not source or not timestamp or not signature:
        raise AuthError("missing signature headers")
    secret = secrets.get(source)
    if secret is None:
        raise AuthError("unknown source")
    try:
        sent_at = float(timestamp)
    except ValueError:
        raise AuthError("invalid timestamp") from None
    # NaN fails every comparison, so it would slip past the window check.
    if not math.isfinite(sent_at):
        raise AuthError("invalid timestamp")
    if abs(now - sent_at) > tolerance:
        raise AuthError("timestamp outside tolerance")
    if not same_secret(sign(secret, timestamp, body), signature):
        raise AuthError("invalid signature")
    return source

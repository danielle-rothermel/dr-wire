"""Explicit bounds for one bounded HTTP client.

Nothing in this module is ever persisted. Every field is a runtime
resource bound chosen by the consumer; consumers that record their own
sizing decisions record their own values, not these objects.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

CONNECT_EXCEEDS_TIMEOUT_MSG = (
    "connect_timeout_seconds must not exceed timeout_seconds"
)
IDLE_EXCEEDS_TIMEOUT_MSG = (
    "idle_timeout_seconds must not exceed timeout_seconds"
)
KEEPALIVE_EXCEEDS_CONNECTIONS_MSG = (
    "max_keepalive_connections must not exceed max_connections"
)

_TIMEOUT_FIELDS = (
    "timeout_seconds",
    "connect_timeout_seconds",
    "idle_timeout_seconds",
)
_COUNT_FIELDS = (
    "max_connections",
    "max_keepalive_connections",
    "max_request_bytes",
    "max_response_bytes",
)


@dataclass(frozen=True, slots=True)
class HttpClientConfig:
    """Bound every resource one client may consume, with no defaults.

    Every field is required so that a caller cannot inherit a bound it
    never chose. Timeouts must be positive and finite; counts and byte
    limits must be positive integers.

    This config does not clamp ``connect_timeout_seconds`` or
    ``idle_timeout_seconds`` down to ``timeout_seconds``. Such a
    normalization changes the sizing a consumer recorded, so it belongs
    to the consumer's own policy; here an out-of-order timeout is
    rejected outright.
    """

    timeout_seconds: float
    """Native write and pool timeout bound."""
    connect_timeout_seconds: float
    """Native TCP/TLS connect bound."""
    idle_timeout_seconds: float
    """Native response-read idle bound."""
    max_connections: int
    """Maximum open connections in the client-owned pool."""
    max_keepalive_connections: int
    """Maximum idle connections retained by the client-owned pool."""
    max_request_bytes: int
    """Maximum request-body bytes dispatched."""
    max_response_bytes: int
    """Maximum decompressed response-body bytes read."""

    def __post_init__(self) -> None:
        for field_name in _TIMEOUT_FIELDS:
            value = getattr(self, field_name)
            if not isinstance(value, float | int) or isinstance(value, bool):
                msg = f"{field_name} must be a real number"
                raise TypeError(msg)
            if not math.isfinite(value) or value <= 0:
                msg = f"{field_name} must be positive and finite"
                raise ValueError(msg)
        for field_name in _COUNT_FIELDS:
            value = getattr(self, field_name)
            if not isinstance(value, int) or isinstance(value, bool):
                msg = f"{field_name} must be an integer"
                raise TypeError(msg)
            if value <= 0:
                msg = f"{field_name} must be positive"
                raise ValueError(msg)
        if self.connect_timeout_seconds > self.timeout_seconds:
            raise ValueError(CONNECT_EXCEEDS_TIMEOUT_MSG)
        if self.idle_timeout_seconds > self.timeout_seconds:
            raise ValueError(IDLE_EXCEEDS_TIMEOUT_MSG)
        if self.max_keepalive_connections > self.max_connections:
            raise ValueError(KEEPALIVE_EXCEEDS_CONNECTIONS_MSG)

"""Pure parsing of the header and URL forms the wire path needs."""

from __future__ import annotations

from datetime import UTC
from email.utils import format_datetime, parsedate_to_datetime

import httpx

from dr_http.wire import ParsedRetryAfter

DISPATCH_URL_SCHEMES = frozenset({"http", "https"})


def parse_retry_after(header_value: str | None) -> ParsedRetryAfter | None:
    """Parse one ``Retry-After`` header value into its declared form.

    A bare non-negative decimal integer is delta-seconds; anything else
    is tried as an HTTP-date and accepted only when it carries a
    timezone, because a naive date names no instant. Every unparseable
    value yields ``None``.

    No bound is applied to the header length or to the delta magnitude.
    How far a hint may reach before it stops being credible is an
    evidence policy, so the caller applies its own caps to the parsed
    result.
    """
    if header_value is None:
        return None
    normalized = header_value.strip()
    if normalized.isascii() and normalized.isdigit():
        return ParsedRetryAfter(kind="delta_seconds", value=int(normalized))
    try:
        parsed = parsedate_to_datetime(normalized)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        return None
    return ParsedRetryAfter(
        kind="http_date",
        value=format_datetime(parsed.astimezone(UTC), usegmt=True),
    )


def is_dispatchable_url(url: str) -> bool:
    """Report whether this URL can be dispatched to a network peer.

    A URL failing this check never reaches the wire, so checking it
    before dispatch lets the caller keep a malformed URL inside its own
    typed reporting. This function is total and never raises.
    """
    try:
        parsed = httpx.URL(url)
    except (httpx.InvalidURL, ValueError):
        return False
    return parsed.scheme in DISPATCH_URL_SCHEMES and bool(parsed.host)

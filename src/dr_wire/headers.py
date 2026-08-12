"""Pure parsing of the header and URL forms the wire path needs."""

from __future__ import annotations

from datetime import UTC
from email.utils import format_datetime, parsedate_to_datetime

import httpx

from dr_wire.wire import ParsedRetryAfter

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

    Totality holds over every peer-controlled value, including ones the
    interpreter itself refuses to convert: a digit string longer than
    the integer-conversion digit limit and a date whose year cannot be
    shifted to UTC both yield ``None`` rather than raising.
    """
    if header_value is None:
        return None
    normalized = header_value.strip()
    if normalized.isascii() and normalized.isdigit():
        try:
            return ParsedRetryAfter(
                kind="delta_seconds", value=int(normalized)
            )
        except ValueError:
            # Python caps int(str) at sys.get_int_max_str_digits, so a
            # long enough all-digit header is unconvertible rather than
            # merely large.
            return None
    try:
        parsed = parsedate_to_datetime(normalized)
        normalized_to_utc = parsed.astimezone(UTC)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        return None
    return ParsedRetryAfter(
        kind="http_date",
        value=format_datetime(normalized_to_utc, usegmt=True),
    )


def is_dispatchable_url(url: str) -> bool:
    """Report whether this URL can be dispatched to a network peer.

    A URL failing this check never reaches the wire, so checking it
    before dispatch lets the caller keep a malformed URL inside its own
    typed reporting. Total over ``str`` inputs: any string httpx refuses
    to parse returns ``False``.
    """
    try:
        parsed = httpx.URL(url)
    except (httpx.InvalidURL, ValueError):
        return False
    return parsed.scheme in DISPATCH_URL_SCHEMES and bool(parsed.host)

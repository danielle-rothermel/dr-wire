"""Typed values describing one wire exchange, and their classification.

Nothing in this module is ever persisted. These are in-process values
handed back to the caller, which maps them into whatever vocabulary it
records. Field names and enum member names here are free to change with
the package version; a consumer that stores a value stores its own
literal, derived at its own boundary.

Mapping an ``httpx`` exception onto a failure kind lives here beside the
taxonomy it produces, so the closed set of kinds and the rules that
reach them are read together.
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass
from enum import Enum, StrEnum, auto, unique
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal

import httpx

if TYPE_CHECKING:
    from collections.abc import Mapping


class WireFailureKind(Enum):
    """Name why a wire exchange produced no response.

    These members are never persisted. They exist so a consumer can map
    one closed set of wire conditions onto its own recorded vocabulary,
    and the mapping lives at the consumer's boundary rather than here.
    """

    INVALID_URL = auto()
    """The URL was rejected while building the request; nothing was sent."""
    CONNECT_ERROR = auto()
    """The connection to the peer could not be established."""
    NETWORK_ERROR = auto()
    """A read, write, or close failed on an established connection."""
    REMOTE_PROTOCOL_ERROR = auto()
    """The peer violated the protocol, classically a mid-exchange
    disconnect."""
    LOCAL_PROTOCOL_ERROR = auto()
    """This side violated the protocol, or the exchange could not be
    decoded, addressed, or completed within the redirect limit."""
    TIMEOUT = auto()
    """A native connect or write phase timeout expired."""
    STALLED_RESPONSE = auto()
    """The peer stopped producing response bytes for longer than the
    idle bound."""
    POOL_TIMEOUT = auto()
    """No connection became available in the client-owned pool."""
    REQUEST_TOO_LARGE = auto()
    """The request body exceeded ``max_request_bytes``; nothing was
    sent."""
    RESPONSE_TOO_LARGE = auto()
    """The response body exceeded ``max_response_bytes`` while
    streaming."""
    UNKNOWN = auto()
    """A wire error outside every other named condition."""


@unique
class WireFailureMessage(StrEnum):
    """Static summary text for each family of wire failure.

    These messages never embed peer-controlled detail, so a summary can
    be logged or compared without carrying anything the peer chose. Like
    every other value here they are in-process only.
    """

    TRANSPORT_ERROR = "http transport error"
    TIMEOUT = "http transport timeout"
    INVALID_URL = "url is not dispatchable as http or https"
    REQUEST_TOO_LARGE = "request body exceeds the configured byte limit"
    RESPONSE_TOO_LARGE = "response body exceeds the configured byte limit"


def _timeout_kind(error: httpx.TimeoutException) -> WireFailureKind:
    """Name the timeout phase so local contention stays distinguishable."""
    if isinstance(error, httpx.PoolTimeout):
        return WireFailureKind.POOL_TIMEOUT
    if isinstance(error, httpx.ReadTimeout):
        return WireFailureKind.STALLED_RESPONSE
    return WireFailureKind.TIMEOUT


_HTTP_ERROR_KINDS: Mapping[type[httpx.HTTPError], WireFailureKind] = {
    httpx.RemoteProtocolError: WireFailureKind.REMOTE_PROTOCOL_ERROR,
    httpx.ProtocolError: WireFailureKind.LOCAL_PROTOCOL_ERROR,
    httpx.DecodingError: WireFailureKind.LOCAL_PROTOCOL_ERROR,
    httpx.UnsupportedProtocol: WireFailureKind.LOCAL_PROTOCOL_ERROR,
    httpx.TooManyRedirects: WireFailureKind.LOCAL_PROTOCOL_ERROR,
    httpx.ConnectError: WireFailureKind.CONNECT_ERROR,
    httpx.NetworkError: WireFailureKind.NETWORK_ERROR,
    httpx.ProxyError: WireFailureKind.NETWORK_ERROR,
}


def _http_error_kind(error: httpx.HTTPError) -> WireFailureKind:
    """Classify one httpx wire error.

    The most specific class in the error's MRO wins, so a subclass named
    in the table is reached before the family it belongs to. The table
    itself is genuinely unordered.
    """
    if isinstance(error, httpx.TimeoutException):
        return _timeout_kind(error)
    for cls in type(error).__mro__:
        kind = _HTTP_ERROR_KINDS.get(cls)
        if kind is not None:
            return kind
    return WireFailureKind.UNKNOWN


def _exception_traceback(error: BaseException) -> str:
    return "".join(
        traceback.format_exception(type(error), error, error.__traceback__)
    )


@dataclass(frozen=True, slots=True)
class ParsedRetryAfter:
    """One parsed ``Retry-After`` header, in whichever form it arrived.

    ``delta_seconds`` carries the parsed non-negative integer; ``http_date``
    carries the canonical ``format_datetime`` rendering of a timezone-aware
    HTTP-date in UTC. No cap is applied to either form: bounding how far a
    hint may reach is the consumer's policy.
    """

    kind: Literal["delta_seconds", "http_date"]
    value: int | str


@dataclass(frozen=True, slots=True)
class WireRequest:
    """One request to dispatch, already fully encoded."""

    method: str
    url: str
    headers: Mapping[str, str]
    body: bytes

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "headers", MappingProxyType(dict(self.headers))
        )


@dataclass(frozen=True, slots=True)
class WireResponse:
    """One response received in full, whatever its status.

    Every HTTP status arrives here, including redirects and server
    errors: a status is data, and classifying it is the caller's job.

    ``headers`` is one flat name-to-value mapping, so a header the peer
    sent more than once arrives as a single comma-joined value under one
    lowercased name. That folding is lossy for headers whose repetitions
    are not equivalent to a comma-joined list, ``Set-Cookie`` being the
    standard case: its individual values cannot be recovered from this
    mapping. A consumer that needs each repetition separately needs a
    representation this type does not carry.
    """

    status_code: int
    headers: Mapping[str, str]
    body: bytes
    retry_after: ParsedRetryAfter | None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "headers", MappingProxyType(dict(self.headers))
        )


@dataclass(frozen=True, slots=True)
class WireFailure:
    """One wire exchange that produced no response.

    ``exception_type`` and ``traceback`` are empty for the failures the
    client detects itself rather than catching, namely the byte-bound
    refusals, which instead carry ``observed_bytes``: the body size
    counted before the bound was crossed. Every other kind leaves
    ``observed_bytes`` as ``None`` because no complete body size was
    ever established.

    ``retry_after`` and ``retry_after_header`` are populated only when a
    response head arrived and the body was then refused for size, which
    is the one failure that still saw the peer's headers. Every other
    kind leaves both ``None`` because no response head was ever
    received. The raw header travels beside the parsed value so a
    consumer whose retention policy bounds the header itself can apply
    the same rule here as on a response.
    """

    kind: WireFailureKind
    exception_type: str
    message: str
    traceback: str
    observed_bytes: int | None = None
    retry_after: ParsedRetryAfter | None = None
    retry_after_header: str | None = None

    @classmethod
    def from_error(
        cls,
        kind: WireFailureKind,
        error: BaseException,
        message: str,
    ) -> WireFailure:
        """Build one failure from the exception that produced it.

        ``exception_type`` and ``traceback`` are taken from ``error``,
        while ``message`` stays the caller's static summary so no
        peer-controlled detail reaches it.
        """
        return cls(
            kind=kind,
            exception_type=type(error).__name__,
            message=message,
            traceback=_exception_traceback(error),
        )

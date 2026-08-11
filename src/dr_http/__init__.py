"""One bounded, lifecycle-managed synchronous HTTP client core.

Nothing this package exposes is ever persisted. Every value here is an
in-process description of one wire exchange or one resource bound;
consumers that record wire outcomes map these values into their own
vocabulary at their own boundary.
"""

from importlib.metadata import version

from dr_http.client import (
    CLOSING_OR_CLOSED_MSG,
    OFFLOAD_THREAD_NAME_PREFIX,
    BoundedHttpClient,
)
from dr_http.config import HttpClientConfig
from dr_http.headers import is_dispatchable_url, parse_retry_after
from dr_http.wire import (
    ParsedRetryAfter,
    WireFailure,
    WireFailureKind,
    WireRequest,
    WireResponse,
)

PACKAGE_NAME = "dr-http"

__all__ = [
    "CLOSING_OR_CLOSED_MSG",
    "OFFLOAD_THREAD_NAME_PREFIX",
    "BoundedHttpClient",
    "HttpClientConfig",
    "ParsedRetryAfter",
    "WireFailure",
    "WireFailureKind",
    "WireRequest",
    "WireResponse",
    "is_dispatchable_url",
    "parse_retry_after",
]

__version__ = version(PACKAGE_NAME)

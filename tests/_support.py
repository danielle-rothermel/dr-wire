from __future__ import annotations

from typing import Any

import httpx

from dr_wire import BoundedHttpClient, HttpClientConfig, WireRequest

TEST_TIMEOUT_SECONDS = 120.0
TEST_CONNECT_TIMEOUT_SECONDS = 30.0
TEST_IDLE_TIMEOUT_SECONDS = 90.0
TEST_MAX_CONNECTIONS = 10
TEST_MAX_KEEPALIVE_CONNECTIONS = 5
TEST_MAX_REQUEST_BYTES = 1024 * 1024
TEST_MAX_RESPONSE_BYTES = 8 * 1024 * 1024

TEST_URL = "https://example.test/v1/chat/completions"
TEST_BODY = b'{"model":"m"}'
OK_BODY = b'{"ok":true}'


def make_config(**overrides: Any) -> HttpClientConfig:
    fields: dict[str, Any] = {
        "timeout_seconds": TEST_TIMEOUT_SECONDS,
        "connect_timeout_seconds": TEST_CONNECT_TIMEOUT_SECONDS,
        "idle_timeout_seconds": TEST_IDLE_TIMEOUT_SECONDS,
        "max_connections": TEST_MAX_CONNECTIONS,
        "max_keepalive_connections": TEST_MAX_KEEPALIVE_CONNECTIONS,
        "max_request_bytes": TEST_MAX_REQUEST_BYTES,
        "max_response_bytes": TEST_MAX_RESPONSE_BYTES,
    }
    fields.update(overrides)
    return HttpClientConfig(**fields)


def make_request(**overrides: Any) -> WireRequest:
    fields: dict[str, Any] = {
        "method": "POST",
        "url": TEST_URL,
        "headers": {"content-type": "application/json"},
        "body": TEST_BODY,
    }
    fields.update(overrides)
    return WireRequest(**fields)


def ok_handler(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, content=OK_BODY)


def make_client(
    handler: Any = ok_handler,
    *,
    client: httpx.Client | None = None,
    **config_overrides: Any,
) -> BoundedHttpClient:
    """Build a client whose transport is a mock rather than a socket."""
    built = (
        httpx.Client(transport=httpx.MockTransport(handler))
        if client is None
        else client
    )
    return BoundedHttpClient(
        make_config(**config_overrides),
        client_factory=lambda **_kwargs: built,
    )

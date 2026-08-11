from __future__ import annotations

import gzip
import threading
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from _support import OK_BODY, make_client, make_config, make_request

from dr_http import WireFailure, WireFailureKind, WireResponse
from dr_http.client import (
    CLOSING_OR_CLOSED_MSG,
    RESPONSE_STREAM_CHUNK_BYTES,
    _httpx_timeout,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


class ByteChunks(httpx.SyncByteStream):
    """Yield one byte at a time so a bound is crossed at an exact byte."""

    def __init__(self, content: bytes) -> None:
        self._content = content
        self.yielded = 0

    def __iter__(self) -> Iterator[bytes]:
        for byte in self._content:
            self.yielded += 1
            yield bytes((byte,))


def _raising(error: BaseException) -> Any:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise error

    return handler


def test_a_success_returns_status_headers_and_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.content == b'{"model":"m"}'
        assert request.headers["content-type"] == "application/json"
        return httpx.Response(200, headers={"x-seen": "1"}, content=OK_BODY)

    result = make_client(handler).call(make_request())

    assert isinstance(result, WireResponse)
    assert result.status_code == 200
    assert result.body == OK_BODY
    assert result.headers["x-seen"] == "1"
    assert result.retry_after is None


@pytest.mark.parametrize(
    "status_code",
    [200, 204, 301, 302, 307, 308, 400, 429, 500, 503],
)
def test_every_status_is_data_not_a_failure(status_code: int) -> None:
    """Redirects and errors are statuses; classification is the caller's."""
    result = make_client(
        lambda _request: httpx.Response(
            status_code, headers={"location": "https://elsewhere.test"}
        )
    ).call(make_request())

    assert isinstance(result, WireResponse)
    assert result.status_code == status_code


def test_redirects_are_not_followed_even_when_the_client_would() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(
            302, headers={"location": "https://elsewhere.test/next"}
        )

    following = httpx.Client(
        transport=httpx.MockTransport(handler), follow_redirects=True
    )
    result = make_client(client=following).call(make_request())

    assert isinstance(result, WireResponse)
    assert result.status_code == 302
    assert len(seen) == 1


def test_a_retry_after_header_is_parsed_onto_the_response() -> None:
    result = make_client(
        lambda _request: httpx.Response(429, headers={"retry-after": "12"})
    ).call(make_request())

    assert isinstance(result, WireResponse)
    assert result.retry_after is not None
    assert result.retry_after.kind == "delta_seconds"
    assert result.retry_after.value == 12


def test_a_body_refused_for_size_still_reports_its_retry_after() -> None:
    """The one refusal that saw a response head keeps the peer's hint.

    The raw header travels beside the parsed value so a consumer whose
    retention policy bounds the header itself can apply that rule on
    this path exactly as it does on a response.
    """
    header = " " * 200 + "12"
    result = make_client(
        lambda _request: httpx.Response(
            429, headers={"retry-after": header}, stream=ByteChunks(OK_BODY)
        ),
        max_response_bytes=1,
    ).call(make_request())

    assert isinstance(result, WireFailure)
    assert result.kind is WireFailureKind.RESPONSE_TOO_LARGE
    assert result.retry_after is not None
    assert result.retry_after.kind == "delta_seconds"
    assert result.retry_after.value == 12
    assert result.retry_after_header == header


def test_a_refusal_with_no_response_head_reports_no_retry_after() -> None:
    refused = make_client(max_request_bytes=1).call(make_request())
    failed = make_client(_raising(httpx.ConnectError("down"))).call(
        make_request()
    )

    assert isinstance(refused, WireFailure)
    assert isinstance(failed, WireFailure)
    assert refused.retry_after is None
    assert refused.retry_after_header is None
    assert failed.retry_after is None
    assert failed.retry_after_header is None


def test_call_on_a_closed_client_raises_this_packages_own_message() -> None:
    """A closed client is a caller error, not an httpx-internal one."""
    client = make_client()
    client.close()

    with pytest.raises(RuntimeError, match=CLOSING_OR_CLOSED_MSG):
        client.call(make_request())


ERROR_KINDS: list[tuple[str, BaseException, WireFailureKind]] = [
    (
        "pool-timeout",
        httpx.PoolTimeout("pool starved"),
        WireFailureKind.POOL_TIMEOUT,
    ),
    (
        "read-timeout",
        httpx.ReadTimeout("idle stall"),
        WireFailureKind.STALLED_RESPONSE,
    ),
    (
        "connect-timeout",
        httpx.ConnectTimeout("slow connect"),
        WireFailureKind.TIMEOUT,
    ),
    (
        "write-timeout",
        httpx.WriteTimeout("slow write"),
        WireFailureKind.TIMEOUT,
    ),
    ("invalid-url", httpx.InvalidURL("rejected"), WireFailureKind.INVALID_URL),
    (
        "connect-error",
        httpx.ConnectError("down"),
        WireFailureKind.CONNECT_ERROR,
    ),
    (
        "read-error",
        httpx.ReadError("read failed"),
        WireFailureKind.NETWORK_ERROR,
    ),
    (
        "write-error",
        httpx.WriteError("write failed"),
        WireFailureKind.NETWORK_ERROR,
    ),
    (
        "close-error",
        httpx.CloseError("close failed"),
        WireFailureKind.NETWORK_ERROR,
    ),
    (
        "proxy-error",
        httpx.ProxyError("proxy down"),
        WireFailureKind.NETWORK_ERROR,
    ),
    (
        "remote-protocol",
        httpx.RemoteProtocolError("server disconnected"),
        WireFailureKind.REMOTE_PROTOCOL_ERROR,
    ),
    (
        "local-protocol",
        httpx.LocalProtocolError("bad framing"),
        WireFailureKind.LOCAL_PROTOCOL_ERROR,
    ),
    (
        "bare-protocol",
        httpx.ProtocolError("bare protocol error"),
        WireFailureKind.LOCAL_PROTOCOL_ERROR,
    ),
    (
        "decoding",
        httpx.DecodingError("bad encoding"),
        WireFailureKind.LOCAL_PROTOCOL_ERROR,
    ),
    (
        "unsupported-protocol",
        httpx.UnsupportedProtocol("bad scheme"),
        WireFailureKind.LOCAL_PROTOCOL_ERROR,
    ),
    (
        "too-many-redirects",
        httpx.TooManyRedirects("too many"),
        WireFailureKind.LOCAL_PROTOCOL_ERROR,
    ),
    (
        "http-status-error",
        httpx.HTTPStatusError(
            "raised status",
            request=httpx.Request("POST", "https://example.test"),
            response=httpx.Response(500),
        ),
        WireFailureKind.UNKNOWN,
    ),
    (
        "bare-http-error",
        httpx.HTTPError("bare http error"),
        WireFailureKind.UNKNOWN,
    ),
]


@pytest.mark.parametrize(
    ("error", "expected_kind"),
    [(error, kind) for _id, error, kind in ERROR_KINDS],
    ids=[case_id for case_id, _error, _kind in ERROR_KINDS],
)
def test_every_httpx_error_maps_to_a_kind_without_raising(
    error: BaseException, expected_kind: WireFailureKind
) -> None:
    result = make_client(_raising(error)).call(make_request())

    assert isinstance(result, WireFailure)
    assert result.kind is expected_kind
    assert result.exception_type == type(error).__name__
    assert result.traceback
    assert type(error).__name__ in result.traceback
    assert str(error) not in result.message
    assert result.observed_bytes is None


def test_summary_messages_stay_static_and_name_the_phase() -> None:
    timeout = make_client(_raising(httpx.ReadTimeout("secret detail"))).call(
        make_request()
    )
    transport = make_client(
        _raising(httpx.ConnectError("secret detail"))
    ).call(make_request())

    assert isinstance(timeout, WireFailure)
    assert isinstance(transport, WireFailure)
    assert timeout.message == "http transport timeout"
    assert transport.message == "http transport error"
    assert "secret detail" not in timeout.message
    assert "secret detail" not in transport.message


def test_every_failure_kind_is_reachable() -> None:
    """No kind names a condition the client cannot actually produce.

    Every kind here is collected from a real ``call`` result rather than
    written down, so a kind that stops being producible fails this test
    instead of being asserted into existence.
    """
    reached: set[WireFailureKind] = set()
    for _case_id, error, _expected_kind in ERROR_KINDS:
        result = make_client(_raising(error)).call(make_request())
        assert isinstance(result, WireFailure)
        reached.add(result.kind)

    request_refusal = make_client(max_request_bytes=1).call(make_request())
    response_refusal = make_client(
        lambda _request: httpx.Response(200, content=OK_BODY),
        max_response_bytes=1,
    ).call(make_request())

    assert isinstance(request_refusal, WireFailure)
    assert isinstance(response_refusal, WireFailure)
    assert request_refusal.kind is WireFailureKind.REQUEST_TOO_LARGE
    assert response_refusal.kind is WireFailureKind.RESPONSE_TOO_LARGE
    reached |= {request_refusal.kind, response_refusal.kind}

    assert reached == set(WireFailureKind)


@pytest.mark.parametrize(
    ("limit_delta", "expected"),
    [(-1, "refused"), (0, "sent"), (1, "sent")],
    ids=("limit-minus-one", "exact-limit", "limit-plus-one"),
)
def test_the_request_bound_is_exact_and_refuses_before_dispatch(
    limit_delta: int, expected: str
) -> None:
    seen: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.content)
        return httpx.Response(200, content=OK_BODY)

    request = make_request()
    limit = len(request.body) + limit_delta
    result = make_client(handler, max_request_bytes=limit).call(request)

    if expected == "sent":
        assert isinstance(result, WireResponse)
        assert seen == [request.body]
    else:
        assert isinstance(result, WireFailure)
        assert result.kind is WireFailureKind.REQUEST_TOO_LARGE
        assert result.observed_bytes == len(request.body)
        assert result.exception_type == ""
        assert result.traceback == ""
        assert seen == []


@pytest.mark.parametrize(
    ("limit_delta", "expected"),
    [(-1, "refused"), (0, "accepted"), (1, "accepted")],
    ids=("limit-minus-one", "exact-limit", "limit-plus-one"),
)
def test_the_response_bound_is_exact_and_stops_streaming(
    limit_delta: int, expected: str
) -> None:
    stream = ByteChunks(OK_BODY)
    limit = len(OK_BODY) + limit_delta
    result = make_client(
        lambda _request: httpx.Response(
            200, headers={"content-length": "1"}, stream=stream
        ),
        max_response_bytes=limit,
    ).call(make_request())

    if expected == "refused":
        assert isinstance(result, WireFailure)
        assert result.kind is WireFailureKind.RESPONSE_TOO_LARGE
        assert result.observed_bytes == limit + 1
        assert result.exception_type == ""
        assert result.traceback == ""
        assert stream.yielded == limit + 1
    else:
        assert isinstance(result, WireResponse)
        assert result.body == OK_BODY
        assert stream.yielded == len(OK_BODY)


def test_the_response_is_read_one_bounded_chunk_at_a_time() -> None:
    """The refusal overshoot is bounded by one chunk, not by body size.

    ByteChunks elsewhere yields single bytes, which hides the chunk size
    the memory bound depends on, so the granularity is pinned here
    against the real reader. The literal is pinned alongside it because
    raising it raises how much a caller buffers past the limit before
    the body is refused.
    """
    assert RESPONSE_STREAM_CHUNK_BYTES == 64 * 1024

    requested: list[int | None] = []
    over_limit = RESPONSE_STREAM_CHUNK_BYTES * 4
    limit = 1

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * over_limit)

    client = make_client(handler, max_response_bytes=limit)
    original = httpx.Response.iter_bytes

    def recording(
        self: httpx.Response, chunk_size: int | None = None
    ) -> Iterator[bytes]:
        requested.append(chunk_size)
        return original(self, chunk_size=chunk_size)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(httpx.Response, "iter_bytes", recording)
        result = client.call(make_request())

    assert requested[-1] == RESPONSE_STREAM_CHUNK_BYTES
    assert isinstance(result, WireFailure)
    assert result.kind is WireFailureKind.RESPONSE_TOO_LARGE
    assert result.observed_bytes is not None
    assert result.observed_bytes <= limit + RESPONSE_STREAM_CHUNK_BYTES
    assert result.observed_bytes < over_limit


def test_a_missing_content_length_does_not_truncate_the_body() -> None:
    stream = ByteChunks(OK_BODY)
    result = make_client(
        lambda _request: httpx.Response(200, stream=stream),
        max_response_bytes=len(OK_BODY),
    ).call(make_request())

    assert isinstance(result, WireResponse)
    assert result.body == OK_BODY
    assert stream.yielded == len(OK_BODY)


def test_the_response_bound_applies_to_decompressed_bytes() -> None:
    compressed = gzip.compress(OK_BODY)
    result = make_client(
        lambda _request: httpx.Response(
            200,
            headers={"content-encoding": "gzip"},
            stream=ByteChunks(compressed),
        ),
        max_response_bytes=len(OK_BODY) - 1,
    ).call(make_request())

    assert isinstance(result, WireFailure)
    assert result.kind is WireFailureKind.RESPONSE_TOO_LARGE
    assert result.observed_bytes is not None
    assert result.observed_bytes > len(OK_BODY) - 1


def test_a_defect_before_dispatch_crashes_rather_than_reporting_a_kind() -> (
    None
):
    """Our own defect must not be reported as a wire condition."""

    class BrokenStreamClient(httpx.Client):
        def stream(self, *_args: Any, **_kwargs: Any) -> Any:
            msg = "defect before the request was dispatched"
            raise ValueError(msg)

    client = make_client(
        client=BrokenStreamClient(transport=httpx.MockTransport(ok_response))
    )

    with pytest.raises(ValueError, match="defect before the request"):
        client.call(make_request())


class _DefectiveStream(httpx.SyncByteStream):
    """Send part of a body, then fail the way a defect of ours would."""

    def __init__(self, error: BaseException) -> None:
        self._error = error

    def __iter__(self) -> Iterator[bytes]:
        yield b"partial"
        raise self._error


def test_a_defect_after_dispatch_crashes_rather_than_reporting_a_kind() -> (
    None
):
    """A defect raised mid-body is ours, so it must escape call()."""
    defect = ValueError("defect while reading the sent response")
    client = make_client(
        lambda _request: httpx.Response(200, stream=_DefectiveStream(defect))
    )

    with pytest.raises(ValueError, match="defect while reading"):
        client.call(make_request())


def test_a_wire_error_after_dispatch_is_still_reported_as_a_kind() -> None:
    """A mid-body transport failure is the peer's, so it stays a value."""
    client = make_client(
        lambda _request: httpx.Response(
            200, stream=_DefectiveStream(httpx.ReadError("connection lost"))
        )
    )

    result = client.call(make_request())

    assert isinstance(result, WireFailure)
    assert result.kind is WireFailureKind.NETWORK_ERROR
    assert result.exception_type == "ReadError"


def ok_response(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, content=OK_BODY)


def test_native_timeout_phases_are_explicit_and_saturated() -> None:
    timeout = _httpx_timeout(
        make_config(
            timeout_seconds=1e300,
            connect_timeout_seconds=1e300,
            idle_timeout_seconds=1e300,
        )
    )

    assert timeout.connect == threading.TIMEOUT_MAX
    assert timeout.read == threading.TIMEOUT_MAX
    assert timeout.write == threading.TIMEOUT_MAX
    assert timeout.pool == threading.TIMEOUT_MAX


def test_each_timeout_phase_comes_from_its_own_config_field() -> None:
    timeout = _httpx_timeout(
        make_config(
            timeout_seconds=120.0,
            connect_timeout_seconds=45.0,
            idle_timeout_seconds=90.0,
        )
    )

    assert timeout.connect == 45.0
    assert timeout.read == 90.0
    assert timeout.write == 120.0
    assert timeout.pool == 120.0


def test_the_pool_is_sized_from_the_config() -> None:
    seen: list[httpx.Limits] = []

    def factory(**kwargs: Any) -> httpx.Client:
        seen.append(kwargs["limits"])
        assert kwargs["follow_redirects"] is False
        return httpx.Client(transport=httpx.MockTransport(ok_response))

    from dr_http import BoundedHttpClient

    BoundedHttpClient(
        make_config(max_connections=7, max_keepalive_connections=3),
        client_factory=factory,
    )

    assert seen[0].max_connections == 7
    assert seen[0].max_keepalive_connections == 3

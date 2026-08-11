"""One bounded synchronous HTTP client with an explicit lifecycle.

Nothing in this module is ever persisted. The client returns in-process
values; the caller decides what any of them mean in its own recorded
vocabulary.
"""

from __future__ import annotations

import contextlib
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from enum import Enum, auto
from typing import TYPE_CHECKING

import httpx

from dr_http.headers import parse_retry_after
from dr_http.wire import (
    WireFailure,
    WireFailureKind,
    WireResponse,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from concurrent.futures import Future

    from dr_http.config import HttpClientConfig
    from dr_http.wire import WireRequest

RESPONSE_STREAM_CHUNK_BYTES = 64 * 1024
OFFLOAD_THREAD_NAME_PREFIX = "dr-http-offload"
CLOSING_OR_CLOSED_MSG = "BoundedHttpClient is closing or closed"

TRANSPORT_ERROR_MESSAGE = "http transport error"
TIMEOUT_MESSAGE = "http transport timeout"
INVALID_URL_MESSAGE = "url is not dispatchable as http or https"
REQUEST_TOO_LARGE_MESSAGE = "request body exceeds the configured byte limit"
RESPONSE_TOO_LARGE_MESSAGE = "response body exceeds the configured byte limit"


class _ClientState(Enum):
    OPEN = auto()
    DRAINING_OFFLOADS = auto()
    CLOSING = auto()
    CLOSED = auto()


_CLOSING_STATES = frozenset(
    {_ClientState.DRAINING_OFFLOADS, _ClientState.CLOSING}
)
_USABLE_STATES = frozenset({_ClientState.OPEN, _ClientState.DRAINING_OFFLOADS})


def _operational_timeout_seconds(timeout_seconds: float) -> float:
    return min(timeout_seconds, threading.TIMEOUT_MAX)


def _httpx_timeout(config: HttpClientConfig) -> httpx.Timeout:
    """Use direct native phase timeouts for the synchronous operation."""
    operation_timeout = _operational_timeout_seconds(config.timeout_seconds)
    connect_timeout = _operational_timeout_seconds(
        config.connect_timeout_seconds
    )
    read_timeout = _operational_timeout_seconds(config.idle_timeout_seconds)
    return httpx.Timeout(
        connect=connect_timeout,
        read=read_timeout,
        write=operation_timeout,
        pool=operation_timeout,
    )


def _exception_traceback(error: BaseException) -> str:
    return "".join(
        traceback.format_exception(type(error), error, error.__traceback__)
    )


def _timeout_kind(error: httpx.TimeoutException) -> WireFailureKind:
    """Name the timeout phase so local contention stays distinguishable."""
    if isinstance(error, httpx.PoolTimeout):
        return WireFailureKind.POOL_TIMEOUT
    if isinstance(error, httpx.ReadTimeout):
        return WireFailureKind.STALLED_RESPONSE
    return WireFailureKind.TIMEOUT


def _http_error_kind(error: httpx.HTTPError) -> WireFailureKind:
    """Classify one non-timeout httpx wire error.

    Ordering is significant: ``RemoteProtocolError`` is checked before
    the other protocol errors because a peer that violates the protocol
    is a different condition from a violation on this side, and
    ``ConnectError`` is checked before the wider ``NetworkError`` family
    it belongs to.
    """
    if isinstance(error, httpx.TimeoutException):
        return _timeout_kind(error)
    if isinstance(error, httpx.RemoteProtocolError):
        return WireFailureKind.REMOTE_PROTOCOL_ERROR
    if isinstance(
        error,
        httpx.ProtocolError
        | httpx.DecodingError
        | httpx.UnsupportedProtocol
        | httpx.TooManyRedirects,
    ):
        return WireFailureKind.LOCAL_PROTOCOL_ERROR
    if isinstance(error, httpx.ConnectError):
        return WireFailureKind.CONNECT_ERROR
    if isinstance(error, httpx.NetworkError | httpx.ProxyError):
        return WireFailureKind.NETWORK_ERROR
    return WireFailureKind.UNKNOWN


def _failure_from_error(
    kind: WireFailureKind,
    error: BaseException,
    message: str,
) -> WireFailure:
    return WireFailure(
        kind=kind,
        exception_type=type(error).__name__,
        message=message,
        traceback=_exception_traceback(error),
    )


class BoundedHttpClient:
    """Own and reuse one bounded synchronous HTTP client until closed.

    The client bounds its own connection pool, request and response
    bytes, and every native timeout phase from one explicit config.
    Callers that need to run blocking work on client-owned threads
    offload it onto an executor whose worker count is
    ``config.max_connections``, so thread count and connection-pool size
    cannot disagree. The executor exists only once ``offload`` is
    called.
    """

    def __init__(
        self,
        config: HttpClientConfig,
        *,
        client_factory: Callable[..., httpx.Client] | None = None,
    ) -> None:
        self._config = config
        limits = httpx.Limits(
            max_connections=config.max_connections,
            max_keepalive_connections=config.max_keepalive_connections,
        )
        factory = httpx.Client if client_factory is None else client_factory
        self._client = factory(limits=limits, follow_redirects=False)
        self._condition = threading.Condition()
        self._state = _ClientState.OPEN
        self._active_work = 0
        self._active_offloads = 0
        self._executor: ThreadPoolExecutor | None = None

    def close(self) -> None:
        """Drain offloaded work, drain admitted work, and close once.

        Offloaded work keeps using the client while it drains, so work
        admission stays open until the offload drain finishes.

        An exception escaping either drain wait, such as a keyboard
        interrupt delivered to the closing thread, aborts the close: the
        client becomes terminal, admission stays refused, the executor
        and the underlying client are released without joining workers,
        and the original exception propagates. An aborted close does not
        complete the drain. Only the closing thread that owns the drain
        aborts this way; a caller interrupted while waiting for another
        closer to finish re-raises without touching client state.
        """
        became_primary = False
        try:
            with self._condition:
                if self._state is _ClientState.CLOSED:
                    return
                if self._state in _CLOSING_STATES:
                    while self._state is not _ClientState.CLOSED:
                        self._condition.wait()
                    return
                became_primary = True
                self._state = _ClientState.DRAINING_OFFLOADS
                self._condition.notify_all()
                while self._active_offloads:
                    self._condition.wait()
                self._state = _ClientState.CLOSING
                self._condition.notify_all()
                while self._active_work:
                    self._condition.wait()
                executor = self._executor
        except BaseException:
            if became_primary:
                self._abort_close()
            raise

        try:
            if executor is not None:
                executor.shutdown(wait=True)
        finally:
            try:
                self._client.close()
            finally:
                with self._condition:
                    self._state = _ClientState.CLOSED
                    self._condition.notify_all()

    def _abort_close(self) -> None:
        """Make an interrupted close terminal and release OS resources.

        The client becomes terminal so no later caller blocks waiting on
        a closer that no longer exists. Executor shutdown does not join
        workers because the abort means the operator asked to stop.
        Release errors are suppressed so the exception that aborted the
        close is the one that propagates.
        """
        with self._condition:
            self._state = _ClientState.CLOSED
            self._condition.notify_all()
            executor = self._executor

        try:
            if executor is not None:
                with contextlib.suppress(Exception):
                    executor.shutdown(wait=False)
        finally:
            with contextlib.suppress(Exception):
                self._client.close()

    def __enter__(self) -> BoundedHttpClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @contextlib.contextmanager
    def admit(self) -> Iterator[None]:
        """Hold one unit of caller work open against the drain.

        Admission is granted while the client is open and while
        offloaded work drains, so draining work can finish whole units,
        and is refused once the work drain begins. A caller wraps one
        complete operation in this, not one wire call, so a clean close
        drains complete operations.
        """
        self._begin_work()
        try:
            yield
        finally:
            self._end_work()

    def offload[ResultT](self, fn: Callable[[], ResultT]) -> Future[ResultT]:
        """Run ``fn`` on the client-owned executor until closing begins.

        The executor is created on first use with
        ``config.max_connections`` workers. A clean ``close`` drains
        admitted work before it shuts the executor down and closes the
        underlying client; an aborted ``close`` releases resources
        without completing that drain.

        Offloaded work must not call ``close`` or ``offload`` on the
        client that runs it: closing from inside offloaded work waits
        forever on that same work, and offloaded work that blocks on a
        nested offload starves once every worker is held that way.
        Honoring that restriction is the caller's obligation.
        """
        with self._condition:
            if self._state is not _ClientState.OPEN:
                raise RuntimeError(CLOSING_OR_CLOSED_MSG)
            if self._executor is None:
                self._executor = ThreadPoolExecutor(
                    max_workers=self._config.max_connections,
                    thread_name_prefix=OFFLOAD_THREAD_NAME_PREFIX,
                )
            self._active_offloads += 1
            executor = self._executor

        released = threading.Lock()

        def release() -> None:
            """Release this offload's hold on the drain exactly once."""
            if released.acquire(blocking=False):
                self._end_offload()

        def run() -> ResultT:
            try:
                return fn()
            finally:
                release()

        try:
            future = executor.submit(run)
        except BaseException:
            release()
            raise
        future.add_done_callback(lambda _future: release())
        return future

    def _end_offload(self) -> None:
        with self._condition:
            self._active_offloads -= 1
            if self._active_offloads == 0:
                self._condition.notify_all()

    def _require_usable(self) -> None:
        """Refuse a caller once the work drain has begun.

        The same predicate governs admission and dispatch, so a caller
        that reaches the client too late always sees this package's own
        message rather than the underlying client's.
        """
        with self._condition:
            if self._state not in _USABLE_STATES:
                raise RuntimeError(CLOSING_OR_CLOSED_MSG)

    def _begin_work(self) -> None:
        with self._condition:
            if self._state not in _USABLE_STATES:
                raise RuntimeError(CLOSING_OR_CLOSED_MSG)
            self._active_work += 1

    def _end_work(self) -> None:
        with self._condition:
            self._active_work -= 1
            if self._active_work == 0:
                self._condition.notify_all()

    def call(self, request: WireRequest) -> WireResponse | WireFailure:
        """Dispatch one request and return the wire result as a value.

        This is total for every wire-level problem: a refused byte
        bound, a timeout, a connection failure, a protocol violation, or
        an unrecognized httpx error all come back as ``WireFailure``.
        Every HTTP status, redirects included, comes back as
        ``WireResponse``; redirects are never followed.

        Anything raised out of this method is a defect rather than a
        wire condition, and crashes loudly instead of being reported as
        a request that failed on the wire.

        ``call`` takes no admission of its own. It runs inside
        ``admit()`` or inside offloaded work, so the caller decides what
        unit of work a close must drain. Calling once the work drain has
        begun is a caller-lifecycle error rather than a wire condition,
        so it raises the client's own ``RuntimeError`` instead of
        returning a ``WireFailure`` or surfacing the underlying client's
        message.
        """
        self._require_usable()
        if len(request.body) > self._config.max_request_bytes:
            return WireFailure(
                kind=WireFailureKind.REQUEST_TOO_LARGE,
                exception_type="",
                message=REQUEST_TOO_LARGE_MESSAGE,
                traceback="",
                observed_bytes=len(request.body),
            )
        try:
            with self._client.stream(
                request.method,
                request.url,
                content=request.body,
                headers=dict(request.headers),
                timeout=_httpx_timeout(self._config),
                follow_redirects=False,
            ) as http_response:
                retry_after_header = http_response.headers.get("retry-after")
                retry_after = parse_retry_after(retry_after_header)
                body, observed_bytes = self._read_response(http_response)
                if body is None:
                    return WireFailure(
                        kind=WireFailureKind.RESPONSE_TOO_LARGE,
                        exception_type="",
                        message=RESPONSE_TOO_LARGE_MESSAGE,
                        traceback="",
                        observed_bytes=observed_bytes,
                        retry_after=retry_after,
                        retry_after_header=retry_after_header,
                    )
                return WireResponse(
                    status_code=http_response.status_code,
                    headers=dict(http_response.headers),
                    body=bytes(body),
                    retry_after=retry_after,
                )
        except httpx.TimeoutException as error:
            return _failure_from_error(
                _timeout_kind(error), error, TIMEOUT_MESSAGE
            )
        except httpx.InvalidURL as error:
            # httpx.InvalidURL is not an httpx.HTTPError, so without this
            # guard a URL problem escapes call() untyped. The catch stays
            # this narrow on purpose: InvalidURL is raised while httpx
            # builds the request, so nothing reached the wire, whereas
            # any other post-dispatch error is a defect of ours and must
            # crash loudly rather than claim the request was never sent.
            return _failure_from_error(
                WireFailureKind.INVALID_URL, error, INVALID_URL_MESSAGE
            )
        except httpx.HTTPError as error:
            return _failure_from_error(
                _http_error_kind(error), error, TRANSPORT_ERROR_MESSAGE
            )

    def _read_response(
        self, http_response: httpx.Response
    ) -> tuple[bytearray | None, int]:
        """Stream the body, refusing it the moment it exceeds the bound.

        The bound applies to decompressed bytes, which is what a caller
        must hold in memory, and the stream stops at the first chunk
        that crosses it rather than reading the rest. The returned count
        is the bytes observed, which on refusal is the count that
        crossed the bound rather than the full body size.
        """
        body = bytearray()
        observed_bytes = 0
        for chunk in http_response.iter_bytes(
            chunk_size=RESPONSE_STREAM_CHUNK_BYTES
        ):
            observed_bytes += len(chunk)
            if observed_bytes > self._config.max_response_bytes:
                return None, observed_bytes
            body.extend(chunk)
        return body, observed_bytes

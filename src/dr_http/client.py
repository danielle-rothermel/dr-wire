"""One bounded synchronous HTTP client with an explicit lifecycle.

Nothing in this module is ever persisted. The client returns in-process
values; the caller decides what any of them mean in its own recorded
vocabulary.
"""

from __future__ import annotations

import contextlib
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING

import httpx

from dr_http.headers import parse_retry_after
from dr_http.wire import (
    WireFailure,
    WireFailureKind,
    WireFailureMessage,
    WireResponse,
    _http_error_kind,
    _timeout_kind,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from concurrent.futures import Future

    from dr_http.config import HttpClientConfig
    from dr_http.wire import WireRequest

# Tuning bound: how much a caller buffers past the limit before refusal.
RESPONSE_STREAM_CHUNK_BYTES = 64 * 1024

# Thread identity: names this client's executor threads in a stack dump.
OFFLOAD_THREAD_NAME_PREFIX = "dr-http-offload"

# Lifecycle message: the one refusal every closed-client path raises.
CLOSING_OR_CLOSED_MSG = "BoundedHttpClient is closing or closed"

# The RFC-defined header name, read once and carried once.
RETRY_AFTER_HEADER = "retry-after"


class _ClientState(Enum):
    OPEN = auto()
    DRAINING_OFFLOADS = auto()
    CLOSING = auto()
    CLOSED = auto()


@dataclass(frozen=True, slots=True)
class _BodyRefusedForSize:
    """A response body that crossed the byte bound while streaming.

    ``observed_bytes`` is the count that crossed the bound rather than
    the full body size, because the stream stops there.
    """

    observed_bytes: int


def _saturate(seconds: float) -> float:
    return min(seconds, threading.TIMEOUT_MAX)


def _httpx_timeout(config: HttpClientConfig) -> httpx.Timeout:
    """Use direct native phase timeouts for the synchronous operation.

    ``connect_timeout_seconds`` owns the connect phase and
    ``idle_timeout_seconds`` owns the read phase, while the general
    ``timeout_seconds`` owns both the write and pool phases. Each is
    saturated at ``threading.TIMEOUT_MAX``.
    """
    return httpx.Timeout(
        connect=_saturate(config.connect_timeout_seconds),
        read=_saturate(config.idle_timeout_seconds),
        write=_saturate(config.timeout_seconds),
        pool=_saturate(config.timeout_seconds),
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
                if self._state is not _ClientState.OPEN:
                    # An already-terminal client never waits here: the
                    # loop guard is false on entry.
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

        release_once = threading.Lock()

        def release() -> None:
            """Release this offload's hold on the drain exactly once.

            Three call sites race to release: the work's own ``finally``
            when it returns or raises, this scope when submission itself
            fails, and the future's done callback when the work was
            cancelled while queued. Whichever arrives first wins the
            lock, and the rest are no-ops, so the drain neither hangs on
            a hold never released nor ends early on one released twice.
            """
            if release_once.acquire(blocking=False):
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

    def _refuse_if_drained(self) -> None:
        """Refuse a caller once the work drain has begun.

        The same predicate governs admission and dispatch, so a caller
        that reaches the client too late always sees this package's own
        message rather than the underlying client's. The caller holds
        the condition lock, so a refusal and whatever it guards stay one
        critical section.
        """
        # Usable while open and while offloaded work drains, so draining
        # work can finish whole units; refused from CLOSING onward.
        if self._state not in {
            _ClientState.OPEN,
            _ClientState.DRAINING_OFFLOADS,
        }:
            raise RuntimeError(CLOSING_OR_CLOSED_MSG)

    def _begin_work(self) -> None:
        with self._condition:
            self._refuse_if_drained()
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
        with self._condition:
            self._refuse_if_drained()
        if len(request.body) > self._config.max_request_bytes:
            return WireFailure(
                kind=WireFailureKind.REQUEST_TOO_LARGE,
                exception_type="",
                message=WireFailureMessage.REQUEST_TOO_LARGE,
                traceback="",
                observed_bytes=len(request.body),
            )
        try:
            return self._exchange(request)
        except httpx.TimeoutException as error:
            return WireFailure.from_error(
                _timeout_kind(error), error, WireFailureMessage.TIMEOUT
            )
        except httpx.InvalidURL as error:
            # httpx.InvalidURL is not an httpx.HTTPError, so without this
            # guard a URL problem escapes call() untyped. The catch stays
            # this narrow on purpose: InvalidURL is raised while httpx
            # builds the request, so nothing reached the wire, whereas
            # any other post-dispatch error is a defect of ours and must
            # crash loudly rather than claim the request was never sent.
            return WireFailure.from_error(
                WireFailureKind.INVALID_URL,
                error,
                WireFailureMessage.INVALID_URL,
            )
        except httpx.HTTPError as error:
            return WireFailure.from_error(
                _http_error_kind(error),
                error,
                WireFailureMessage.TRANSPORT_ERROR,
            )

    def _exchange(self, request: WireRequest) -> WireResponse | WireFailure:
        """Stream one dispatched exchange into a wire value.

        Every failure this produces is a refused response bound; a wire
        exception raised here reaches ``call`` for translation.
        """
        with self._client.stream(
            request.method,
            request.url,
            content=request.body,
            headers=dict(request.headers),
            timeout=_httpx_timeout(self._config),
            follow_redirects=False,
        ) as http_response:
            retry_after_header = http_response.headers.get(RETRY_AFTER_HEADER)
            retry_after = parse_retry_after(retry_after_header)
            body = self._read_response(http_response)
            if isinstance(body, _BodyRefusedForSize):
                return WireFailure(
                    kind=WireFailureKind.RESPONSE_TOO_LARGE,
                    exception_type="",
                    message=WireFailureMessage.RESPONSE_TOO_LARGE,
                    traceback="",
                    observed_bytes=body.observed_bytes,
                    retry_after=retry_after,
                    retry_after_header=retry_after_header,
                )
            return WireResponse(
                status_code=http_response.status_code,
                headers=dict(http_response.headers),
                body=body,
                retry_after=retry_after,
            )

    def _read_response(
        self, http_response: httpx.Response
    ) -> bytes | _BodyRefusedForSize:
        """Stream the body, refusing it the moment it exceeds the bound.

        The bound applies to decompressed bytes, which is what a caller
        must hold in memory, and the stream stops at the first chunk
        that crosses it rather than reading the rest.
        """
        body = bytearray()
        observed_bytes = 0
        for chunk in http_response.iter_bytes(
            chunk_size=RESPONSE_STREAM_CHUNK_BYTES
        ):
            observed_bytes += len(chunk)
            if observed_bytes > self._config.max_response_bytes:
                return _BodyRefusedForSize(observed_bytes=observed_bytes)
            body.extend(chunk)
        return bytes(body)

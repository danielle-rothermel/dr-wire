from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, override

import httpx
import pytest
from _concurrency import WATCHDOG_SECONDS, DaemonCall, wait_for
from _support import (
    OK_BODY,
    TEST_MAX_CONNECTIONS,
    make_client,
    make_config,
    make_request,
    ok_handler,
)

from dr_http import BoundedHttpClient, WireResponse
from dr_http.client import OFFLOAD_THREAD_NAME_PREFIX

if TYPE_CHECKING:
    from collections.abc import Callable

OFFLOADED_FAILURE_MSG = "offloaded failure"
OFFLOADED_WORK_NOT_RELEASED = "offloaded work was not released"
OFFLOADED_WORK_DID_NOT_START = "offloaded work did not start"
DRAINED_OFFLOAD_RESULT = "drained"
SHUTDOWN_FAILED_MSG = "shutdown failed"
CLOSE_WAIT_NOT_REACHED = "close did not reach the drain wait"
SECONDARY_CLOSE_NOT_REACHED = "secondary closer did not reach its wait"
CLIENT_CLOSE_FAILED_MSG = "client close failed"
NO_THREAD_IDENT = 0


def _call(client: BoundedHttpClient) -> WireResponse:
    with client.admit():
        result = client.call(make_request())
    assert isinstance(result, WireResponse)
    return result


def _wait_for_state(client: BoundedHttpClient, name: str) -> None:
    with client._condition:
        reached = client._condition.wait_for(
            lambda: client._state.name == name,
            timeout=WATCHDOG_SECONDS,
        )
    assert reached, f"client did not reach {name}"


class _InterruptingCondition(threading.Condition):
    """Raise out of the drain wait taken in one named client state.

    This stands in for a keyboard interrupt delivered to the closing
    thread, which CPython raises out of exactly this wait. Setting
    ``only_thread_ident`` restricts the interrupt to one thread so a
    test can target a specific waiter.
    """

    def __init__(self, interrupt_in_state: str) -> None:
        super().__init__()
        self._interrupt_in_state = interrupt_in_state
        self.state_name: Callable[[], str] = lambda: ""
        self.interrupted = threading.Event()
        self.only_thread_ident: int | None = None

    @override
    def wait(self, timeout: float | None = None) -> bool:
        targeted = (
            self.only_thread_ident is None
            or self.only_thread_ident == threading.get_ident()
        )
        if targeted and self.state_name() == self._interrupt_in_state:
            self.interrupted.set()
            raise KeyboardInterrupt(self._interrupt_in_state)
        return super().wait(timeout)


def _client_interrupted_in(
    state_name: str, *, client: httpx.Client | None = None
) -> tuple[BoundedHttpClient, _InterruptingCondition]:
    """Build a client whose close aborts while waiting in ``state_name``."""
    bounded = make_client(client=client)
    condition = _InterruptingCondition(state_name)
    condition.state_name = lambda: bounded._state.name
    bounded._condition = condition
    return bounded, condition


def test_sync_only_use_never_creates_the_executor() -> None:
    client = make_client()

    _call(client)
    client.close()

    assert client._executor is None


def test_first_offload_creates_an_executor_sized_from_max_connections() -> (
    None
):
    client = make_client()

    with client:
        assert (
            client.offload(lambda: "offloaded").result(
                timeout=WATCHDOG_SECONDS
            )
            == "offloaded"
        )
        executor = client._executor

    assert executor is not None
    assert executor._max_workers == TEST_MAX_CONNECTIONS


def test_executor_size_follows_max_connections() -> None:
    client = make_client(max_connections=3, max_keepalive_connections=1)

    with client:
        client.offload(lambda: None).result(timeout=WATCHDOG_SECONDS)
        executor = client._executor

    assert executor is not None
    assert executor._max_workers == 3


def test_offload_reuses_one_executor() -> None:
    client = make_client()

    with client:
        client.offload(lambda: None).result(timeout=WATCHDOG_SECONDS)
        first = client._executor
        client.offload(lambda: None).result(timeout=WATCHDOG_SECONDS)

        assert client._executor is first


def test_failing_offloaded_work_releases_the_drain() -> None:
    client = make_client()

    def boom() -> None:
        raise ValueError(OFFLOADED_FAILURE_MSG)

    with client:
        future = client.offload(boom)
        with pytest.raises(ValueError, match=OFFLOADED_FAILURE_MSG):
            future.result(timeout=WATCHDOG_SECONDS)

        assert client._active_offloads == 0


def test_cancelling_a_queued_offload_releases_the_drain() -> None:
    started = threading.Event()
    release = threading.Event()
    client = make_client(max_connections=1, max_keepalive_connections=1)

    def gated() -> None:
        started.set()
        wait_for(release, OFFLOADED_WORK_NOT_RELEASED)

    running = client.offload(gated)
    wait_for(started, OFFLOADED_WORK_DID_NOT_START)
    queued = client.offload(lambda: "never runs")

    assert queued.cancel()
    assert queued.cancelled()

    release.set()
    running.result(timeout=WATCHDOG_SECONDS)

    with client._condition:
        drained = client._condition.wait_for(
            lambda: client._active_offloads == 0,
            timeout=WATCHDOG_SECONDS,
        )
    assert drained

    DaemonCall.start(client.close).result()
    assert client._state.name == "CLOSED"


def test_a_failed_submit_releases_the_drain_and_close_completes() -> None:
    client = make_client()

    client.offload(lambda: None).result(timeout=WATCHDOG_SECONDS)
    executor = client._executor
    assert executor is not None
    executor.shutdown(wait=True)

    with pytest.raises(RuntimeError, match="cannot schedule new futures"):
        client.offload(lambda: None)
    assert client._active_offloads == 0

    DaemonCall.start(client.close).result()
    assert client._state.name == "CLOSED"


def test_close_shuts_down_the_executor_and_joins_its_threads() -> None:
    client = make_client()

    client.offload(lambda: None).result(timeout=WATCHDOG_SECONDS)
    executor = client._executor
    assert executor is not None

    client.close()

    assert executor._shutdown is True
    assert not [
        thread
        for thread in threading.enumerate()
        if thread.name.startswith(OFFLOAD_THREAD_NAME_PREFIX)
        and thread.is_alive()
    ]


class _RaisingShutdownExecutor(ThreadPoolExecutor):
    """Fail shutdown so close must still reach the underlying client."""

    @override
    def shutdown(
        self, wait: bool = True, *, cancel_futures: bool = False
    ) -> None:
        super().shutdown(wait=wait, cancel_futures=cancel_futures)
        raise RuntimeError(SHUTDOWN_FAILED_MSG)


class _CloseCountingClient(httpx.Client):
    """Count how many times the underlying client is closed."""

    close_count: int = 0

    @override
    def close(self) -> None:
        self.close_count += 1
        super().close()


def test_a_failing_executor_shutdown_still_closes_the_client_once() -> None:
    inner = _CloseCountingClient(transport=httpx.MockTransport(ok_handler))
    client = make_client(client=inner)
    client._executor = _RaisingShutdownExecutor(max_workers=1)

    with pytest.raises(RuntimeError, match=SHUTDOWN_FAILED_MSG):
        client.close()

    assert inner.close_count == 1
    assert inner.is_closed is True
    assert client._state.name == "CLOSED"


def test_close_drains_offloaded_work_before_completing() -> None:
    started = threading.Event()
    release = threading.Event()
    client = make_client()

    def gated() -> str:
        started.set()
        wait_for(release, OFFLOADED_WORK_NOT_RELEASED)
        return DRAINED_OFFLOAD_RESULT

    future = client.offload(gated)
    wait_for(started, OFFLOADED_WORK_DID_NOT_START)

    closer = DaemonCall.start(client.close)
    closer.wait_until_entered()
    _wait_for_state(client, "DRAINING_OFFLOADS")
    with pytest.raises(RuntimeError, match="closing or closed"):
        client.offload(lambda: None)
    assert not closer.is_done()
    executor = client._executor
    assert executor is not None

    release.set()
    assert future.result(timeout=WATCHDOG_SECONDS) == DRAINED_OFFLOAD_RESULT
    closer.result()

    assert client._state.name == "CLOSED"
    assert executor._shutdown is True


def test_any_caller_may_be_admitted_while_offloaded_work_drains() -> None:
    started = threading.Event()
    release = threading.Event()
    client = make_client()

    def gated() -> None:
        started.set()
        wait_for(release, OFFLOADED_WORK_NOT_RELEASED)

    future = client.offload(gated)
    wait_for(started, OFFLOADED_WORK_DID_NOT_START)

    closer = DaemonCall.start(client.close)
    closer.wait_until_entered()
    _wait_for_state(client, "DRAINING_OFFLOADS")

    external = DaemonCall.start(lambda: _call(client))
    response = external.result()

    release.set()
    future.result(timeout=WATCHDOG_SECONDS)
    closer.result()

    assert response.body == OK_BODY
    assert client._state.name == "CLOSED"


def test_draining_offloaded_work_can_still_call_the_client() -> None:
    started = threading.Event()
    release = threading.Event()
    client = make_client()

    def gated() -> WireResponse:
        started.set()
        wait_for(release, OFFLOADED_WORK_NOT_RELEASED)
        assert client._client.is_closed is False
        return _call(client)

    future = client.offload(gated)
    wait_for(started, OFFLOADED_WORK_DID_NOT_START)

    closer = DaemonCall.start(client.close)
    closer.wait_until_entered()
    _wait_for_state(client, "DRAINING_OFFLOADS")
    with pytest.raises(RuntimeError, match="closing or closed"):
        client.offload(lambda: None)

    release.set()
    response = future.result(timeout=WATCHDOG_SECONDS)
    closer.result()

    assert response.body == OK_BODY
    assert client._state.name == "CLOSED"
    assert client._client.is_closed is True


def test_offload_after_close_raises_and_concurrent_closers_return() -> None:
    started = threading.Event()
    release = threading.Event()
    client = make_client()

    def gated() -> None:
        started.set()
        wait_for(release, OFFLOADED_WORK_NOT_RELEASED)

    future = client.offload(gated)
    wait_for(started, OFFLOADED_WORK_DID_NOT_START)

    closers = [DaemonCall.start(client.close) for _ in range(2)]
    for closer in closers:
        closer.wait_until_entered()
    _wait_for_state(client, "DRAINING_OFFLOADS")

    release.set()
    future.result(timeout=WATCHDOG_SECONDS)
    for closer in closers:
        closer.result()

    assert client._state.name == "CLOSED"
    with pytest.raises(RuntimeError, match="closing or closed"):
        client.offload(lambda: None)
    with (
        pytest.raises(RuntimeError, match="closing or closed"),
        client.admit(),
    ):
        pass


def _assert_aborted_close_released_everything(
    client: BoundedHttpClient,
    executor: ThreadPoolExecutor,
) -> None:
    """An aborted close is terminal, released, and refuses admission."""
    assert client._state.name == "CLOSED"
    assert client._client.is_closed is True
    assert executor._shutdown is True

    DaemonCall.start(client.close).result()

    with pytest.raises(RuntimeError, match="closing or closed"):
        client.offload(lambda: None)
    with (
        pytest.raises(RuntimeError, match="closing or closed"),
        client.admit(),
    ):
        pass


def test_an_interrupted_offload_drain_leaves_the_client_terminal() -> None:
    started = threading.Event()
    release = threading.Event()
    client, condition = _client_interrupted_in("DRAINING_OFFLOADS")

    def gated() -> None:
        started.set()
        wait_for(release, OFFLOADED_WORK_NOT_RELEASED)

    future = client.offload(gated)
    wait_for(started, OFFLOADED_WORK_DID_NOT_START)
    executor = client._executor
    assert executor is not None

    closer = DaemonCall.start(client.close)
    wait_for(condition.interrupted, CLOSE_WAIT_NOT_REACHED)
    with pytest.raises(KeyboardInterrupt, match="DRAINING_OFFLOADS"):
        closer.result()

    release.set()
    future.result(timeout=WATCHDOG_SECONDS)
    _assert_aborted_close_released_everything(client, executor)


def test_an_interrupted_work_drain_leaves_the_client_terminal() -> None:
    started = threading.Event()
    release = threading.Event()
    client, condition = _client_interrupted_in("CLOSING")
    client.offload(lambda: None).result(timeout=WATCHDOG_SECONDS)
    executor = client._executor
    assert executor is not None

    def gated_work() -> None:
        with client.admit():
            started.set()
            wait_for(release, OFFLOADED_WORK_NOT_RELEASED)

    worker = DaemonCall.start(gated_work)
    wait_for(started, OFFLOADED_WORK_DID_NOT_START)

    closer = DaemonCall.start(client.close)
    wait_for(condition.interrupted, CLOSE_WAIT_NOT_REACHED)
    with pytest.raises(KeyboardInterrupt, match="CLOSING"):
        closer.result()

    release.set()
    worker.result()
    _assert_aborted_close_released_everything(client, executor)


def test_an_interrupted_secondary_closer_leaves_the_primary_intact() -> None:
    started = threading.Event()
    release = threading.Event()
    client, condition = _client_interrupted_in("DRAINING_OFFLOADS")
    condition.only_thread_ident = NO_THREAD_IDENT

    def gated() -> None:
        started.set()
        wait_for(release, OFFLOADED_WORK_NOT_RELEASED)

    future = client.offload(gated)
    wait_for(started, OFFLOADED_WORK_DID_NOT_START)

    primary = DaemonCall.start(client.close)
    _wait_for_state(client, "DRAINING_OFFLOADS")

    def secondary_close() -> None:
        condition.only_thread_ident = threading.get_ident()
        client.close()

    secondary = DaemonCall.start(secondary_close)
    wait_for(condition.interrupted, SECONDARY_CLOSE_NOT_REACHED)
    with pytest.raises(KeyboardInterrupt, match="DRAINING_OFFLOADS"):
        secondary.result()

    assert client._client.is_closed is False
    assert client._state.name == "DRAINING_OFFLOADS"

    release.set()
    future.result(timeout=WATCHDOG_SECONDS)
    primary.result()
    assert client._state.name == "CLOSED"
    assert client._client.is_closed is True


class _RaisingCloseClient(httpx.Client):
    @override
    def close(self) -> None:
        raise RuntimeError(CLIENT_CLOSE_FAILED_MSG)


def test_abort_close_release_errors_do_not_mask_the_interrupt() -> None:
    started = threading.Event()
    release = threading.Event()
    inner = _RaisingCloseClient(transport=httpx.MockTransport(ok_handler))
    client, condition = _client_interrupted_in(
        "DRAINING_OFFLOADS", client=inner
    )

    def gated() -> None:
        started.set()
        wait_for(release, OFFLOADED_WORK_NOT_RELEASED)

    future = client.offload(gated)
    wait_for(started, OFFLOADED_WORK_DID_NOT_START)
    executor = client._executor
    assert executor is not None

    closer = DaemonCall.start(client.close)
    wait_for(condition.interrupted, CLOSE_WAIT_NOT_REACHED)
    with pytest.raises(KeyboardInterrupt, match="DRAINING_OFFLOADS"):
        closer.result()

    assert client._state.name == "CLOSED"
    assert executor._shutdown is True

    release.set()
    future.result(timeout=WATCHDOG_SECONDS)


def test_close_is_idempotent_and_closes_the_client_once() -> None:
    inner = _CloseCountingClient(transport=httpx.MockTransport(ok_handler))
    client = make_client(client=inner)

    client.close()
    client.close()

    assert inner.close_count == 1
    assert client._state.name == "CLOSED"


def test_the_context_manager_closes_on_exit() -> None:
    inner = _CloseCountingClient(transport=httpx.MockTransport(ok_handler))
    client = make_client(client=inner)

    with client as entered:
        assert entered is client
        _call(client)

    assert inner.close_count == 1
    assert client._state.name == "CLOSED"


def test_one_underlying_client_is_reused_across_calls() -> None:
    built: list[httpx.Client] = []

    def factory(**_kwargs: object) -> httpx.Client:
        inner = httpx.Client(transport=httpx.MockTransport(ok_handler))
        built.append(inner)
        return inner

    client = BoundedHttpClient(make_config(), client_factory=factory)

    with client:
        _call(client)
        _call(client)

    assert len(built) == 1


def test_admit_is_granted_while_open_and_refused_once_work_drains() -> None:
    client = make_client()

    with client.admit():
        assert client._active_work == 1
    assert client._active_work == 0

    client.close()
    with (
        pytest.raises(RuntimeError, match="closing or closed"),
        client.admit(),
    ):
        pass


def test_admit_releases_its_hold_when_the_body_raises() -> None:
    client = make_client()

    with (
        pytest.raises(ValueError, match=OFFLOADED_FAILURE_MSG),
        client.admit(),
    ):
        raise ValueError(OFFLOADED_FAILURE_MSG)

    assert client._active_work == 0
    client.close()
    assert client._state.name == "CLOSED"


def test_close_waits_for_admitted_work_to_finish() -> None:
    started = threading.Event()
    release = threading.Event()
    client = make_client()

    def gated_work() -> None:
        with client.admit():
            started.set()
            wait_for(release, OFFLOADED_WORK_NOT_RELEASED)

    worker = DaemonCall.start(gated_work)
    wait_for(started, OFFLOADED_WORK_DID_NOT_START)

    closer = DaemonCall.start(client.close)
    closer.wait_until_entered()
    _wait_for_state(client, "CLOSING")
    assert not closer.is_done()
    assert client._client.is_closed is False

    release.set()
    worker.result()
    closer.result()

    assert client._state.name == "CLOSED"
    assert client._client.is_closed is True


def test_call_takes_no_admission_of_its_own() -> None:
    """A bare call does not hold the drain open; admit() is the unit."""
    client = make_client()

    result = client.call(make_request())

    assert isinstance(result, WireResponse)
    assert client._active_work == 0

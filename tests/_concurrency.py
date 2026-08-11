from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

WATCHDOG_SECONDS = 10.0


@dataclass(frozen=True, slots=True)
class DaemonCall[ResultT]:
    """Run an event-gated call with time used only as a test watchdog."""

    _entered: threading.Event
    _done: threading.Event
    _thread: threading.Thread
    _results: list[ResultT]
    _errors: list[BaseException]

    @classmethod
    def start(cls, target: Callable[[], ResultT]) -> DaemonCall[ResultT]:
        entered = threading.Event()
        done = threading.Event()
        results: list[ResultT] = []
        errors: list[BaseException] = []

        def run() -> None:
            entered.set()
            try:
                results.append(target())
            except BaseException as error:  # noqa: BLE001 -- re-raised by result
                errors.append(error)
            finally:
                done.set()

        thread = threading.Thread(target=run, daemon=True)
        call = cls(
            _entered=entered,
            _done=done,
            _thread=thread,
            _results=results,
            _errors=errors,
        )
        thread.start()
        return call

    def wait_until_entered(self) -> None:
        if not self._entered.wait(timeout=WATCHDOG_SECONDS):
            raise TimeoutError("daemon call did not enter")

    def is_done(self) -> bool:
        return self._done.is_set()

    def result(self) -> ResultT:
        if not self._done.wait(timeout=WATCHDOG_SECONDS):
            raise TimeoutError("event-gated call exceeded the test watchdog")
        if self._errors:
            raise self._errors[0]
        return self._results[0]


def wait_for(event: threading.Event, description: str) -> None:
    if not event.wait(timeout=WATCHDOG_SECONDS):
        raise TimeoutError(description)

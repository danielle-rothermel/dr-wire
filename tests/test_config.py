from __future__ import annotations

import dataclasses
import math
from typing import Any

import pytest
from _support import make_config

from dr_http import HttpClientConfig

_TIMEOUT_FIELDS = (
    "timeout_seconds",
    "connect_timeout_seconds",
    "idle_timeout_seconds",
)
_COUNT_FIELDS = (
    "max_connections",
    "max_keepalive_connections",
    "max_request_bytes",
    "max_response_bytes",
)


def test_every_field_is_required() -> None:
    fields = {field.name for field in dataclasses.fields(HttpClientConfig)}

    assert fields == {*_TIMEOUT_FIELDS, *_COUNT_FIELDS}
    assert not [
        field
        for field in dataclasses.fields(HttpClientConfig)
        if field.default is not dataclasses.MISSING
        or field.default_factory is not dataclasses.MISSING
    ]


def test_a_valid_config_is_frozen_and_slotted() -> None:
    config = make_config()

    for field_name in (*_TIMEOUT_FIELDS, *_COUNT_FIELDS):
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(config, field_name, 1)
    assert not hasattr(config, "__dict__")


@pytest.mark.parametrize("field_name", _TIMEOUT_FIELDS)
@pytest.mark.parametrize(
    "value",
    [0.0, -1.0, math.inf, math.nan],
    ids=("zero", "negative", "infinite", "nan"),
)
def test_timeouts_must_be_positive_and_finite(
    field_name: str, value: float
) -> None:
    with pytest.raises(ValueError, match="positive and finite"):
        make_config(**{field_name: value})


@pytest.mark.parametrize("field_name", _TIMEOUT_FIELDS)
def test_timeouts_reject_non_numbers(field_name: str) -> None:
    with pytest.raises(TypeError, match="must be a real number"):
        make_config(**{field_name: "30"})


@pytest.mark.parametrize("field_name", _COUNT_FIELDS)
@pytest.mark.parametrize("value", [0, -1], ids=("zero", "negative"))
def test_counts_must_be_positive(field_name: str, value: int) -> None:
    with pytest.raises(ValueError, match="must be positive"):
        make_config(**{field_name: value})


@pytest.mark.parametrize("field_name", _COUNT_FIELDS)
@pytest.mark.parametrize("value", [1.5, True], ids=("float", "bool"))
def test_counts_must_be_integers(field_name: str, value: Any) -> None:
    with pytest.raises(TypeError, match="must be an integer"):
        make_config(**{field_name: value})


def test_keepalive_may_not_exceed_max_connections() -> None:
    with pytest.raises(ValueError, match="max_keepalive_connections"):
        make_config(max_connections=2, max_keepalive_connections=3)

    assert (
        make_config(
            max_connections=2, max_keepalive_connections=2
        ).max_keepalive_connections
        == 2
    )


@pytest.mark.parametrize(
    ("field_name", "message"),
    [
        ("connect_timeout_seconds", "connect_timeout_seconds"),
        ("idle_timeout_seconds", "idle_timeout_seconds"),
    ],
)
def test_phase_timeouts_are_rejected_rather_than_clamped(
    field_name: str, message: str
) -> None:
    """Clamping would silently change sizing the caller chose."""
    within: dict[str, Any] = dict.fromkeys(_TIMEOUT_FIELDS, 10.0)

    with pytest.raises(ValueError, match=message):
        make_config(**{**within, field_name: 11.0})

    assert getattr(make_config(**within), field_name) == 10.0


def test_very_large_finite_timeouts_are_accepted_unmodified() -> None:
    config = make_config(
        timeout_seconds=1e300,
        connect_timeout_seconds=1e300,
        idle_timeout_seconds=1e300,
    )

    assert config.timeout_seconds == 1e300
    assert config.connect_timeout_seconds == 1e300
    assert config.idle_timeout_seconds == 1e300

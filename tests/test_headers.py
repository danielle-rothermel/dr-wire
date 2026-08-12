from __future__ import annotations

import pytest

from dr_http import ParsedRetryAfter, is_dispatchable_url, parse_retry_after


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("12", ParsedRetryAfter(kind="delta_seconds", value=12)),
        ("00012", ParsedRetryAfter(kind="delta_seconds", value=12)),
        ("  12  ", ParsedRetryAfter(kind="delta_seconds", value=12)),
        ("0", ParsedRetryAfter(kind="delta_seconds", value=0)),
        (
            "Wed, 21 Oct 2015 07:28:00 GMT",
            ParsedRetryAfter(
                kind="http_date", value="Wed, 21 Oct 2015 07:28:00 GMT"
            ),
        ),
        (
            "Wed, 21 Oct 2015 09:28:00 +0200",
            ParsedRetryAfter(
                kind="http_date", value="Wed, 21 Oct 2015 07:28:00 GMT"
            ),
        ),
    ],
    ids=(
        "delta",
        "zero-padded-delta",
        "surrounding-space",
        "zero",
        "http-date",
        "offset-date-normalized-to-utc",
    ),
)
def test_parse_retry_after_accepts_both_declared_forms(
    header: str, expected: ParsedRetryAfter
) -> None:
    assert parse_retry_after(header) == expected


@pytest.mark.parametrize(
    "header",
    [
        None,
        "",
        "not a retry hint",
        "-5",
        "1.5",
        "١٢",
        "Wed, 21 Oct 2015 07:28:00",
    ],
    ids=(
        "none",
        "empty",
        "prose",
        "negative",
        "fractional",
        "non-ascii-digits",
        "naive-date",
    ),
)
def test_parse_retry_after_rejects_unparseable_values(
    header: str | None,
) -> None:
    assert parse_retry_after(header) is None


def test_parse_retry_after_applies_no_caps() -> None:
    """Bounding how far a hint reaches is the consumer's policy."""
    huge_delta = "9" * 400

    parsed = parse_retry_after(huge_delta)

    assert parsed == ParsedRetryAfter(
        kind="delta_seconds", value=int(huge_delta)
    )
    assert parse_retry_after("x" * 100_000) is None


@pytest.mark.parametrize(
    "header",
    ["9" * 5000, "Fri, 31 Dec 9999 23:59:59 -1400"],
    ids=("past-int-digit-limit", "year-overflows-utc-shift"),
)
def test_parse_retry_after_is_total_over_unconvertible_values(
    header: str,
) -> None:
    """A value the interpreter refuses to convert is unparseable, not fatal.

    ``int`` refuses a digit string past ``sys.get_int_max_str_digits``
    and shifting year 9999 to UTC overflows, so both escape the parse as
    exceptions unless the conversions are guarded.
    """
    assert parse_retry_after(header) is None


@pytest.mark.parametrize(
    "url",
    [
        "https://example.test",
        "http://example.test",
        "https://example.test/v1/chat/completions",
        "https://127.0.0.1:8080/path",
        "HTTPS://EXAMPLE.TEST",
    ],
    ids=(
        "https",
        "http",
        "with-path",
        "with-port",
        "uppercase-scheme",
    ),
)
def test_dispatchable_urls_are_accepted(url: str) -> None:
    assert is_dispatchable_url(url) is True


@pytest.mark.parametrize(
    "url",
    [
        "not-a-url-at-all",
        "ftp://example.test",
        "https://",
        "://nope",
        "",
        "/relative/path",
        "file:///etc/hosts",
    ],
    ids=(
        "no-scheme",
        "wrong-scheme",
        "no-host",
        "empty-scheme",
        "empty",
        "relative",
        "file-scheme",
    ),
)
def test_undispatchable_urls_are_refused_without_raising(url: str) -> None:
    assert is_dispatchable_url(url) is False


@pytest.mark.parametrize(
    "url",
    ["http://\x00bad.test", "https://exam\nple.test", "https://[::1x]"],
    ids=("null-byte", "newline", "malformed-ipv6"),
)
def test_a_url_httpx_rejects_outright_is_refused_not_raised(url: str) -> None:
    """The check is total: a URL httpx refuses to parse returns False."""
    assert is_dispatchable_url(url) is False

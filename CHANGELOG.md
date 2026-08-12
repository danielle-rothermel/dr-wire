# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-08-11

### Added

- `HttpClientConfig`: every resource bound one client enforces, with no
  defaults, validating positivity and finiteness, keep-alive against pool
  size, and phase timeouts against the general timeout.
- `BoundedHttpClient`: one long-lived synchronous client with a two-phase
  drain to a terminal close, an `admit()` context manager holding one unit of
  caller work against that drain, an `offload()` executor sized from
  `max_connections`, and a `call()` that is total for every wire-level
  condition and refuses a closed client with the package's own
  `RuntimeError`. A determined result outranks cleanup noise: once a result
  exists, a wire error raised while releasing the response stream is
  suppressed rather than replacing it, so a size refusal keeps its
  `Retry-After` hint. A stream failing while its final bytes are still
  arriving reports as a wire failure rather than as a truncated body. The
  response byte bound governs retained decoded bytes.
- `WireRequest`, `WireResponse`, `WireFailure`, and the closed
  `WireFailureKind` taxonomy describing one wire exchange as values. A body
  refused for size reports the bytes observed and both the parsed
  `Retry-After` hint its response head carried and that header's raw value,
  so a consumer whose retention policy bounds the header itself can apply the
  same rule on this path as on a response. `WireFailure.from_error` builds one
  failure from the exception that produced it.
- `parse_retry_after` and `is_dispatchable_url` as pure, uncapped, total
  parsing helpers. Totality covers values the interpreter itself refuses to
  convert, such as a digit string past the integer-conversion digit limit or a
  date whose year cannot be shifted to UTC.

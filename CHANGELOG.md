# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `HttpClientConfig`: every resource bound one client enforces, with no
  defaults, validating positivity and finiteness, keep-alive against pool
  size, and phase timeouts against the general timeout.
- `BoundedHttpClient`: one long-lived synchronous client with a two-phase
  drain to a terminal close, an `admit()` context manager holding one unit of
  caller work against that drain, an `offload()` executor sized from
  `max_connections`, and a `call()` that is total for every wire-level
  condition and refuses a closed client with the package's own
  `RuntimeError`.
- `WireRequest`, `WireResponse`, `WireFailure`, and the closed
  `WireFailureKind` taxonomy describing one wire exchange as values. A body
  refused for size reports the bytes observed and both the parsed
  `Retry-After` hint its response head carried and that header's raw value,
  so a consumer whose retention policy bounds the header itself can apply the
  same rule on this path as on a response. `WireFailure.from_error` builds one
  failure from the exception that produced it.
- `parse_retry_after` and `is_dispatchable_url` as pure, uncapped, total
  parsing helpers.

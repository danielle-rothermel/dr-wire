# dr-wire

A bounded, lifecycle-managed synchronous HTTP client core. `dr-wire` owns one
canonical client lifecycle: explicit bounds on connections and request bodies,
explicit timeouts on every phase, deterministic drain-on-close, and a closed
taxonomy of wire failures returned as values rather than leaked transport
exceptions. It is a foundational capability, isolated so that changing how
HTTP works is a visible boundary crossing; consumers depend on it
rev-pinned and update in lockstep.

The package depends on the standard library and `httpx` only. Nothing it
exposes is ever persisted: every value is an in-process description of one
wire exchange or one resource bound, and a consumer that records wire
outcomes maps these values into its own vocabulary at its own boundary.

## Surface

### `HttpClientConfig`

Every resource bound one client enforces, with no defaults, so an unsized
client cannot be constructed:

```python
from dr_wire import HttpClientConfig

config = HttpClientConfig(
    timeout_seconds=120.0,
    connect_timeout_seconds=30.0,
    idle_timeout_seconds=90.0,
    max_connections=10,
    max_keepalive_connections=5,
    max_request_bytes=1024 * 1024,
    max_response_bytes=8 * 1024 * 1024,
)
```

Timeouts must be positive and finite, pool sizes and byte limits positive
integers, `max_keepalive_connections` at most `max_connections`, and neither
phase timeout above the general timeout. A phase timeout above the general
timeout is rejected rather than clamped, because clamping would enforce a
bound different from the one the caller chose.

### `BoundedHttpClient`

```python
from dr_wire import BoundedHttpClient, WireRequest, WireResponse

with BoundedHttpClient(config) as client:
    with client.admit():
        result = client.call(
            WireRequest(
                method="POST",
                url="https://api.example.test/v1/chat",
                headers={"content-type": "application/json"},
                body=b'{"model": "m"}',
            )
        )
    if isinstance(result, WireResponse):
        ...
```

- `call(request)` is total for wire-level problems: it returns a
  `WireResponse` or a `WireFailure` and never raises for a timeout, a
  connection failure, a protocol violation, a refused byte bound, or an
  unrecognized `httpx` error. Calling once the work drain has begun raises
  `RuntimeError`, since reaching a closed client is a caller-lifecycle error
  rather than a wire condition; anything else it raises is a defect of this
  package. Every HTTP status, redirects included, comes back as a
  `WireResponse`; redirects are never followed and status meaning belongs to
  the caller.
- `admit()` holds one unit of *caller* work open against the drain. `call`
  takes no admission of its own, so a caller whose operation spans several
  wire calls wraps the whole operation and `close()` drains operations rather
  than individual calls.
- `offload(fn)` submits work to a client-owned executor created on first use
  and sized from `max_connections`, so worker count and pool size cannot
  disagree. Offloaded work must not call `close()` or `offload()` on the
  client running it.
- `close()` is two-phase: it stops offload admission, drains offloaded work,
  stops work admission, drains admitted work, shuts down the executor, and
  closes the underlying client exactly once, leaving the client terminal.
  An exception escaping either drain wait aborts the close: the client still
  becomes terminal and releases its resources, without claiming the drain
  completed.

### Wire values

`WireRequest`, `WireResponse`, `WireFailure`, and `WireFailureKind` describe
one exchange. `WireFailureKind` is a closed taxonomy — `INVALID_URL`,
`CONNECT_ERROR`, `NETWORK_ERROR`, `REMOTE_PROTOCOL_ERROR`,
`LOCAL_PROTOCOL_ERROR`, `TIMEOUT`, `STALLED_RESPONSE`, `POOL_TIMEOUT`,
`REQUEST_TOO_LARGE`, `RESPONSE_TOO_LARGE`, `UNKNOWN` — whose members are
never persisted.

### Parsing helpers

- `parse_retry_after(value)` returns a `ParsedRetryAfter` for a delta-seconds
  integer or a timezone-aware HTTP-date rendered canonically in UTC, and
  `None` otherwise. It applies no magnitude or length cap; bounding how far a
  hint may reach is the consumer's evidence policy.
- `is_dispatchable_url(url)` reports whether a URL carries an http or https
  scheme and a non-empty host. It is total and never raises.

## Definitions

`.defs/terms.toml` and `.defs/contracts.toml` hold this repository's shared
vocabulary and binding rules, rendered at
[the terms and contracts page](https://danielle-rothermel.github.io/dr-wire/).

## Checks

```sh
bash scripts/pre-check.sh
```

## License

MIT

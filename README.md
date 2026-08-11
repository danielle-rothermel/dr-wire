# dr-http

A bounded, lifecycle-managed synchronous HTTP client core. `dr-http` owns one
canonical client lifecycle: explicit bounds on connections and request bodies,
explicit timeouts on every phase, deterministic drain-on-close, and a closed
taxonomy of wire failures returned as values rather than leaked transport
exceptions. It is a foundational capability, isolated so that changing how
HTTP works is a visible boundary crossing; consumers depend on it
rev-pinned and update in lockstep.

The public surface arrives by pull request.

## Definitions

`.defs/terms.toml` and `.defs/contracts.toml` hold this repository's shared
vocabulary and binding rules, rendered at
[the terms and contracts page](https://danielle-rothermel.github.io/dr-http/).

## Checks

```sh
bash scripts/pre-check.sh
```

## License

MIT

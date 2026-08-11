#!/usr/bin/env bash

set -euo pipefail

repository_root="$({
    cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
    pwd -P
})"
cd -- "${repository_root}"

uv sync --locked
uv run --locked ruff format --check .
uv run --locked ruff check .
uv run --locked ty check
uv run --locked pytest
uvx tombi@1.2.5 lint --offline .defs/terms.toml
uv run --locked python scripts/check_defs.py

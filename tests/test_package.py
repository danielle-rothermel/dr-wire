from __future__ import annotations

import dr_wire


def test_package_exposes_version() -> None:
    assert isinstance(dr_wire.__version__, str)
    assert dr_wire.__version__


def test_every_public_export_resolves_exactly_once() -> None:
    assert len(set(dr_wire.__all__)) == len(dr_wire.__all__)
    for symbol in dr_wire.__all__:
        assert hasattr(dr_wire, symbol), symbol


def test_the_package_depends_on_nothing_beyond_the_stdlib_and_httpx() -> None:
    """The wire capability stays schema-light: no pydantic, no models."""
    import ast
    import pathlib
    import sys

    source_root = pathlib.Path(dr_wire.__file__).parent
    allowed = {"dr_wire", "httpx", *sys.stdlib_module_names}
    imported: set[str] = set()
    for path in source_root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(
                    alias.name.split(".")[0] for alias in node.names
                )
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imported.add(node.module.split(".")[0])

    assert imported <= allowed, sorted(imported - allowed)
    assert "pydantic" not in imported

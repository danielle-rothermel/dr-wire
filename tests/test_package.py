import dr_http


def test_package_exposes_version() -> None:
    assert isinstance(dr_http.__version__, str)
    assert dr_http.__version__


def test_public_surface_is_empty() -> None:
    assert dr_http.__all__ == []

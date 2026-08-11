from importlib.metadata import version

PACKAGE_NAME = "dr-http"

__all__: list[str] = []

__version__ = version(PACKAGE_NAME)

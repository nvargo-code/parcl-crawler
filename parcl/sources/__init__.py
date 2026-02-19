"""Source plugin registry."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parcl.sources.base import BaseSource

_REGISTRY: dict[str, type[BaseSource]] = {}


def register(source_type: str):
    """Decorator to register a source plugin class."""
    def wrapper(cls):
        _REGISTRY[source_type] = cls
        return cls
    return wrapper


def get_source_class(source_type: str) -> type[BaseSource]:
    """Look up a source class by type string."""
    # Lazy imports to populate registry
    if not _REGISTRY:
        from parcl.sources import socrata_source, arcgis_source, csv_source, pdf_source  # noqa: F401
    if source_type not in _REGISTRY:
        raise ValueError(
            f"Unknown source_type '{source_type}'. "
            f"Available: {list(_REGISTRY.keys())}"
        )
    return _REGISTRY[source_type]

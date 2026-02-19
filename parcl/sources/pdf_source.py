"""PDF source plugin — stub for future implementation."""

from __future__ import annotations

from typing import Any, Iterator

from parcl.sources import register
from parcl.sources.base import BaseSource


@register("pdf")
class PDFSource(BaseSource):
    """Placeholder for PDF document parsing (not yet implemented)."""

    def fetch(self) -> Iterator[list[dict[str, Any]]]:
        raise NotImplementedError(
            "PDF source parsing is planned for a future version. "
            "Consider using csv or socrata sources for structured data."
        )

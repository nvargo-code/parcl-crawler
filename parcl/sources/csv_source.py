"""Generic CSV download source plugin."""

from __future__ import annotations

import csv
import io
import tempfile
from typing import Any, Iterator

from parcl.config import CrawlerConfig, SourceConfig
from parcl.sources import register
from parcl.sources.base import BaseSource


@register("csv")
class CSVSource(BaseSource):
    """Downloads a CSV file and yields rows as dicts."""

    def fetch(self) -> Iterator[list[dict[str, Any]]]:
        url = self.config.base_url
        if self.config.dataset_id:
            url = f"{url.rstrip('/')}/{self.config.dataset_id}"

        self.log.info(f"Downloading CSV from {url}")
        resp = self.session.get(url, timeout=self.crawler.timeout_seconds, stream=True)
        resp.raise_for_status()

        # Write to temp file to handle large CSVs
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as tmp:
            for chunk in resp.iter_content(chunk_size=8192, decode_unicode=True):
                tmp.write(chunk)
            tmp.seek(0)

            reader = csv.DictReader(tmp)
            batch: list[dict[str, Any]] = []
            for row in reader:
                batch.append(dict(row))
                if len(batch) >= self.crawler.page_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

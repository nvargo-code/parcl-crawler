"""Socrata Open Data API source plugin."""

from __future__ import annotations

import os
from typing import Any, Iterator

from parcl.config import CrawlerConfig, SourceConfig
from parcl.sources import register
from parcl.sources.base import BaseSource


@register("socrata")
class SocrataSource(BaseSource):
    """Fetches data from Socrata JSON API with pagination and filters."""

    def __init__(self, source_config: SourceConfig, crawler_config: CrawlerConfig):
        super().__init__(source_config, crawler_config)
        # Optional app token from env
        token = os.environ.get("SOCRATA_APP_TOKEN", "")
        if token:
            self.session.headers["X-App-Token"] = token

    def fetch(self) -> Iterator[list[dict[str, Any]]]:
        base = self.config.base_url.rstrip("/")
        resource = self.config.dataset_id
        url = f"{base}/resource/{resource}.json"

        limit = self.crawler.page_size
        offset = 0

        for page_num in range(self.crawler.max_pages):
            params: dict[str, Any] = {
                "$limit": limit,
                "$offset": offset,
                "$order": ":id",
            }
            # Apply configured filters
            where_clauses = []
            for key, val in self.config.filters.items():
                if key == "$where":
                    where_clauses.append(str(val))
                elif key == "$select":
                    params["$select"] = val
                else:
                    params[key] = val
            if where_clauses:
                params["$where"] = " AND ".join(where_clauses)

            self.log.info(
                f"Fetching page {page_num + 1}: offset={offset}, limit={limit}"
            )
            resp = self.session.get(
                url, params=params, timeout=self.crawler.timeout_seconds
            )
            resp.raise_for_status()
            records = resp.json()

            if not records:
                self.log.info(f"No more records at offset {offset}")
                break

            yield records
            offset += limit

            if len(records) < limit:
                self.log.info(f"Last page ({len(records)} records)")
                break

            self._rate_limit()

"""Abstract base class for all data source plugins."""

from __future__ import annotations

import abc
import time
from typing import Any, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from parcl.config import CrawlerConfig, SourceConfig
from parcl.logger import get_logger


class BaseSource(abc.ABC):
    """Base class for all source plugins.

    Subclasses must implement `fetch()` which yields batches of raw dicts.
    """

    def __init__(self, source_config: SourceConfig, crawler_config: CrawlerConfig):
        self.config = source_config
        self.crawler = crawler_config
        self.log = get_logger(f"source.{source_config.id}")
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        """Build a requests session with retry and backoff."""
        session = requests.Session()
        retry = Retry(
            total=self.crawler.max_retries,
            backoff_factor=self.crawler.retry_backoff,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers["User-Agent"] = "parcl-crawler/0.1"
        return session

    def _rate_limit(self) -> None:
        """Sleep to respect rate limit."""
        if self.crawler.rate_limit_seconds > 0:
            time.sleep(self.crawler.rate_limit_seconds)

    @abc.abstractmethod
    def fetch(self) -> Iterator[list[dict[str, Any]]]:
        """Yield batches (pages) of raw records as dicts."""
        ...

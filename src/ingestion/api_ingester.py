"""REST API ingester — polls external APIs and writes records to Bronze."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
class APIConfig:
    base_url: str
    endpoint: str
    auth_header: str | None = None
    page_size: int = 500
    page_param: str = "page"
    limit_param: str = "limit"
    results_key: str = "results"
    next_key: str | None = "next"
    rate_limit_rps: float = 5.0
    timeout: int = 30
    headers: dict[str, str] = field(default_factory=dict)


class APIIngester:
    """Generic paginated REST API ingester with retry and rate-limiting."""

    def __init__(self, config: APIConfig) -> None:
        self.config = config
        self._session = self._build_session()
        self._last_call: float = 0.0

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist={429, 500, 502, 503, 504},
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        if self.config.auth_header:
            session.headers["Authorization"] = self.config.auth_header
        session.headers.update(self.config.headers)
        return session

    def _rate_limit(self) -> None:
        min_interval = 1.0 / self.config.rate_limit_rps
        elapsed = time.monotonic() - self._last_call
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_call = time.monotonic()

    def _fetch_page(self, params: dict[str, Any]) -> dict[str, Any]:
        self._rate_limit()
        url = urljoin(self.config.base_url, self.config.endpoint)
        resp = self._session.get(url, params=params, timeout=self.config.timeout)
        resp.raise_for_status()
        return resp.json()

    def paginate(self, extra_params: dict[str, Any] | None = None) -> Iterator[list[dict[str, Any]]]:
        """Yield pages of records until exhausted."""
        params: dict[str, Any] = {
            self.config.limit_param: self.config.page_size,
            **(extra_params or {}),
        }
        page = 1

        while True:
            params[self.config.page_param] = page
            logger.debug("Fetching page %d from %s", page, self.config.endpoint)

            data = self._fetch_page(params)
            records = data.get(self.config.results_key, data)

            if not records:
                break

            yield records

            # Follow cursor-based pagination if supported
            if self.config.next_key and data.get(self.config.next_key):
                next_url = data[self.config.next_key]
                # Extract params from next URL for cursor-based APIs
                if "?" in next_url:
                    from urllib.parse import parse_qs, urlparse
                    parsed = parse_qs(urlparse(next_url).query)
                    params.update({k: v[0] for k, v in parsed.items()})
                page += 1
            else:
                # Standard offset pagination ends when fewer records than page_size
                if len(records) < self.config.page_size:
                    break
                page += 1

    def ingest_all(self, extra_params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Collect all records across all pages (use only for small datasets)."""
        all_records: list[dict[str, Any]] = []
        for page in self.paginate(extra_params):
            all_records.extend(page)
        logger.info("Ingested %d records from %s", len(all_records), self.config.endpoint)
        return all_records

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OpenFDAFetchResult:
    records: list[dict[str, Any]]
    api_status: str
    total_available: int | None
    pages_retrieved: int


class OpenFDAClient:
    def __init__(
        self,
        base_url: str,
        date_field: str = "receivedate",
        page_size: int = 100,
        max_pages_per_run: int = 200,
        request_timeout_seconds: int = 60,
        sleep_seconds_between_requests: float = 0.2,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url
        self.date_field = date_field
        self.page_size = min(page_size, 100)
        self.max_pages_per_run = max_pages_per_run
        self.request_timeout_seconds = request_timeout_seconds
        self.sleep_seconds_between_requests = sleep_seconds_between_requests
        self.session = session or self._build_session()

    @staticmethod
    def _build_session() -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _build_search_expression(self, window_start: date, window_end: date) -> str:
        # openFDA expects spaces around TO; requests will URL-encode them correctly.
        return f"{self.date_field}:[{window_start:%Y%m%d} TO {window_end:%Y%m%d}]"

    def _request_page(self, search_expression: str, skip: int, limit: int) -> dict[str, Any] | None:
        response = self.session.get(
            self.base_url,
            params={"search": search_expression, "limit": limit, "skip": skip},
            timeout=self.request_timeout_seconds,
        )

        if response.status_code == 404:
            try:
                payload = response.json()
            except ValueError:
                payload = {}

            error_message = str(payload.get("error", {}).get("message", "")).lower()
            if "no matches found" in error_message:
                logger.info("No openFDA records found for search expression %s", search_expression)
                return None

        response.raise_for_status()
        return response.json()

    def fetch_reports_by_window(
        self,
        window_start: date,
        window_end: date,
        page_size: int | None = None,
        max_pages: int | None = None,
    ) -> OpenFDAFetchResult:
        limit = min(page_size or self.page_size, 100)
        page_cap = max_pages or self.max_pages_per_run
        search_expression = self._build_search_expression(window_start, window_end)

        records: list[dict[str, Any]] = []
        total_available: int | None = None
        pages_retrieved = 0

        logger.info(
            "Starting openFDA extraction window_start=%s window_end=%s limit=%s max_pages=%s",
            window_start,
            window_end,
            limit,
            page_cap,
        )

        for page_index in range(page_cap):
            skip = page_index * limit
            payload = self._request_page(search_expression, skip=skip, limit=limit)

            if payload is None:
                if not records:
                    return OpenFDAFetchResult(
                        records=[],
                        api_status="NO_DATA",
                        total_available=0,
                        pages_retrieved=0,
                    )
                break

            page_records = payload.get("results", [])
            meta_results = payload.get("meta", {}).get("results", {})
            total_available = int(meta_results.get("total", len(page_records)))

            if not page_records:
                break

            records.extend(page_records)
            pages_retrieved += 1

            logger.info(
                "Fetched page=%s rows=%s cumulative_rows=%s total_available=%s",
                page_index + 1,
                len(page_records),
                len(records),
                total_available,
            )

            if len(records) >= total_available or len(page_records) < limit:
                break

            if self.sleep_seconds_between_requests > 0:
                time.sleep(self.sleep_seconds_between_requests)

        if total_available is not None and len(records) < total_available:
            raise RuntimeError(
                "openFDA extraction stopped before all records were retrieved. "
                f"Fetched {len(records)} of {total_available} rows for "
                f"{window_start:%Y-%m-%d} to {window_end:%Y-%m-%d}. "
                "Increase OPENFDA_MAX_PAGES_PER_RUN or narrow the extraction window."
            )

        return OpenFDAFetchResult(
            records=records,
            api_status="SUCCESS",
            total_available=total_available,
            pages_retrieved=pages_retrieved,
        )

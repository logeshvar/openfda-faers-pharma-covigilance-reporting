from __future__ import annotations

from datetime import date
from typing import Any

from src.ingestion.openfda_client import OpenFDAClient


class FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self) -> dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, params: dict[str, Any], timeout: int) -> FakeResponse:
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        skip = int(params["skip"])
        limit = int(params["limit"])
        total = 568
        remaining = max(total - skip, 0)
        rows = min(limit, remaining)
        payload = {
            "meta": {"results": {"skip": skip, "limit": limit, "total": total}},
            "results": [{"safetyreportid": f"case-{skip + index}"} for index in range(rows)],
        }
        return FakeResponse(status_code=200, payload=payload)


def test_fetch_reports_auto_extends_low_page_cap_to_complete_batch() -> None:
    session = FakeSession()
    client = OpenFDAClient(
        base_url="https://api.fda.gov/drug/event.json",
        page_size=100,
        max_pages_per_run=5,
        sleep_seconds_between_requests=0,
        session=session,  # type: ignore[arg-type]
    )

    result = client.fetch_reports_by_window(
        window_start=date(2025, 12, 27),
        window_end=date(2025, 12, 27),
        page_size=100,
        max_pages=5,
    )

    assert result.api_status == "SUCCESS"
    assert result.total_available == 568
    assert result.pages_retrieved == 6
    assert len(result.records) == 568
    assert [call["params"]["skip"] for call in session.calls] == [0, 100, 200, 300, 400, 500]

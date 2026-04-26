from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from src.common.config import AppConfig
from src.common.path_builders import build_ingest_batch_id, build_local_stage_path, build_raw_file_name
from src.ingestion.openfda_client import OpenFDAClient

logger = logging.getLogger(__name__)

DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


@dataclass
class ExtractionResult:
    ingest_batch_id: str
    source_name: str
    query_window_start: str
    query_window_end: str
    source_file_name: str
    load_timestamp: str
    api_status: str
    records_fetched: int
    records: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self, include_records: bool = True) -> dict[str, Any]:
        payload = asdict(self)
        if not include_records:
            payload.pop("records", None)
        return payload


@dataclass(frozen=True)
class ExtractionManifest:
    staged_file_path: str
    ingest_batch_id: str
    source_name: str
    source_file_name: str
    query_window_start: str
    query_window_end: str
    load_timestamp: str
    api_status: str
    records_fetched: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_window_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.strptime(value, DATE_FORMAT).date()


def extract_openfda_window(
    config: AppConfig,
    window_start: str | date,
    window_end: str | date,
    ingest_batch_id: str | None = None,
    page_size: int | None = None,
    max_pages: int | None = None,
) -> ExtractionResult:
    parsed_window_start = parse_window_date(window_start)
    parsed_window_end = parse_window_date(window_end)

    if parsed_window_end < parsed_window_start:
        raise ValueError("query_window_end must be on or after query_window_start")

    resolved_ingest_batch_id = ingest_batch_id or build_ingest_batch_id(
        source_name=config.ingestion.source_name,
        window_start=parsed_window_start,
        window_end=parsed_window_end,
    )
    load_timestamp = datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)
    source_file_name = build_raw_file_name(config.ingestion.source_name, resolved_ingest_batch_id)

    client = OpenFDAClient(
        base_url=config.openfda.base_url,
        date_field=config.openfda.date_field,
        page_size=config.openfda.page_size,
        max_pages_per_run=config.openfda.max_pages_per_run,
        request_timeout_seconds=config.openfda.request_timeout_seconds,
        sleep_seconds_between_requests=config.openfda.sleep_seconds_between_requests,
    )

    fetch_result = client.fetch_reports_by_window(
        window_start=parsed_window_start,
        window_end=parsed_window_end,
        page_size=page_size,
        max_pages=max_pages,
    )

    logger.info(
        "Completed openFDA extraction ingest_batch_id=%s api_status=%s records_fetched=%s",
        resolved_ingest_batch_id,
        fetch_result.api_status,
        len(fetch_result.records),
    )

    return ExtractionResult(
        ingest_batch_id=resolved_ingest_batch_id,
        source_name=config.ingestion.source_name,
        query_window_start=parsed_window_start.strftime(DATE_FORMAT),
        query_window_end=parsed_window_end.strftime(DATE_FORMAT),
        source_file_name=source_file_name,
        load_timestamp=load_timestamp,
        api_status=fetch_result.api_status,
        records_fetched=len(fetch_result.records),
        records=fetch_result.records,
    )


def stage_extraction_result(
    extraction_result: ExtractionResult,
    staging_dir: str | Path,
) -> ExtractionManifest:
    stage_path = build_local_stage_path(staging_dir, extraction_result.ingest_batch_id)
    stage_path.parent.mkdir(parents=True, exist_ok=True)

    with stage_path.open("w", encoding="utf-8") as handle:
        json.dump(extraction_result.to_dict(include_records=True), handle, indent=2)

    logger.info("Staged extraction manifest to %s", stage_path)

    return ExtractionManifest(
        staged_file_path=str(stage_path),
        ingest_batch_id=extraction_result.ingest_batch_id,
        source_name=extraction_result.source_name,
        source_file_name=extraction_result.source_file_name,
        query_window_start=extraction_result.query_window_start,
        query_window_end=extraction_result.query_window_end,
        load_timestamp=extraction_result.load_timestamp,
        api_status=extraction_result.api_status,
        records_fetched=extraction_result.records_fetched,
    )


def load_staged_extraction(staged_file_path: str | Path) -> ExtractionResult:
    path = Path(staged_file_path).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"Staged extraction file does not exist: {path}")

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    return ExtractionResult(
        ingest_batch_id=str(payload["ingest_batch_id"]),
        source_name=str(payload["source_name"]),
        query_window_start=str(payload["query_window_start"]),
        query_window_end=str(payload["query_window_end"]),
        source_file_name=str(payload["source_file_name"]),
        load_timestamp=str(payload["load_timestamp"]),
        api_status=str(payload["api_status"]),
        records_fetched=int(payload["records_fetched"]),
        records=list(payload.get("records", [])),
    )


def cleanup_staged_extraction(staged_file_path: str | Path) -> bool:
    path = Path(staged_file_path).expanduser()
    if not path.exists():
        return False

    path.unlink()
    logger.info("Removed staged extraction file %s", path)
    return True

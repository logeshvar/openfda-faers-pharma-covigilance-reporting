from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

import boto3

from src.common.config import AppConfig, S3Settings
from src.common.path_builders import build_raw_s3_key, build_s3_uri
from src.ingestion.extract_openfda import ExtractionResult, parse_window_date

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RawWriteResult:
    bucket_name: str
    records_written: int
    source_file_name: str
    s3_key: str | None
    s3_uri: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _build_s3_client(s3_settings: S3Settings):
    client_kwargs: dict[str, Any] = {"service_name": "s3", "region_name": s3_settings.region_name}
    if s3_settings.endpoint_url:
        client_kwargs["endpoint_url"] = s3_settings.endpoint_url
    if s3_settings.access_key_id:
        client_kwargs["aws_access_key_id"] = s3_settings.access_key_id
    if s3_settings.secret_access_key:
        client_kwargs["aws_secret_access_key"] = s3_settings.secret_access_key
    return boto3.client(**client_kwargs)


def _build_raw_record_envelopes(extraction_result: ExtractionResult) -> list[dict[str, Any]]:
    return [
        {
            "raw_payload": raw_record,
            "safetyreportid": raw_record.get("safetyreportid"),
            "safetyreportversion": raw_record.get("safetyreportversion"),
            "receivedate": raw_record.get("receivedate"),
            "ingest_batch_id": extraction_result.ingest_batch_id,
            "source_file_name": extraction_result.source_file_name,
            "api_query_window_start": extraction_result.query_window_start,
            "api_query_window_end": extraction_result.query_window_end,
            "load_timestamp": extraction_result.load_timestamp,
        }
        for raw_record in extraction_result.records
    ]


def write_raw_batch_to_s3(config: AppConfig, extraction_result: ExtractionResult) -> RawWriteResult:
    if extraction_result.records_fetched == 0:
        logger.info(
            "No raw openFDA records to write for ingest_batch_id=%s",
            extraction_result.ingest_batch_id,
        )
        return RawWriteResult(
            bucket_name=config.s3.bucket_name,
            records_written=0,
            source_file_name=extraction_result.source_file_name,
            s3_key=None,
            s3_uri=None,
        )

    query_window_start = parse_window_date(extraction_result.query_window_start)
    query_window_end = parse_window_date(extraction_result.query_window_end)
    raw_s3_key = build_raw_s3_key(
        raw_prefix=config.s3.raw_prefix,
        source_name=extraction_result.source_name,
        window_start=query_window_start,
        window_end=query_window_end,
        ingest_batch_id=extraction_result.ingest_batch_id,
        file_name=extraction_result.source_file_name,
    )
    payload = "\n".join(
        json.dumps(record, separators=(",", ":"), ensure_ascii=False)
        for record in _build_raw_record_envelopes(extraction_result)
    )
    client = _build_s3_client(config.s3)
    client.put_object(
        Bucket=config.s3.bucket_name,
        Key=raw_s3_key,
        Body=payload.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    logger.info(
        "Wrote raw openFDA batch to s3_uri=%s",
        build_s3_uri(config.s3.bucket_name, raw_s3_key),
    )

    return RawWriteResult(
        bucket_name=config.s3.bucket_name,
        records_written=extraction_result.records_fetched,
        source_file_name=extraction_result.source_file_name,
        s3_key=raw_s3_key,
        s3_uri=build_s3_uri(config.s3.bucket_name, raw_s3_key),
    )

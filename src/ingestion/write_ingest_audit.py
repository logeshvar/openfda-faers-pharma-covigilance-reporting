from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from typing import Any

import boto3

from src.common.config import AppConfig, S3Settings
from src.common.path_builders import build_audit_s3_key, build_s3_uri
from src.dq.raw_checks import DQSummary
from src.ingestion.extract_openfda import ExtractionResult, parse_window_date
from src.ingestion.write_raw_s3 import RawWriteResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuditWriteResult:
    bucket_name: str
    s3_key: str
    s3_uri: str

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


def build_ingest_audit_record(
    extraction_result: ExtractionResult,
    raw_write_result: RawWriteResult,
    dq_summary: DQSummary,
) -> dict[str, Any]:
    failed_checks = [
        check.check_name for check in dq_summary.check_results if check.status == "FAIL"
    ]

    return {
        "ingest_batch_id": extraction_result.ingest_batch_id,
        "source_name": extraction_result.source_name,
        "query_window_start": extraction_result.query_window_start,
        "query_window_end": extraction_result.query_window_end,
        "records_fetched": extraction_result.records_fetched,
        "records_written": raw_write_result.records_written,
        "api_status": extraction_result.api_status,
        "source_file_name": extraction_result.source_file_name,
        "load_timestamp": extraction_result.load_timestamp,
        "dq_status": dq_summary.overall_status,
        "dq_failed_checks": failed_checks,
        "raw_s3_uri": raw_write_result.s3_uri,
    }


def write_ingest_audit_to_s3(
    config: AppConfig,
    extraction_result: ExtractionResult,
    audit_record: dict[str, Any],
) -> AuditWriteResult:
    window_start = parse_window_date(extraction_result.query_window_start)
    window_end = parse_window_date(extraction_result.query_window_end)
    audit_s3_key = build_audit_s3_key(
        audit_prefix=config.s3.audit_prefix,
        source_name=extraction_result.source_name,
        window_start=window_start,
        window_end=window_end,
        ingest_batch_id=extraction_result.ingest_batch_id,
    )
    client = _build_s3_client(config.s3)
    client.put_object(
        Bucket=config.s3.bucket_name,
        Key=audit_s3_key,
        Body=json.dumps(audit_record, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(
        "Wrote ingest audit record to s3_uri=%s",
        build_s3_uri(config.s3.bucket_name, audit_s3_key),
    )

    return AuditWriteResult(
        bucket_name=config.s3.bucket_name,
        s3_key=audit_s3_key,
        s3_uri=build_s3_uri(config.s3.bucket_name, audit_s3_key),
    )

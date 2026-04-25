from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path


def _slugify(value: str) -> str:
    return value.strip().lower().replace(" ", "_").replace("-", "_")


def build_ingest_batch_id(
    source_name: str,
    window_start: date,
    window_end: date,
    run_started_at: datetime | None = None,
) -> str:
    timestamp = (run_started_at or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return f"{_slugify(source_name)}_{window_start:%Y%m%d}_{window_end:%Y%m%d}_{timestamp}"


def build_raw_file_name(source_name: str, ingest_batch_id: str) -> str:
    return f"{_slugify(source_name)}_raw_{ingest_batch_id}.ndjson"


def build_audit_file_name(source_name: str, ingest_batch_id: str) -> str:
    return f"{_slugify(source_name)}_audit_{ingest_batch_id}.json"


def build_raw_s3_key(
    raw_prefix: str,
    source_name: str,
    window_start: date,
    window_end: date,
    ingest_batch_id: str,
    file_name: str | None = None,
) -> str:
    resolved_file_name = file_name or build_raw_file_name(source_name, ingest_batch_id)
    return "/".join(
        [
            raw_prefix.strip("/"),
            f"query_year={window_start:%Y}",
            f"query_month={window_start:%m}",
            f"window_start={window_start:%Y-%m-%d}",
            f"window_end={window_end:%Y-%m-%d}",
            f"ingest_batch_id={ingest_batch_id}",
            resolved_file_name,
        ]
    )


def build_audit_s3_key(
    audit_prefix: str,
    source_name: str,
    window_start: date,
    window_end: date,
    ingest_batch_id: str,
    file_name: str | None = None,
) -> str:
    resolved_file_name = file_name or build_audit_file_name(source_name, ingest_batch_id)
    return "/".join(
        [
            audit_prefix.strip("/"),
            f"source_name={_slugify(source_name)}",
            f"query_year={window_start:%Y}",
            f"query_month={window_start:%m}",
            f"window_start={window_start:%Y-%m-%d}",
            f"window_end={window_end:%Y-%m-%d}",
            resolved_file_name,
        ]
    )


def build_local_stage_path(staging_dir: str | Path, ingest_batch_id: str) -> Path:
    return Path(staging_dir).expanduser() / f"{ingest_batch_id}.json"


def build_table_storage_key(layer_prefix: str, table_name: str) -> str:
    return "/".join([layer_prefix.strip("/"), table_name.strip("/")])


def build_s3_uri(bucket_name: str, key: str) -> str:
    return f"s3://{bucket_name}/{key.lstrip('/')}"


def build_table_s3_uri(bucket_name: str, layer_prefix: str, table_name: str) -> str:
    return build_s3_uri(bucket_name, build_table_storage_key(layer_prefix, table_name))

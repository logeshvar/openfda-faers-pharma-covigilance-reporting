from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

import boto3

from src.common.config import AppConfig, S3Settings
from src.common.path_builders import build_s3_uri, build_table_s3_uri
from src.curated.build_case_header import (
    CASE_HEADER_KEY_COLUMNS,
    CASE_HEADER_PARTITION_COLUMNS,
    CURATED_CASE_HEADER_TABLE,
)
from src.ingestion.extract_openfda import parse_window_date

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RawBatchObject:
    bucket_name: str
    s3_key: str
    s3_uri: str
    source_file_name: str
    ingest_batch_id: str
    query_window_start: str
    query_window_end: str
    size_bytes: int | None
    last_modified: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CaseHeaderJobManifest:
    table_name: str
    query_window_start: str
    query_window_end: str
    raw_input_s3_uri: str
    raw_input_s3_key: str
    selected_ingest_batch_id: str
    curated_output_s3_uri: str
    key_columns: tuple[str, ...]
    partition_columns: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_job_args(self) -> dict[str, str]:
        return {
            "raw_input_path": self.raw_input_s3_uri,
            "output_path": self.curated_output_s3_uri,
        }


def _build_s3_client(s3_settings: S3Settings):
    client_kwargs: dict[str, Any] = {"service_name": "s3", "region_name": s3_settings.region_name}
    if s3_settings.endpoint_url:
        client_kwargs["endpoint_url"] = s3_settings.endpoint_url
    if s3_settings.access_key_id:
        client_kwargs["aws_access_key_id"] = s3_settings.access_key_id
    if s3_settings.secret_access_key:
        client_kwargs["aws_secret_access_key"] = s3_settings.secret_access_key
    return boto3.client(**client_kwargs)


def build_raw_window_prefix(raw_prefix: str, window_start: date, window_end: date) -> str:
    return "/".join(
        [
            raw_prefix.strip("/"),
            f"query_year={window_start:%Y}",
            f"query_month={window_start:%m}",
            f"window_start={window_start:%Y-%m-%d}",
            f"window_end={window_end:%Y-%m-%d}",
        ]
    )


def _extract_ingest_batch_id_from_key(s3_key: str) -> str:
    for segment in s3_key.split("/"):
        if segment.startswith("ingest_batch_id="):
            return segment.split("=", 1)[1]
    raise ValueError(f"Could not determine ingest_batch_id from s3 key: {s3_key}")


def list_raw_batch_objects_for_window(
    config: AppConfig,
    window_start: str | date,
    window_end: str | date,
) -> list[RawBatchObject]:
    parsed_window_start = parse_window_date(window_start)
    parsed_window_end = parse_window_date(window_end)
    prefix = build_raw_window_prefix(
        raw_prefix=config.s3.raw_prefix,
        window_start=parsed_window_start,
        window_end=parsed_window_end,
    )
    client = _build_s3_client(config.s3)

    paginator = client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=config.s3.bucket_name, Prefix=prefix)

    batch_objects: list[RawBatchObject] = []
    for page in page_iterator:
        for item in page.get("Contents", []):
            s3_key = str(item["Key"])
            if not s3_key.endswith(".ndjson"):
                continue

            batch_objects.append(
                RawBatchObject(
                    bucket_name=config.s3.bucket_name,
                    s3_key=s3_key,
                    s3_uri=build_s3_uri(config.s3.bucket_name, s3_key),
                    source_file_name=s3_key.rsplit("/", 1)[-1],
                    ingest_batch_id=_extract_ingest_batch_id_from_key(s3_key),
                    query_window_start=parsed_window_start.isoformat(),
                    query_window_end=parsed_window_end.isoformat(),
                    size_bytes=int(item.get("Size", 0)),
                    last_modified=item.get("LastModified").isoformat()
                    if item.get("LastModified")
                    else None,
                )
            )

    logger.info(
        "Located %s raw batch object(s) for window_start=%s window_end=%s under prefix=%s",
        len(batch_objects),
        parsed_window_start,
        parsed_window_end,
        prefix,
    )
    return batch_objects


def select_raw_batch_object(
    batch_objects: list[RawBatchObject],
    ingest_batch_id: str | None = None,
) -> RawBatchObject:
    if not batch_objects:
        raise FileNotFoundError("No raw batch objects were found for the requested window.")

    if ingest_batch_id:
        for batch_object in batch_objects:
            if batch_object.ingest_batch_id == ingest_batch_id:
                return batch_object
        raise FileNotFoundError(
            f"No raw batch object matched ingest_batch_id={ingest_batch_id!r} for the requested window."
        )

    return max(batch_objects, key=lambda batch_object: batch_object.ingest_batch_id)


def build_case_header_job_manifest(
    config: AppConfig,
    raw_batch_object: RawBatchObject,
) -> CaseHeaderJobManifest:
    return CaseHeaderJobManifest(
        table_name=CURATED_CASE_HEADER_TABLE,
        query_window_start=raw_batch_object.query_window_start,
        query_window_end=raw_batch_object.query_window_end,
        raw_input_s3_uri=raw_batch_object.s3_uri,
        raw_input_s3_key=raw_batch_object.s3_key,
        selected_ingest_batch_id=raw_batch_object.ingest_batch_id,
        curated_output_s3_uri=build_table_s3_uri(
            bucket_name=config.s3.bucket_name,
            layer_prefix=config.s3.curated_prefix,
            table_name=CURATED_CASE_HEADER_TABLE,
        ),
        key_columns=CASE_HEADER_KEY_COLUMNS,
        partition_columns=CASE_HEADER_PARTITION_COLUMNS,
    )


def resolve_case_header_job_manifest(
    config: AppConfig,
    window_start: str | date,
    window_end: str | date,
    ingest_batch_id: str | None = None,
) -> CaseHeaderJobManifest:
    batch_objects = list_raw_batch_objects_for_window(
        config=config,
        window_start=window_start,
        window_end=window_end,
    )
    selected_batch = select_raw_batch_object(batch_objects, ingest_batch_id=ingest_batch_id)
    manifest = build_case_header_job_manifest(config=config, raw_batch_object=selected_batch)

    logger.info(
        "Prepared case header job manifest ingest_batch_id=%s raw_input=%s output=%s",
        manifest.selected_ingest_batch_id,
        manifest.raw_input_s3_uri,
        manifest.curated_output_s3_uri,
    )
    return manifest

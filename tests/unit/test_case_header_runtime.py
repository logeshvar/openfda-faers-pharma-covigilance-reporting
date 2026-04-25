from __future__ import annotations

from datetime import date
from pathlib import Path

import boto3
from moto import mock_aws

from src.common.config import (
    AppConfig,
    DatabricksSettings,
    DQSettings,
    IngestionSettings,
    LoggingSettings,
    OpenFDASettings,
    ProjectSettings,
    S3Settings,
)
from src.curated.case_header_runtime import (
    build_databricks_tasks_for_curated_manifests,
    build_raw_window_prefix,
    list_raw_batch_objects_for_window,
    resolve_case_header_job_manifest,
    resolve_curated_job_manifests,
)


def _build_test_config(endpoint_url: str | None = None) -> AppConfig:
    return AppConfig(
        project=ProjectSettings(name="pharma-pv-pipeline", environment="test"),
        s3=S3Settings(
            bucket_name="pharma-pv-test",
            region_name="us-east-1",
            endpoint_url=endpoint_url,
            raw_prefix="raw/openfda/drug_event",
            audit_prefix="ops/audit",
            curated_prefix="curated",
            gold_prefix="gold",
            access_key_id="testing",
            secret_access_key="testing",
        ),
        openfda=OpenFDASettings(
            base_url="https://api.fda.gov/drug/event.json",
            date_field="receivedate",
            page_size=100,
            max_pages_per_run=200,
            request_timeout_seconds=60,
            sleep_seconds_between_requests=0.0,
        ),
        databricks=DatabricksSettings(
            submit_enabled=False,
            host=None,
            token=None,
            run_name_prefix="pharma-cv",
            python_file_base_uri="s3://pharma-cv-test/jobs/src",
            spark_version="14.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=1,
        ),
        ingestion=IngestionSettings(
            source_name="openfda_drug_event",
            schedule="@monthly",
            local_staging_dir=Path("/tmp/openfda"),
        ),
        dq=DQSettings(min_expected_records=0, required_raw_fields=("safetyreportid",)),
        logging=LoggingSettings(level="INFO"),
        config_path=Path("/tmp/dev.yaml"),
        root_dir=Path("/tmp"),
    )


@mock_aws
def test_list_raw_batch_objects_for_window_filters_to_requested_window() -> None:
    config = _build_test_config()
    client = boto3.client("s3", region_name=config.s3.region_name)
    client.create_bucket(Bucket=config.s3.bucket_name)

    requested_window_prefix = build_raw_window_prefix(
        config.s3.raw_prefix,
        window_start=date(2026, 3, 1),
        window_end=date(2026, 3, 31),
    )
    other_window_prefix = build_raw_window_prefix(
        config.s3.raw_prefix,
        window_start=date(2026, 2, 1),
        window_end=date(2026, 2, 28),
    )

    client.put_object(
        Bucket=config.s3.bucket_name,
        Key=f"{requested_window_prefix}/ingest_batch_id=batch_001/file_001.ndjson",
        Body=b"{}",
    )
    client.put_object(
        Bucket=config.s3.bucket_name,
        Key=f"{requested_window_prefix}/ingest_batch_id=batch_001/_SUCCESS",
        Body=b"",
    )
    client.put_object(
        Bucket=config.s3.bucket_name,
        Key=f"{other_window_prefix}/ingest_batch_id=batch_999/file_999.ndjson",
        Body=b"{}",
    )

    raw_batches = list_raw_batch_objects_for_window(
        config=config,
        window_start="2026-03-01",
        window_end="2026-03-31",
    )

    assert len(raw_batches) == 1
    assert raw_batches[0].ingest_batch_id == "batch_001"
    assert raw_batches[0].query_window_start == "2026-03-01"
    assert raw_batches[0].query_window_end == "2026-03-31"


@mock_aws
def test_resolve_case_header_job_manifest_selects_latest_batch_by_default() -> None:
    config = _build_test_config()
    client = boto3.client("s3", region_name=config.s3.region_name)
    client.create_bucket(Bucket=config.s3.bucket_name)

    requested_window_prefix = build_raw_window_prefix(
        config.s3.raw_prefix,
        window_start=date(2026, 3, 1),
        window_end=date(2026, 3, 31),
    )

    client.put_object(
        Bucket=config.s3.bucket_name,
        Key=f"{requested_window_prefix}/ingest_batch_id=openfda_drug_event_20260301_20260331_20260421T030000Z/older.ndjson",
        Body=b"{}",
    )
    client.put_object(
        Bucket=config.s3.bucket_name,
        Key=f"{requested_window_prefix}/ingest_batch_id=openfda_drug_event_20260301_20260331_20260421T040000Z/newer.ndjson",
        Body=b"{}",
    )

    manifest = resolve_case_header_job_manifest(
        config=config,
        window_start="2026-03-01",
        window_end="2026-03-31",
    )

    assert manifest.selected_ingest_batch_id == "openfda_drug_event_20260301_20260331_20260421T040000Z"
    assert manifest.raw_input_s3_uri.endswith("/newer.ndjson")
    assert manifest.curated_output_s3_uri == "s3://pharma-cv-test/curated/curated_case_header"


@mock_aws
def test_resolve_curated_job_manifests_prepares_milestone_2_and_3_tables() -> None:
    config = _build_test_config()
    client = boto3.client("s3", region_name=config.s3.region_name)
    client.create_bucket(Bucket=config.s3.bucket_name)
    requested_window_prefix = build_raw_window_prefix(
        config.s3.raw_prefix,
        window_start=date(2026, 3, 1),
        window_end=date(2026, 3, 31),
    )
    client.put_object(
        Bucket=config.s3.bucket_name,
        Key=f"{requested_window_prefix}/ingest_batch_id=batch_001/file_001.ndjson",
        Body=b"{}",
    )

    manifests = resolve_curated_job_manifests(
        config=config,
        window_start="2026-03-01",
        window_end="2026-03-31",
    )

    assert [manifest.table_name for manifest in manifests] == [
        "curated_case_header",
        "curated_primary_source",
        "curated_patient_demo",
    ]
    assert [manifest.curated_output_s3_uri for manifest in manifests] == [
        "s3://pharma-cv-test/curated/curated_case_header",
        "s3://pharma-cv-test/curated/curated_primary_source",
        "s3://pharma-cv-test/curated/curated_patient_demo",
    ]


def test_build_databricks_tasks_for_curated_manifests_builds_spark_python_tasks() -> None:
    config = _build_test_config()
    manifests = [
        {
            "table_name": "curated_primary_source",
            "python_file": "curated/build_primary_source.py",
            "raw_input_s3_uri": "s3://pharma-cv-test/raw/file.ndjson",
            "curated_output_s3_uri": "s3://pharma-cv-test/curated/curated_primary_source",
        }
    ]

    tasks = build_databricks_tasks_for_curated_manifests(config=config, manifests=manifests)

    assert tasks == [
        {
            "task_key": "build_curated_primary_source",
            "job_cluster_key": "curated_job_cluster",
            "spark_python_task": {
                "python_file": "s3://pharma-cv-test/jobs/src/curated/build_primary_source.py",
                "parameters": [
                    "--raw-input-path",
                    "s3://pharma-cv-test/raw/file.ndjson",
                    "--output-path",
                    "s3://pharma-cv-test/curated/curated_primary_source",
                ],
            },
        }
    ]

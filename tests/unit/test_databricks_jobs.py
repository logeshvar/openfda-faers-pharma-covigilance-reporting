from __future__ import annotations

from pathlib import Path

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
from src.common.databricks_jobs import build_databricks_submit_run_payload, submit_databricks_run


def _build_test_config(submit_enabled: bool = False) -> AppConfig:
    return AppConfig(
        project=ProjectSettings(name="pharma-cv-pipeline", environment="test"),
        s3=S3Settings(
            bucket_name="pharma-cv-test",
            region_name="us-east-1",
            endpoint_url=None,
            raw_prefix="raw/openfda/drug_event",
            audit_prefix="ops/audit",
            curated_prefix="curated",
            gold_prefix="gold",
            access_key_id=None,
            secret_access_key=None,
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
            submit_enabled=submit_enabled,
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


def test_build_databricks_submit_run_payload_wraps_tasks_in_job_cluster() -> None:
    config = _build_test_config()

    payload = build_databricks_submit_run_payload(
        config=config,
        run_name="pharma-cv_curated_2026-03-01_2026-03-31",
        tasks=[{"task_key": "build_curated_case_header"}],
    )

    assert payload["run_name"] == "pharma-cv_curated_2026-03-01_2026-03-31"
    assert payload["job_clusters"][0]["job_cluster_key"] == "curated_job_cluster"
    assert payload["job_clusters"][0]["new_cluster"]["spark_version"] == "14.3.x-scala2.12"
    assert payload["tasks"] == [{"task_key": "build_curated_case_header"}]


def test_submit_databricks_run_returns_dry_run_result_when_submission_disabled() -> None:
    config = _build_test_config(submit_enabled=False)

    result = submit_databricks_run(config=config, payload={"run_name": "test"})

    assert result.submitted is False
    assert result.run_id is None
    assert result.message == "Databricks submission disabled; prepared payload only."

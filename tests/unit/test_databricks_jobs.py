from __future__ import annotations

from pathlib import Path

from src.common.config import (
    AppConfig,
    DatabricksSettings,
    DQSettings,
    IngestionSettings,
    LoggingSettings,
    MetadataSettings,
    OpenFDASettings,
    ProjectSettings,
    S3Settings,
)
from src.common.databricks_jobs import (
    build_databricks_saved_job_settings,
    build_databricks_smoke_test_payload,
    build_databricks_submit_run_payload,
    submit_databricks_run,
)


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
            execution_source="git",
            git_url="https://github.com/logeshvar/openfda-faers-pharma-covigilance-reporting.git",
            git_provider="gitHub",
            git_branch="main",
            python_file_base_path="src",
            instance_profile_arn="arn:aws:iam::123456789012:instance-profile/test",
        ),
        metadata=MetadataSettings(
            glue_database_name="pharma_cv_test",
            athena_results_s3_uri="s3://pharma-cv-test/ops/athena-results/",
        ),
        ingestion=IngestionSettings(
            source_name="openfda_drug_event",
            schedule="@monthly",
            local_staging_dir=Path("/tmp/openfda"),
            cleanup_staging_files=True,
        ),
        dq=DQSettings(min_expected_records=0, required_raw_fields=("safetyreportid",)),
        logging=LoggingSettings(level="INFO"),
        config_path=Path("/tmp/dev.yaml"),
        root_dir=Path("/tmp"),
    )


def test_build_databricks_submit_run_payload_materializes_task_clusters() -> None:
    config = _build_test_config()

    payload = build_databricks_submit_run_payload(
        config=config,
        run_name="pharma-cv_curated_2026-03-01_2026-03-31",
        tasks=[{"task_key": "build_curated_case_header"}],
    )

    assert payload["run_name"] == "pharma-cv_curated_2026-03-01_2026-03-31"
    assert "job_clusters" not in payload
    assert payload["tasks"][0]["new_cluster"]["spark_version"] == "14.3.x-scala2.12"
    assert payload["tasks"][0]["new_cluster"]["spark_conf"] == {
        "spark.databricks.delta.properties.defaults.enableDeletionVectors": "false"
    }
    assert (
        payload["tasks"][0]["new_cluster"]["aws_attributes"]["instance_profile_arn"]
        == "arn:aws:iam::123456789012:instance-profile/test"
    )
    assert payload["git_source"] == {
        "git_url": "https://github.com/logeshvar/openfda-faers-pharma-covigilance-reporting.git",
        "git_provider": "gitHub",
        "git_branch": "main",
    }
    assert payload["tasks"][0]["task_key"] == "build_curated_case_header"


def test_submit_databricks_run_returns_dry_run_result_when_submission_disabled() -> None:
    config = _build_test_config(submit_enabled=False)

    result = submit_databricks_run(config=config, payload={"run_name": "test"})

    assert result.submitted is False
    assert result.run_id is None
    assert result.message == "Databricks submission disabled or dry-run requested; prepared payload only."


def test_build_databricks_smoke_test_payload_uses_git_source() -> None:
    config = _build_test_config()

    payload = build_databricks_smoke_test_payload(config=config, run_name="smoke")

    assert payload["git_source"]["git_provider"] == "gitHub"
    assert payload["tasks"][0]["spark_python_task"]["python_file"] == "src/databricks/smoke_test_s3_access.py"
    assert payload["tasks"][0]["spark_python_task"]["source"] == "GIT"
    assert "job_cluster_key" not in payload["tasks"][0]
    assert payload["tasks"][0]["new_cluster"]["spark_version"] == "14.3.x-scala2.12"


def test_build_databricks_saved_job_settings_uses_shared_job_cluster() -> None:
    config = _build_test_config()

    settings = build_databricks_saved_job_settings(
        config=config,
        job_name="pharma-cv-curated-gold",
        tasks=[
            {
                "task_key": "build_curated_case_header",
                "job_cluster_key": "pharma_cv_job_cluster",
                "spark_python_task": {
                    "python_file": "src/curated/build_case_header.py",
                    "source": "GIT",
                    "parameters": [],
                },
            }
        ],
    )

    assert settings["name"] == "pharma-cv-curated-gold"
    assert settings["max_concurrent_runs"] == 1
    assert settings["job_clusters"][0]["job_cluster_key"] == "pharma_cv_job_cluster"
    assert settings["job_clusters"][0]["new_cluster"]["spark_version"] == "14.3.x-scala2.12"
    assert settings["job_clusters"][0]["new_cluster"]["spark_conf"] == {
        "spark.databricks.delta.properties.defaults.enableDeletionVectors": "false"
    }
    assert settings["tasks"][0]["job_cluster_key"] == "pharma_cv_job_cluster"
    assert "new_cluster" not in settings["tasks"][0]
    assert settings["git_source"]["git_branch"] == "main"

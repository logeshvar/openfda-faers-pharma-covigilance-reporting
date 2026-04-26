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
from src.metadata.athena_validation_queries import render_validation_queries
from src.metadata.glue_refresh import (
    build_athena_create_delta_table_sql,
    build_delta_table_registrations,
)


def _build_test_config() -> AppConfig:
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
            submit_enabled=False,
            host=None,
            token=None,
            run_name_prefix="pharma-cv",
            python_file_base_uri="s3://pharma-cv-test/jobs/src",
            spark_version="14.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=1,
        ),
        metadata=MetadataSettings(
            glue_database_name="pharma_cv_test",
            athena_results_s3_uri="s3://pharma-cv-test/ops/athena-results/",
        ),
        ingestion=IngestionSettings("openfda_drug_event", "@monthly", Path("/tmp/openfda"), True),
        dq=DQSettings(min_expected_records=0, required_raw_fields=("safetyreportid",)),
        logging=LoggingSettings(level="INFO"),
        config_path=Path("/tmp/dev.yaml"),
        root_dir=Path("/tmp"),
    )


def test_build_delta_table_registrations_includes_curated_and_gold_tables() -> None:
    registrations = build_delta_table_registrations(_build_test_config())

    assert len(registrations) == 11
    assert registrations[0].table_name == "curated_case_header"
    assert registrations[-1].table_name == "gold_manufacturer_class_serious_trends"


def test_build_athena_create_delta_table_sql_sets_delta_property() -> None:
    registration = build_delta_table_registrations(_build_test_config())[0]

    sql = build_athena_create_delta_table_sql("pharma_cv_test", registration)

    assert "CREATE EXTERNAL TABLE IF NOT EXISTS pharma_cv_test.curated_case_header" in sql
    assert "TBLPROPERTIES ('table_type'='DELTA')" in sql


def test_render_validation_queries_qualifies_tables_with_database() -> None:
    rendered = render_validation_queries("pharma_cv_test")

    assert "FROM pharma_cv_test.curated_case_header" in rendered["curated_case_header_row_count"]
    assert "FROM pharma_cv_test.gold_drug_reaction_trends" in rendered["gold_top_drug_reactions"]

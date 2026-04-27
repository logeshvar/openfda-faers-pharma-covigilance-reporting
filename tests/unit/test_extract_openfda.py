from __future__ import annotations

from dataclasses import replace
from datetime import date
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
from src.ingestion import extract_openfda
from src.ingestion.openfda_client import OpenFDAFetchResult


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
            python_file_base_uri=None,
            spark_version="14.3.x-scala2.12",
            node_type_id="i3.xlarge",
            num_workers=1,
        ),
        metadata=MetadataSettings(
            glue_database_name="pharma_cv_test",
            athena_results_s3_uri="s3://pharma-cv-test/ops/athena-results/",
        ),
        ingestion=IngestionSettings(
            source_name="openfda_drug_event",
            schedule="@daily",
            local_staging_dir=Path("/tmp/openfda"),
            cleanup_staging_files=True,
        ),
        dq=DQSettings(min_expected_records=0, required_raw_fields=("safetyreportid",)),
        logging=LoggingSettings(level="INFO"),
        config_path=Path("/tmp/dev.yaml"),
        root_dir=Path("/tmp"),
    )


def test_extract_openfda_window_fetches_multi_day_window_as_daily_chunks(monkeypatch) -> None:
    calls: list[tuple[date, date]] = []

    class FakeClient:
        def __init__(self, **kwargs) -> None:
            pass

        def fetch_reports_by_window(
            self,
            window_start: date,
            window_end: date,
            page_size: int | None = None,
            max_pages: int | None = None,
        ) -> OpenFDAFetchResult:
            calls.append((window_start, window_end))
            return OpenFDAFetchResult(
                records=[{"safetyreportid": window_start.isoformat()}],
                api_status="SUCCESS",
                total_available=1,
                pages_retrieved=1,
            )

    monkeypatch.setattr(extract_openfda, "OpenFDAClient", FakeClient)

    result = extract_openfda.extract_openfda_window(
        config=replace(_build_test_config()),
        window_start="2025-12-27",
        window_end="2025-12-29",
        ingest_batch_id="batch-1",
    )

    assert calls == [
        (date(2025, 12, 27), date(2025, 12, 27)),
        (date(2025, 12, 28), date(2025, 12, 28)),
        (date(2025, 12, 29), date(2025, 12, 29)),
    ]
    assert result.api_status == "SUCCESS"
    assert result.records_fetched == 3

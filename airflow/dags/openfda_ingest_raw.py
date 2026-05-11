from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from src.common.config import load_config
from src.common.path_builders import build_ingest_batch_id
from src.dq.raw_checks import raise_for_failed_checks, run_raw_checks, run_raw_checks_from_iterable
from src.ingestion.extract_openfda import (
    cleanup_staged_extraction,
    extract_openfda_window,
    load_staged_extraction,
    parse_window_date,
    stage_extraction_result,
)
from src.ingestion.windowing import resolve_lagged_daily_window
from src.ingestion.write_ingest_audit import build_ingest_audit_record, write_ingest_audit_to_s3
from src.ingestion.write_raw_s3 import RawWriteResult, write_raw_batch_to_s3, write_staged_raw_file_to_s3

logger = logging.getLogger(__name__)
CONFIG = load_config()


def _default_window_from_context(context: dict[str, Any]) -> tuple[str, str]:
    window_start, window_end = resolve_lagged_daily_window(
        reference_date=context["logical_date"],
        source_lag_days=CONFIG.ingestion.source_lag_days,
        default_window_days=CONFIG.ingestion.default_window_days,
    )
    return window_start.isoformat(), window_end.isoformat()


def _iter_raw_payloads_from_staged_ndjson(staged_raw_file_path: str):
    with Path(staged_raw_file_path).expanduser().open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            envelope = json.loads(line)
            yield envelope.get("raw_payload", {})


@dag(
    dag_id="openfda_ingest_raw",
    schedule=CONFIG.ingestion.schedule,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["openfda", "raw", "ingestion"],
    default_args={"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=5)},
    doc_md="""
    ## Raw openFDA ingestion DAG

    openFDA FAERS data is published quarterly and can lag by 3+ months. Scheduled
    runs therefore ingest a small stable window behind a configurable lag instead
    of querying the newest month or a full quarter.

    Manual backfill overrides can be supplied with `dag_run.conf`. Multi-day
    windows are fetched from openFDA as daily chunks and landed as one raw batch.
    If `max_pages` is lower than the actual page count needed for a day, the
    extractor auto-extends to avoid an incomplete landing.

    ```json
    {
      "window_start": "2026-03-01",
      "window_end": "2026-03-31",
      "page_size": 100,
      "max_pages": 200
    }
    ```
    """,
)
def openfda_ingest_raw():
    @task
    def resolve_runtime_options() -> dict[str, Any]:
        context = get_current_context()
        dag_run = context.get("dag_run")
        dag_run_conf = dag_run.conf if dag_run and dag_run.conf else {}

        default_window_start, default_window_end = _default_window_from_context(context)
        window_start = dag_run_conf.get("window_start", default_window_start)
        window_end = dag_run_conf.get("window_end", default_window_end)
        parsed_window_start = parse_window_date(window_start)
        parsed_window_end = parse_window_date(window_end)

        if parsed_window_end < parsed_window_start:
            raise ValueError("window_end must be on or after window_start")

        ingest_batch_id = build_ingest_batch_id(
            source_name=CONFIG.ingestion.source_name,
            window_start=parsed_window_start,
            window_end=parsed_window_end,
        )

        return {
            "window_start": parsed_window_start.isoformat(),
            "window_end": parsed_window_end.isoformat(),
            "page_size": int(dag_run_conf.get("page_size", CONFIG.openfda.page_size)),
            "max_pages": int(dag_run_conf.get("max_pages", CONFIG.openfda.max_pages_per_run)),
            "ingest_batch_id": ingest_batch_id,
        }

    @task
    def extract_and_stage(runtime_options: dict[str, Any]) -> dict[str, Any]:
        extraction_result = extract_openfda_window(
            config=CONFIG,
            window_start=runtime_options["window_start"],
            window_end=runtime_options["window_end"],
            ingest_batch_id=runtime_options["ingest_batch_id"],
            page_size=runtime_options["page_size"],
            max_pages=runtime_options["max_pages"],
        )
        manifest = stage_extraction_result(
            extraction_result=extraction_result,
            staging_dir=CONFIG.ingestion.local_staging_dir,
        )
        return manifest.to_dict()

    @task
    def validate_and_persist(stage_manifest: dict[str, Any]) -> dict[str, Any]:
        extraction_result = load_staged_extraction(stage_manifest["staged_file_path"])
        staged_raw_file_path = stage_manifest.get("staged_raw_file_path")
        if staged_raw_file_path:
            dq_summary = run_raw_checks_from_iterable(
                records=_iter_raw_payloads_from_staged_ndjson(staged_raw_file_path),
                required_fields=CONFIG.dq.required_raw_fields,
                min_expected_records=CONFIG.dq.min_expected_records,
            )
        else:
            dq_summary = run_raw_checks(
                records=extraction_result.records,
                required_fields=CONFIG.dq.required_raw_fields,
                min_expected_records=CONFIG.dq.min_expected_records,
            )

        raw_write_result = RawWriteResult(
            bucket_name=CONFIG.s3.bucket_name,
            records_written=0,
            source_file_name=extraction_result.source_file_name,
            s3_key=None,
            s3_uri=None,
        )

        if dq_summary.overall_status == "PASS":
            if staged_raw_file_path:
                raw_write_result = write_staged_raw_file_to_s3(
                    CONFIG,
                    extraction_result,
                    staged_raw_file_path,
                )
            else:
                raw_write_result = write_raw_batch_to_s3(CONFIG, extraction_result)

        audit_record = build_ingest_audit_record(
            extraction_result=extraction_result,
            raw_write_result=raw_write_result,
            dq_summary=dq_summary,
        )
        audit_write_result = write_ingest_audit_to_s3(
            config=CONFIG,
            extraction_result=extraction_result,
            audit_record=audit_record,
        )

        logger.info(
            "Ingestion batch complete ingest_batch_id=%s api_status=%s dq_status=%s raw_s3_uri=%s audit_s3_uri=%s",
            extraction_result.ingest_batch_id,
            extraction_result.api_status,
            dq_summary.overall_status,
            raw_write_result.s3_uri,
            audit_write_result.s3_uri,
        )

        if CONFIG.ingestion.cleanup_staging_files:
            cleanup_staged_extraction(stage_manifest["staged_file_path"])

        raise_for_failed_checks(dq_summary)

        return {
            "ingest_batch_id": extraction_result.ingest_batch_id,
            "dq_summary": dq_summary.to_dict(),
            "raw_write_result": raw_write_result.to_dict(),
            "audit_write_result": audit_write_result.to_dict(),
        }

    runtime_options = resolve_runtime_options()
    staged_extract = extract_and_stage(runtime_options)
    validate_and_persist(staged_extract)


openfda_ingest_raw()

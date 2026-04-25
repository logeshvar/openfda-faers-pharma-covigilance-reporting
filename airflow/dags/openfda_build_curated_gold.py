from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from src.common.config import load_config
from src.curated.case_header_runtime import resolve_case_header_job_manifest
from src.ingestion.extract_openfda import parse_window_date

logger = logging.getLogger(__name__)
CONFIG = load_config()


def _default_window_from_context(context: dict[str, Any]) -> tuple[str, str]:
    interval_start = context.get("data_interval_start")
    interval_end = context.get("data_interval_end")

    if interval_start and interval_end:
        return interval_start.date().isoformat(), (interval_end - timedelta(days=1)).date().isoformat()

    logical_date = context["logical_date"].date()
    previous_month_end = logical_date.replace(day=1) - timedelta(days=1)
    previous_month_start = previous_month_end.replace(day=1)
    return previous_month_start.isoformat(), previous_month_end.isoformat()


@dag(
    dag_id="openfda_build_curated_gold",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["openfda", "curated", "gold"],
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=5)},
    doc_md="""
    ## Curated and gold build DAG

    Milestone 2 currently implements the first curated step:
    - locate the latest raw batch for a reporting window
    - prepare the `curated_case_header` job manifest

    Manual overrides can be supplied with `dag_run.conf`:

    ```json
    {
      "window_start": "2026-03-01",
      "window_end": "2026-03-31",
      "ingest_batch_id": "openfda_drug_event_20260301_20260331_20260421T035810Z"
    }
    ```
    """,
)
def openfda_build_curated_gold():
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

        return {
            "window_start": parsed_window_start.isoformat(),
            "window_end": parsed_window_end.isoformat(),
            "ingest_batch_id": dag_run_conf.get("ingest_batch_id"),
        }

    @task
    def prepare_case_header_job(runtime_options: dict[str, Any]) -> dict[str, Any]:
        manifest = resolve_case_header_job_manifest(
            config=CONFIG,
            window_start=runtime_options["window_start"],
            window_end=runtime_options["window_end"],
            ingest_batch_id=runtime_options.get("ingest_batch_id"),
        )

        logger.info(
            "Prepared curated_case_header manifest ingest_batch_id=%s raw_input=%s output=%s",
            manifest.selected_ingest_batch_id,
            manifest.raw_input_s3_uri,
            manifest.curated_output_s3_uri,
        )
        return manifest.to_dict()

    @task
    def log_execution_handoff(case_header_manifest: dict[str, Any]) -> None:
        logger.info(
            "Milestone 2 handoff ready for Spark/Databricks execution: table=%s raw_input=%s output=%s key_columns=%s partition_columns=%s",
            case_header_manifest["table_name"],
            case_header_manifest["raw_input_s3_uri"],
            case_header_manifest["curated_output_s3_uri"],
            case_header_manifest["key_columns"],
            case_header_manifest["partition_columns"],
        )

    runtime_options = resolve_runtime_options()
    case_header_manifest = prepare_case_header_job(runtime_options)
    log_execution_handoff(case_header_manifest)


openfda_build_curated_gold()

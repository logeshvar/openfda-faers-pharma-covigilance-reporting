from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from src.common.databricks_jobs import build_databricks_submit_run_payload, submit_databricks_run
from src.common.config import load_config
from src.curated.case_header_runtime import (
    build_databricks_tasks_for_gold,
    build_databricks_tasks_for_curated_manifests,
    resolve_curated_job_manifests,
)
from src.ingestion.extract_openfda import parse_window_date
from src.ingestion.windowing import resolve_lagged_daily_window

logger = logging.getLogger(__name__)
CONFIG = load_config()


def _default_window_from_context(context: dict[str, Any]) -> tuple[str, str]:
    window_start, window_end = resolve_lagged_daily_window(
        reference_date=context["logical_date"],
        source_lag_days=CONFIG.ingestion.source_lag_days,
        default_window_days=CONFIG.ingestion.default_window_days,
    )
    return window_start.isoformat(), window_end.isoformat()


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

    Current build scope:
    - `curated_case_header`
    - `curated_primary_source`
    - `curated_patient_demo`
    - `curated_case_drug`
    - `curated_case_drug_openfda`
    - `curated_case_reaction`
    - gold reporting tables

    Manual overrides can be supplied with `dag_run.conf`:

    ```json
    {
      "window_start": "2025-03-01",
      "window_end": "2025-03-01",
      "ingest_batch_id": "openfda_drug_event_20250301_20250301_20260426T114125Z"
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
            "airflow_run_id": context["run_id"],
            "dry_run": bool(dag_run_conf.get("dry_run", False)),
        }

    @task
    def prepare_curated_jobs(runtime_options: dict[str, Any]) -> list[dict[str, Any]]:
        manifests = resolve_curated_job_manifests(
            config=CONFIG,
            window_start=runtime_options["window_start"],
            window_end=runtime_options["window_end"],
            ingest_batch_id=runtime_options.get("ingest_batch_id"),
        )

        logger.info(
            "Prepared %s curated manifest(s) for ingest_batch_id=%s",
            len(manifests),
            manifests[0].selected_ingest_batch_id if manifests else None,
        )
        return [manifest.to_dict() for manifest in manifests]

    @task
    def build_databricks_payload(
        runtime_options: dict[str, Any],
        curated_manifests: list[dict[str, Any]],
    ) -> dict[str, Any]:
        batch_id = curated_manifests[0]["selected_ingest_batch_id"] if curated_manifests else None
        tasks = build_databricks_tasks_for_curated_manifests(
            config=CONFIG,
            manifests=curated_manifests,
            run_id=runtime_options["airflow_run_id"],
        ) + build_databricks_tasks_for_gold(
            config=CONFIG,
            run_id=runtime_options["airflow_run_id"],
            window_start=runtime_options["window_start"],
            window_end=runtime_options["window_end"],
            batch_id=batch_id,
        )
        run_name = (
            f"{CONFIG.databricks.run_name_prefix}_curated_gold_"
            f"{curated_manifests[0]['query_window_start']}_{curated_manifests[0]['query_window_end']}"
        )
        return build_databricks_submit_run_payload(config=CONFIG, run_name=run_name, tasks=tasks)

    @task
    def submit_or_log_databricks_run(runtime_options: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        config = load_config(include_databricks_secrets=not runtime_options["dry_run"])
        result = submit_databricks_run(
            config=config,
            payload=payload,
            dry_run=runtime_options["dry_run"],
        )
        logger.info(
            "Databricks curated run result submitted=%s run_id=%s url=%s message=%s",
            result.submitted,
            result.run_id,
            result.run_page_url,
            result.message,
        )
        return result.to_dict()

    runtime_options = resolve_runtime_options()
    curated_manifests = prepare_curated_jobs(runtime_options)
    databricks_payload = build_databricks_payload(runtime_options, curated_manifests)
    submit_or_log_databricks_run(runtime_options, databricks_payload)


openfda_build_curated_gold()

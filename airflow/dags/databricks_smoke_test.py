from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from src.common.config import load_config
from src.common.databricks_jobs import (
    build_databricks_smoke_test_payload,
    build_layer_base_paths,
    submit_databricks_run,
)
from src.common.path_builders import build_s3_uri

logger = logging.getLogger(__name__)
CONFIG = load_config()


@dag(
    dag_id="databricks_smoke_test",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["databricks", "smoke-test"],
    default_args={"owner": "data-eng", "retries": 0, "retry_delay": timedelta(minutes=5)},
    doc_md="""
    ## Databricks smoke test

    Submits a single Git-backed Databricks Python task that validates cluster startup,
    instance-profile S3 access, and write access to the ops zone.

    Optional manual config:

    ```json
    {
      "dry_run": true,
      "bucket_path": "s3://pharma-cv-prod/",
      "ops_base_path": "s3://pharma-cv-prod/ops"
    }
    ```
    """,
)
def databricks_smoke_test():
    @task
    def build_payload() -> dict[str, Any]:
        context = get_current_context()
        dag_run = context.get("dag_run")
        dag_run_conf = dag_run.conf if dag_run and dag_run.conf else {}
        base_paths = build_layer_base_paths(CONFIG)
        run_name = f"{CONFIG.databricks.run_name_prefix}_smoke_test_{context['run_id']}"

        return {
            "payload": build_databricks_smoke_test_payload(
                config=CONFIG,
                run_name=run_name,
                bucket_path=dag_run_conf.get("bucket_path", build_s3_uri(CONFIG.s3.bucket_name, "")),
                ops_base_path=dag_run_conf.get("ops_base_path", base_paths["ops_base_path"]),
            ),
            "dry_run": bool(dag_run_conf.get("dry_run", False)),
        }

    @task
    def submit_or_log(payload_bundle: dict[str, Any]) -> dict[str, Any]:
        config = load_config(include_databricks_secrets=not payload_bundle["dry_run"])
        result = submit_databricks_run(
            config=config,
            payload=payload_bundle["payload"],
            dry_run=payload_bundle["dry_run"],
        )
        logger.info(
            "Databricks smoke test result submitted=%s run_id=%s url=%s message=%s",
            result.submitted,
            result.run_id,
            result.run_page_url,
            result.message,
        )
        return result.to_dict()

    submit_or_log(build_payload())


databricks_smoke_test()

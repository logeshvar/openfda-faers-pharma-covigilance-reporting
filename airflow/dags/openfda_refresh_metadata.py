from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from src.common.config import load_config
from src.metadata.glue_refresh import (
    build_athena_create_delta_table_sql,
    build_delta_table_registrations,
    refresh_glue_delta_tables,
)

CONFIG = load_config()


@dag(
    dag_id="openfda_refresh_metadata",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["openfda", "glue", "athena", "metadata"],
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=5)},
    doc_md="""
    ## Glue and Athena metadata refresh

    By default this DAG prepares the Delta table DDL only. To execute the
    Athena DDL statements, trigger with:

    ```json
    {"execute_refresh": true}
    ```
    """,
)
def openfda_refresh_metadata():
    @task
    def resolve_runtime_options() -> dict[str, Any]:
        context = get_current_context()
        dag_run = context.get("dag_run")
        dag_run_conf = dag_run.conf if dag_run and dag_run.conf else {}

        return {
            "execute_refresh": bool(dag_run_conf.get("execute_refresh", False)),
            "glue_database_name": CONFIG.metadata.glue_database_name,
        }

    @task
    def prepare_ddl(runtime_options: dict[str, Any]) -> list[str]:
        return [
            build_athena_create_delta_table_sql(CONFIG.metadata.glue_database_name, registration)
            for registration in build_delta_table_registrations(CONFIG)
        ]

    @task
    def execute_or_log_refresh(runtime_options: dict[str, Any], ddl_statements: list[str]) -> dict[str, Any]:
        if not runtime_options["execute_refresh"]:
            return {
                "executed": False,
                "glue_database_name": runtime_options["glue_database_name"],
                "ddl_statement_count": len(ddl_statements),
                "query_execution_ids": [],
            }

        query_execution_ids = refresh_glue_delta_tables(CONFIG)
        return {
            "executed": True,
            "glue_database_name": runtime_options["glue_database_name"],
            "ddl_statement_count": len(ddl_statements),
            "query_execution_ids": query_execution_ids,
        }

    options = resolve_runtime_options()
    ddl = prepare_ddl(options)
    execute_or_log_refresh(options, ddl)


openfda_refresh_metadata()

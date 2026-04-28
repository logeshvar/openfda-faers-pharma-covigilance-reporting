from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from typing import Any

import boto3

from src.common.config import AppConfig, load_config
from src.common.path_builders import build_table_s3_uri


@dataclass(frozen=True)
class DeltaTableRegistration:
    table_name: str
    s3_uri: str
    layer: str

    def to_dict(self) -> dict[str, Any]:
        return {"table_name": self.table_name, "s3_uri": self.s3_uri, "layer": self.layer}


CURATED_TABLES = (
    "curated_case_header",
    "curated_primary_source",
    "curated_patient_demo",
    "curated_case_drug",
    "curated_case_drug_openfda",
    "curated_case_reaction",
)

GOLD_TABLES = (
    "gold_latest_case_helper",
    "gold_case_seriousness_trends",
    "gold_drug_reaction_trends",
    "gold_reaction_demographic_trends",
    "gold_manufacturer_class_serious_trends",
)


def build_delta_table_registrations(config: AppConfig) -> list[DeltaTableRegistration]:
    registrations: list[DeltaTableRegistration] = []

    for table_name in CURATED_TABLES:
        registrations.append(
            DeltaTableRegistration(
                table_name=table_name,
                s3_uri=build_table_s3_uri(config.s3.bucket_name, config.s3.curated_prefix, table_name),
                layer="curated",
            )
        )
    for table_name in GOLD_TABLES:
        registrations.append(
            DeltaTableRegistration(
                table_name=table_name,
                s3_uri=build_table_s3_uri(config.s3.bucket_name, config.s3.gold_prefix, table_name),
                layer="gold",
            )
        )
    return registrations


def build_athena_create_delta_table_sql(database_name: str, registration: DeltaTableRegistration) -> str:
    return (
        f"CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{registration.table_name}\n"
        f"LOCATION '{registration.s3_uri}'\n"
        "TBLPROPERTIES ('table_type'='DELTA');"
    )


def build_athena_create_delta_table_sql_for_recreate(
    database_name: str, registration: DeltaTableRegistration
) -> str:
    return build_athena_create_delta_table_sql(database_name, registration).replace(" IF NOT EXISTS", "")


def ensure_glue_database(config: AppConfig) -> None:
    glue = boto3.client("glue", region_name=config.s3.region_name)
    try:
        glue.get_database(Name=config.metadata.glue_database_name)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": config.metadata.glue_database_name})


def delete_glue_table_if_exists(config: AppConfig, table_name: str) -> bool:
    glue = boto3.client("glue", region_name=config.s3.region_name)
    try:
        glue.delete_table(DatabaseName=config.metadata.glue_database_name, Name=table_name)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False


def start_athena_query(config: AppConfig, query: str) -> str:
    athena = boto3.client("athena", region_name=config.s3.region_name)
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": config.metadata.glue_database_name},
        ResultConfiguration={"OutputLocation": config.metadata.athena_results_s3_uri},
    )
    return str(response["QueryExecutionId"])


def wait_for_athena_query(
    config: AppConfig,
    query_execution_id: str,
    poll_seconds: float = 2.0,
    timeout_seconds: int = 300,
) -> dict[str, Any]:
    athena = boto3.client("athena", region_name=config.s3.region_name)
    deadline = time.monotonic() + timeout_seconds

    while True:
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution = response["QueryExecution"]
        status = query_execution["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            return query_execution
        if state in {"FAILED", "CANCELLED"}:
            reason = status.get("StateChangeReason", "No failure reason returned by Athena.")
            raise RuntimeError(f"Athena query {query_execution_id} ended in state={state}: {reason}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Athena query {query_execution_id} did not finish within {timeout_seconds}s.")
        time.sleep(poll_seconds)


def refresh_glue_delta_tables(
    config: AppConfig,
    force_recreate: bool = False,
    wait_for_completion: bool = True,
) -> list[str]:
    ensure_glue_database(config)
    query_execution_ids: list[str] = []
    for registration in build_delta_table_registrations(config):
        if force_recreate:
            delete_glue_table_if_exists(config, registration.table_name)
            query = build_athena_create_delta_table_sql_for_recreate(
                config.metadata.glue_database_name, registration
            )
        else:
            query = build_athena_create_delta_table_sql(config.metadata.glue_database_name, registration)

        query_execution_id = start_athena_query(config, query)
        if wait_for_completion:
            wait_for_athena_query(config, query_execution_id)
        query_execution_ids.append(query_execution_id)
    return query_execution_ids


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register Delta table locations in Glue via Athena DDL.")
    parser.add_argument("--config-path", default=None)
    parser.add_argument("--print-sql", action="store_true")
    parser.add_argument("--force-recreate", action="store_true")
    parser.add_argument("--no-wait", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = load_config(args.config_path)
    registrations = build_delta_table_registrations(config)
    if args.print_sql:
        for registration in registrations:
            print(build_athena_create_delta_table_sql(config.metadata.glue_database_name, registration))
        return 0

    for query_execution_id in refresh_glue_delta_tables(
        config,
        force_recreate=args.force_recreate,
        wait_for_completion=not args.no_wait,
    ):
        print(query_execution_id)
    return 0


if __name__ == "__main__":
    main()

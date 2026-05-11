from __future__ import annotations

import argparse
import sys
import time
from typing import Any

import requests
from airflow.models.xcom import XCom
from airflow.utils.session import create_session

from src.common.config import load_config


TERMINAL_LIFE_CYCLE_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wait for a Databricks run returned by an Airflow task.")
    parser.add_argument("--airflow-dag-id", required=True)
    parser.add_argument("--airflow-run-id", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--poll-seconds", type=int, default=30)
    parser.add_argument("--timeout-seconds", type=int, default=14400)
    return parser.parse_args()


def _load_databricks_run_id(dag_id: str, run_id: str, task_id: str) -> int:
    with create_session() as session:
        value: Any = XCom.get_one(
            key="return_value",
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            session=session,
        )
    if not isinstance(value, dict) or not value.get("run_id"):
        raise ValueError(f"No Databricks run_id found in XCom for {dag_id}.{task_id} run_id={run_id}")
    return int(value["run_id"])


def main() -> int:
    args = parse_args()
    databricks_run_id = _load_databricks_run_id(args.airflow_dag_id, args.airflow_run_id, args.task_id)
    config = load_config(include_databricks_secrets=True)
    if not config.databricks.host or not config.databricks.token:
        raise ValueError("Databricks host/token are required to wait for a Databricks run.")

    endpoint = config.databricks.host.rstrip("/") + "/api/2.1/jobs/runs/get"
    headers = {"Authorization": f"Bearer {config.databricks.token}"}
    deadline = time.monotonic() + args.timeout_seconds
    last_state: dict[str, Any] | None = None

    print(f"Waiting for Databricks run_id={databricks_run_id}", flush=True)
    while time.monotonic() < deadline:
        response = requests.get(
            endpoint,
            headers=headers,
            params={"run_id": databricks_run_id},
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json()
        state = payload.get("state", {})

        if state != last_state:
            print(f"Databricks run_id={databricks_run_id} state={state}", flush=True)
            last_state = state

        life_cycle_state = state.get("life_cycle_state")
        result_state = state.get("result_state")
        if life_cycle_state in TERMINAL_LIFE_CYCLE_STATES:
            return 0 if result_state == "SUCCESS" else 1
        time.sleep(args.poll_seconds)

    print(f"Timed out waiting for Databricks run_id={databricks_run_id}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

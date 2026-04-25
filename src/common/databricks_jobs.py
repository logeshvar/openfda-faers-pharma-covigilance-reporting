from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Any

import requests

from src.common.config import AppConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatabricksSubmitResult:
    submitted: bool
    run_id: int | None
    run_page_url: str | None
    message: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_databricks_submit_run_payload(
    config: AppConfig,
    run_name: str,
    tasks: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "run_name": run_name,
        "job_clusters": [
            {
                "job_cluster_key": "curated_job_cluster",
                "new_cluster": {
                    "spark_version": config.databricks.spark_version,
                    "node_type_id": config.databricks.node_type_id,
                    "num_workers": config.databricks.num_workers,
                    "data_security_mode": "SINGLE_USER",
                    "aws_attributes": {
                        "availability": "ON_DEMAND",
                    },
                },
            }
        ],
        "tasks": tasks,
    }


def submit_databricks_run(config: AppConfig, payload: dict[str, Any]) -> DatabricksSubmitResult:
    if not config.databricks.submit_enabled:
        logger.info("Databricks submission disabled. Payload prepared for review: %s", payload)
        return DatabricksSubmitResult(
            submitted=False,
            run_id=None,
            run_page_url=None,
            message="Databricks submission disabled; prepared payload only.",
        )

    if not config.databricks.host or not config.databricks.token:
        raise ValueError(
            "Databricks submission is enabled, but DATABRICKS_HOST or DATABRICKS_TOKEN is missing."
        )

    endpoint = config.databricks.host.rstrip("/") + "/api/2.1/jobs/runs/submit"
    response = requests.post(
        endpoint,
        headers={"Authorization": f"Bearer {config.databricks.token}"},
        json=payload,
        timeout=60,
    )
    response.raise_for_status()
    response_payload = response.json()

    return DatabricksSubmitResult(
        submitted=True,
        run_id=response_payload.get("run_id"),
        run_page_url=response_payload.get("run_page_url"),
        message="Databricks run submitted.",
    )

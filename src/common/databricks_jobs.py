from __future__ import annotations

import logging
from copy import deepcopy
from dataclasses import asdict, dataclass
from typing import Any

import requests

from src.common.config import AppConfig
from src.common.path_builders import build_s3_uri

logger = logging.getLogger(__name__)

DATABRICKS_JOB_CLUSTER_KEY = "pharma_cv_job_cluster"
SUPPORTED_EXECUTION_SOURCES = {"git", "s3"}


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
    validate_databricks_job_config(config)
    payload = {
        "run_name": run_name,
        "tasks": build_run_submit_tasks(config, tasks),
    }

    if config.databricks.execution_source == "git":
        payload["git_source"] = {
            "git_url": config.databricks.git_url,
            "git_provider": config.databricks.git_provider,
            "git_branch": config.databricks.git_branch,
        }

    return payload


def build_run_submit_tasks(config: AppConfig, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Adapt task definitions for the Jobs runs/submit API.

    Databricks accepts shared `job_clusters` on saved Jobs, but not on one-time
    `runs/submit` calls. Our internal task builders still use the shared-cluster
    shape so the dependency graph stays readable, then this function materializes
    each submitted task with its own cluster spec.
    """

    cluster = build_databricks_new_cluster(config)
    submitted_tasks: list[dict[str, Any]] = []
    for task in tasks:
        submitted_task = deepcopy(task)
        submitted_task.pop("job_cluster_key", None)

        has_cluster = any(
            key in submitted_task for key in ("new_cluster", "existing_cluster_id", "job_cluster_key")
        )
        if not has_cluster:
            submitted_task["new_cluster"] = deepcopy(cluster)

        submitted_tasks.append(submitted_task)

    return submitted_tasks


def validate_databricks_job_config(config: AppConfig) -> None:
    execution_source = config.databricks.execution_source
    if execution_source not in SUPPORTED_EXECUTION_SOURCES:
        raise ValueError(
            f"Unsupported Databricks execution_source={execution_source!r}. "
            f"Expected one of {sorted(SUPPORTED_EXECUTION_SOURCES)}."
        )

    if execution_source == "git":
        missing = [
            name
            for name, value in {
                "git_url": config.databricks.git_url,
                "git_provider": config.databricks.git_provider,
                "git_branch": config.databricks.git_branch,
                "python_file_base_path": config.databricks.python_file_base_path,
            }.items()
            if value in (None, "")
        ]
        if missing:
            raise ValueError(f"Databricks Git execution is missing required config: {', '.join(missing)}")

    if execution_source == "s3" and not config.databricks.python_file_base_uri:
        raise ValueError("Databricks S3 execution requires python_file_base_uri.")


def build_databricks_new_cluster(config: AppConfig) -> dict[str, Any]:
    cluster: dict[str, Any] = {
        "spark_version": config.databricks.spark_version,
        "node_type_id": config.databricks.node_type_id,
        "num_workers": config.databricks.num_workers,
        "aws_attributes": {
            "availability": "ON_DEMAND",
        },
    }

    if config.databricks.instance_profile_arn:
        cluster["aws_attributes"]["instance_profile_arn"] = config.databricks.instance_profile_arn
    if config.databricks.data_security_mode:
        cluster["data_security_mode"] = config.databricks.data_security_mode
    if config.databricks.single_user_name:
        cluster["single_user_name"] = config.databricks.single_user_name

    return cluster


def build_python_file_path(config: AppConfig, relative_path: str) -> str:
    clean_relative_path = relative_path.strip("/")
    if config.databricks.execution_source == "git":
        return "/".join([config.databricks.python_file_base_path.strip("/"), clean_relative_path])
    if not config.databricks.python_file_base_uri:
        raise ValueError("python_file_base_uri is required for Databricks S3 execution.")
    return "/".join([config.databricks.python_file_base_uri.rstrip("/"), clean_relative_path])


def _prefix_base(prefix: str) -> str:
    return prefix.strip("/").split("/", 1)[0]


def build_layer_base_paths(config: AppConfig) -> dict[str, str]:
    ops_prefix = _prefix_base(config.s3.audit_prefix)
    return {
        "raw_base_path": build_s3_uri(config.s3.bucket_name, config.s3.raw_prefix),
        "curated_base_path": build_s3_uri(config.s3.bucket_name, config.s3.curated_prefix),
        "gold_base_path": build_s3_uri(config.s3.bucket_name, config.s3.gold_prefix),
        "ops_base_path": build_s3_uri(config.s3.bucket_name, ops_prefix),
    }


def build_common_task_parameters(
    config: AppConfig,
    run_id: str,
    window_start: str,
    window_end: str,
    batch_id: str | None = None,
) -> list[str]:
    base_paths = build_layer_base_paths(config)
    parameters = [
        "--env",
        config.project.environment,
        "--run-id",
        run_id,
        "--raw-base-path",
        base_paths["raw_base_path"],
        "--curated-base-path",
        base_paths["curated_base_path"],
        "--gold-base-path",
        base_paths["gold_base_path"],
        "--ops-base-path",
        base_paths["ops_base_path"],
        "--window-start",
        window_start,
        "--window-end",
        window_end,
    ]
    if batch_id:
        parameters.extend(["--batch-id", batch_id])
    return parameters


def build_spark_python_task(
    config: AppConfig,
    task_key: str,
    python_file: str,
    parameters: list[str],
    depends_on: list[str] | None = None,
) -> dict[str, Any]:
    task: dict[str, Any] = {
        "task_key": task_key,
        "job_cluster_key": DATABRICKS_JOB_CLUSTER_KEY,
        "spark_python_task": {
            "python_file": build_python_file_path(config, python_file),
            "parameters": parameters,
        },
    }
    if config.databricks.execution_source == "git":
        task["spark_python_task"]["source"] = "GIT"
    if depends_on:
        task["depends_on"] = [{"task_key": task_key} for task_key in depends_on]
    return task


def build_databricks_smoke_test_payload(
    config: AppConfig,
    run_name: str,
    bucket_path: str | None = None,
    ops_base_path: str | None = None,
) -> dict[str, Any]:
    base_paths = build_layer_base_paths(config)
    task = build_spark_python_task(
        config=config,
        task_key="smoke_test_s3_access",
        python_file="databricks/smoke_test_s3_access.py",
        parameters=[
            "--bucket-path",
            bucket_path or build_s3_uri(config.s3.bucket_name, ""),
            "--ops-base-path",
            ops_base_path or base_paths["ops_base_path"],
        ],
    )
    return build_databricks_submit_run_payload(config=config, run_name=run_name, tasks=[task])


def submit_databricks_run(
    config: AppConfig,
    payload: dict[str, Any],
    dry_run: bool = False,
) -> DatabricksSubmitResult:
    if dry_run or not config.databricks.submit_enabled:
        logger.info("Databricks submission disabled. Payload prepared for review: %s", payload)
        return DatabricksSubmitResult(
            submitted=False,
            run_id=None,
            run_page_url=None,
            message="Databricks submission disabled or dry-run requested; prepared payload only.",
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
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        logger.error(
            "Databricks run submit failed status=%s response=%s payload=%s",
            response.status_code,
            response.text,
            payload,
        )
        raise RuntimeError(
            f"Databricks run submit failed with HTTP {response.status_code}: {response.text}"
        ) from exc
    response_payload = response.json()

    return DatabricksSubmitResult(
        submitted=True,
        run_id=response_payload.get("run_id"),
        run_page_url=response_payload.get("run_page_url"),
        message="Databricks run submitted.",
    )

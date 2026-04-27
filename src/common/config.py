from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from src.common.secrets import load_json_secret

ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = ROOT_DIR / "conf" / "dev.yaml"
DEFAULT_DOTENV_PATH = ROOT_DIR / ".env"

load_dotenv(DEFAULT_DOTENV_PATH, override=False)


@dataclass(frozen=True)
class ProjectSettings:
    name: str
    environment: str


@dataclass(frozen=True)
class S3Settings:
    bucket_name: str
    region_name: str
    endpoint_url: str | None
    raw_prefix: str
    audit_prefix: str
    curated_prefix: str
    gold_prefix: str
    access_key_id: str | None
    secret_access_key: str | None


@dataclass(frozen=True)
class OpenFDASettings:
    base_url: str
    date_field: str
    page_size: int
    max_pages_per_run: int
    request_timeout_seconds: int
    sleep_seconds_between_requests: float


@dataclass(frozen=True)
class DatabricksSettings:
    submit_enabled: bool
    host: str | None
    token: str | None
    run_name_prefix: str
    python_file_base_uri: str | None
    spark_version: str
    node_type_id: str
    num_workers: int
    secret_id: str | None = None
    execution_source: str = "s3"
    git_url: str | None = None
    git_provider: str | None = None
    git_branch: str | None = None
    python_file_base_path: str = "src"
    autotermination_minutes: int = 30
    instance_profile_arn: str | None = None
    data_security_mode: str | None = None
    single_user_name: str | None = None


@dataclass(frozen=True)
class MetadataSettings:
    glue_database_name: str
    athena_results_s3_uri: str


@dataclass(frozen=True)
class IngestionSettings:
    source_name: str
    schedule: str
    local_staging_dir: Path
    cleanup_staging_files: bool = True
    source_lag_days: int = 120
    default_window_days: int = 1


@dataclass(frozen=True)
class DQSettings:
    min_expected_records: int
    required_raw_fields: tuple[str, ...]


@dataclass(frozen=True)
class LoggingSettings:
    level: str


@dataclass(frozen=True)
class AppConfig:
    project: ProjectSettings
    s3: S3Settings
    openfda: OpenFDASettings
    databricks: DatabricksSettings
    metadata: MetadataSettings
    ingestion: IngestionSettings
    dq: DQSettings
    logging: LoggingSettings
    config_path: Path
    root_dir: Path


def _read_yaml(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file does not exist: {config_path}")

    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _get_nested(mapping: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = mapping
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value not in (None, ""):
            return value
    return None


def _env_or_default(name: str, default: Any) -> Any:
    value = os.getenv(name)
    if value in (None, ""):
        return default
    return value


def _as_int(value: Any) -> int:
    return int(value)


def _as_float(value: Any) -> float:
    return float(value)


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _resolve_path(root_dir: Path, value: str | Path) -> Path:
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (root_dir / candidate).resolve()


def _load_databricks_secret(config_data: dict[str, Any], region_name: str) -> dict[str, Any]:
    secret_id = _first_env("DATABRICKS_SECRET_ID") or _get_nested(
        config_data, "databricks", "secret_id", default=None
    )
    if not secret_id:
        return {}
    return load_json_secret(str(secret_id), region_name=region_name)


def _env_secret_or_default(
    env_name: str,
    secret_payload: dict[str, Any],
    secret_key_env_name: str,
    default_secret_key: str,
    default: Any,
) -> Any:
    env_value = _first_env(env_name)
    if env_value is not None:
        return env_value

    secret_key = _first_env(secret_key_env_name) or default_secret_key
    secret_value = secret_payload.get(secret_key)
    if secret_value not in (None, ""):
        return secret_value

    return default


@lru_cache(maxsize=8)
def load_config(
    config_path: str | Path | None = None,
    include_databricks_secrets: bool = False,
) -> AppConfig:
    config_path_from_env = _first_env("CV_CONFIG_PATH")
    resolved_config_path = Path(config_path_from_env or config_path or DEFAULT_CONFIG_PATH).expanduser()
    config_data = _read_yaml(resolved_config_path)

    project = ProjectSettings(
        name=str(_get_nested(config_data, "project", "name", default="pharma-cv-pipeline")),
        environment=str(_get_nested(config_data, "project", "environment", default="dev")),
    )

    s3 = S3Settings(
        bucket_name=str(
            _env_or_default(
                "S3_BUCKET", _get_nested(config_data, "storage", "bucket_name", default="pharma-cv-dev")
            )
        ),
        region_name=str(
            _first_env("AWS_REGION", "AWS_DEFAULT_REGION")
            or _get_nested(config_data, "storage", "region_name", default="us-east-1")
        ),
        endpoint_url=_first_env("S3_ENDPOINT_URL")
        or _get_nested(config_data, "storage", "endpoint_url", default=None),
        raw_prefix=str(_get_nested(config_data, "storage", "raw_prefix", default="raw/openfda/drug_event")),
        audit_prefix=str(_get_nested(config_data, "storage", "audit_prefix", default="ops/audit")),
        curated_prefix=str(_get_nested(config_data, "storage", "curated_prefix", default="curated")),
        gold_prefix=str(_get_nested(config_data, "storage", "gold_prefix", default="gold")),
        access_key_id=_first_env("AWS_ACCESS_KEY_ID"),
        secret_access_key=_first_env("AWS_SECRET_ACCESS_KEY"),
    )

    openfda = OpenFDASettings(
        base_url=str(
            _env_or_default(
                "OPENFDA_BASE_URL",
                _get_nested(config_data, "openfda", "base_url", default="https://api.fda.gov/drug/event.json"),
            )
        ),
        date_field=str(_get_nested(config_data, "openfda", "date_field", default="receivedate")),
        page_size=_as_int(
            _env_or_default(
                "OPENFDA_PAGE_SIZE", _get_nested(config_data, "openfda", "page_size", default=100)
            )
        ),
        max_pages_per_run=_as_int(
            _env_or_default(
                "OPENFDA_MAX_PAGES_PER_RUN",
                _get_nested(config_data, "openfda", "max_pages_per_run", default=200),
            )
        ),
        request_timeout_seconds=_as_int(
            _env_or_default(
                "OPENFDA_TIMEOUT_SECONDS",
                _get_nested(config_data, "openfda", "request_timeout_seconds", default=60),
            )
        ),
        sleep_seconds_between_requests=_as_float(
            _env_or_default(
                "OPENFDA_SLEEP_SECONDS",
                _get_nested(config_data, "openfda", "sleep_seconds_between_requests", default=0.2),
            )
        ),
    )

    databricks_secret = (
        _load_databricks_secret(config_data, region_name=s3.region_name)
        if include_databricks_secrets
        else {}
    )
    databricks_secret_id = _first_env("DATABRICKS_SECRET_ID") or _get_nested(
        config_data, "databricks", "secret_id", default=None
    )

    databricks = DatabricksSettings(
        submit_enabled=_as_bool(
            _env_or_default(
                "DATABRICKS_SUBMIT_ENABLED",
                _get_nested(config_data, "databricks", "submit_enabled", default=False),
            )
        ),
        secret_id=str(databricks_secret_id) if databricks_secret_id else None,
        host=_env_secret_or_default(
            "DATABRICKS_HOST",
            databricks_secret,
            "DATABRICKS_HOST_SECRET_KEY",
            "host",
            _get_nested(config_data, "databricks", "host", default=None),
        ),
        token=_env_secret_or_default(
            "DATABRICKS_TOKEN",
            databricks_secret,
            "DATABRICKS_TOKEN_SECRET_KEY",
            "token",
            _get_nested(config_data, "databricks", "token", default=None),
        ),
        run_name_prefix=str(
            _env_or_default(
                "DATABRICKS_RUN_NAME_PREFIX",
                _get_nested(config_data, "databricks", "run_name_prefix", default="pharma-cv"),
            )
        ),
        execution_source=str(
            _env_or_default(
                "DATABRICKS_EXECUTION_SOURCE",
                _get_nested(config_data, "databricks", "execution_source", default="s3"),
            )
        ).lower(),
        git_url=_first_env("DATABRICKS_GIT_URL")
        or _get_nested(config_data, "databricks", "git_url", default=None),
        git_provider=_first_env("DATABRICKS_GIT_PROVIDER")
        or _get_nested(config_data, "databricks", "git_provider", default=None),
        git_branch=_first_env("DATABRICKS_GIT_BRANCH")
        or _get_nested(config_data, "databricks", "git_branch", default=None),
        python_file_base_path=str(
            _env_or_default(
                "DATABRICKS_PYTHON_FILE_BASE_PATH",
                _get_nested(config_data, "databricks", "python_file_base_path", default="src"),
            )
        ),
        python_file_base_uri=(
            str(
                _env_or_default(
                    "DATABRICKS_PYTHON_FILE_BASE_URI",
                    _get_nested(
                        config_data,
                        "databricks",
                        "python_file_base_uri",
                        default=f"s3://{s3.bucket_name}/jobs/src",
                    ),
                )
            )
            or None
        ),
        spark_version=str(
            _env_or_default(
                "DATABRICKS_SPARK_VERSION",
                _get_nested(config_data, "databricks", "spark_version", default="14.3.x-scala2.12"),
            )
        ),
        node_type_id=str(
            _env_or_default(
                "DATABRICKS_NODE_TYPE_ID",
                _get_nested(config_data, "databricks", "node_type_id", default="i3.xlarge"),
            )
        ),
        num_workers=_as_int(
            _env_or_default(
                "DATABRICKS_NUM_WORKERS",
                _get_nested(config_data, "databricks", "num_workers", default=1),
            )
        ),
        autotermination_minutes=_as_int(
            _env_or_default(
                "DATABRICKS_AUTOTERMINATION_MINUTES",
                _get_nested(config_data, "databricks", "autotermination_minutes", default=30),
            )
        ),
        instance_profile_arn=_first_env("DATABRICKS_INSTANCE_PROFILE_ARN")
        or _get_nested(config_data, "databricks", "instance_profile_arn", default=None),
        data_security_mode=_first_env("DATABRICKS_DATA_SECURITY_MODE")
        or _get_nested(config_data, "databricks", "data_security_mode", default=None),
        single_user_name=_first_env("DATABRICKS_SINGLE_USER_NAME")
        or _get_nested(config_data, "databricks", "single_user_name", default=None),
    )

    metadata = MetadataSettings(
        glue_database_name=str(
            _env_or_default(
                "GLUE_DATABASE_NAME",
                _get_nested(config_data, "metadata", "glue_database_name", default="pharma_cv_dev"),
            )
        ),
        athena_results_s3_uri=str(
            _env_or_default(
                "ATHENA_RESULTS_S3_URI",
                _get_nested(
                    config_data,
                    "metadata",
                    "athena_results_s3_uri",
                    default=f"s3://{s3.bucket_name}/ops/athena-results/",
                ),
            )
        ),
    )

    ingestion = IngestionSettings(
        source_name=str(_get_nested(config_data, "ingestion", "source_name", default="openfda_drug_event")),
        schedule=str(_get_nested(config_data, "ingestion", "schedule", default="0 6 * * *")),
        local_staging_dir=_resolve_path(
            ROOT_DIR, str(_get_nested(config_data, "ingestion", "local_staging_dir", default=".tmp/openfda"))
        ),
        cleanup_staging_files=_as_bool(
            _env_or_default(
                "OPENFDA_CLEANUP_STAGING_FILES",
                _get_nested(config_data, "ingestion", "cleanup_staging_files", default=True),
            )
        ),
        source_lag_days=_as_int(
            _env_or_default(
                "OPENFDA_SOURCE_LAG_DAYS",
                _get_nested(config_data, "ingestion", "source_lag_days", default=120),
            )
        ),
        default_window_days=_as_int(
            _env_or_default(
                "OPENFDA_DEFAULT_WINDOW_DAYS",
                _get_nested(config_data, "ingestion", "default_window_days", default=1),
            )
        ),
    )

    dq = DQSettings(
        min_expected_records=_as_int(_get_nested(config_data, "dq", "min_expected_records", default=0)),
        required_raw_fields=tuple(
            str(field) for field in _get_nested(config_data, "dq", "required_raw_fields", default=[])
        ),
    )

    logging = LoggingSettings(
        level=str(_get_nested(config_data, "logging", "level", default="INFO")).upper()
    )

    return AppConfig(
        project=project,
        s3=s3,
        openfda=openfda,
        databricks=databricks,
        metadata=metadata,
        ingestion=ingestion,
        dq=dq,
        logging=logging,
        config_path=resolved_config_path,
        root_dir=ROOT_DIR,
    )

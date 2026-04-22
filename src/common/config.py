from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

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
class IngestionSettings:
    source_name: str
    schedule: str
    local_staging_dir: Path


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


def _resolve_path(root_dir: Path, value: str | Path) -> Path:
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (root_dir / candidate).resolve()


@lru_cache(maxsize=4)
def load_config(config_path: str | Path | None = None) -> AppConfig:
    resolved_config_path = Path(
        _env_or_default("PV_CONFIG_PATH", config_path or DEFAULT_CONFIG_PATH)
    ).expanduser()
    config_data = _read_yaml(resolved_config_path)

    project = ProjectSettings(
        name=str(_get_nested(config_data, "project", "name", default="pharma-pv-pipeline")),
        environment=str(_get_nested(config_data, "project", "environment", default="dev")),
    )

    s3 = S3Settings(
        bucket_name=str(
            _env_or_default(
                "S3_BUCKET", _get_nested(config_data, "storage", "bucket_name", default="pharma-pv-dev")
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

    ingestion = IngestionSettings(
        source_name=str(_get_nested(config_data, "ingestion", "source_name", default="openfda_drug_event")),
        schedule=str(_get_nested(config_data, "ingestion", "schedule", default="@monthly")),
        local_staging_dir=_resolve_path(
            ROOT_DIR, str(_get_nested(config_data, "ingestion", "local_staging_dir", default=".tmp/openfda"))
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
        ingestion=ingestion,
        dq=dq,
        logging=logging,
        config_path=resolved_config_path,
        root_dir=ROOT_DIR,
    )

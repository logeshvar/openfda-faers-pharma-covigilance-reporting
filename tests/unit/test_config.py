from __future__ import annotations

from pathlib import Path
from typing import Any

from src.common import config as config_module


def _write_minimal_config(path: Path) -> None:
    path.write_text(
        """
project:
  name: pharma-cv-pipeline
  environment: test
storage:
  bucket_name: pharma-cv-test
  region_name: us-east-1
ingestion:
  source_name: openfda_drug_event
  schedule: "@monthly"
  local_staging_dir: .tmp/openfda
""",
        encoding="utf-8",
    )


def test_load_config_can_resolve_databricks_values_from_secrets_manager(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "dev.yaml"
    _write_minimal_config(config_path)

    def fake_load_json_secret(secret_id: str, region_name: str) -> dict[str, Any]:
        assert secret_id == "pharma-cv/databricks"
        assert region_name == "us-east-1"
        return {
            "host": "https://example.cloud.databricks.com",
            "token": "secret-token",
            "python_file_base_uri": "s3://pharma-cv-test/jobs/src",
        }

    monkeypatch.delenv("CV_CONFIG_PATH", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("DATABRICKS_PYTHON_FILE_BASE_URI", raising=False)
    monkeypatch.setenv("DATABRICKS_SECRET_ID", "pharma-cv/databricks")
    monkeypatch.setattr(config_module, "load_json_secret", fake_load_json_secret)
    config_module.load_config.cache_clear()

    app_config = config_module.load_config(config_path)

    assert app_config.databricks.host == "https://example.cloud.databricks.com"
    assert app_config.databricks.token == "secret-token"
    assert app_config.databricks.python_file_base_uri == "s3://pharma-cv-test/jobs/src"


def test_load_config_allows_staging_cleanup_to_be_disabled(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "dev.yaml"
    _write_minimal_config(config_path)

    monkeypatch.delenv("CV_CONFIG_PATH", raising=False)
    monkeypatch.setenv("OPENFDA_CLEANUP_STAGING_FILES", "false")
    config_module.load_config.cache_clear()

    app_config = config_module.load_config(config_path)

    assert app_config.ingestion.cleanup_staging_files is False


def test_load_config_reads_lagged_window_settings(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "dev.yaml"
    _write_minimal_config(config_path)

    monkeypatch.delenv("CV_CONFIG_PATH", raising=False)
    monkeypatch.setenv("OPENFDA_SOURCE_LAG_DAYS", "100")
    monkeypatch.setenv("OPENFDA_DEFAULT_WINDOW_DAYS", "3")
    config_module.load_config.cache_clear()

    app_config = config_module.load_config(config_path)

    assert app_config.ingestion.source_lag_days == 100
    assert app_config.ingestion.default_window_days == 3

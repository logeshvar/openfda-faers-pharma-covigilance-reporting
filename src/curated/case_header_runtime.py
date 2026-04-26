from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

from src.common.aws_clients import build_s3_client
from src.common.config import AppConfig
from src.common.path_builders import build_s3_uri, build_table_s3_uri
from src.curated.build_case_header import (
    CASE_HEADER_KEY_COLUMNS,
    CASE_HEADER_PARTITION_COLUMNS,
    CURATED_CASE_HEADER_TABLE,
)
from src.curated.build_case_drug import (
    CASE_DRUG_KEY_COLUMNS,
    CASE_DRUG_PARTITION_COLUMNS,
    CURATED_CASE_DRUG_TABLE,
)
from src.curated.build_case_drug_openfda import (
    CASE_DRUG_OPENFDA_KEY_COLUMNS,
    CASE_DRUG_OPENFDA_PARTITION_COLUMNS,
    CURATED_CASE_DRUG_OPENFDA_TABLE,
)
from src.curated.build_case_reaction import (
    CASE_REACTION_KEY_COLUMNS,
    CASE_REACTION_PARTITION_COLUMNS,
    CURATED_CASE_REACTION_TABLE,
)
from src.curated.build_patient_demo import (
    CURATED_PATIENT_DEMO_TABLE,
    PATIENT_DEMO_KEY_COLUMNS,
    PATIENT_DEMO_PARTITION_COLUMNS,
)
from src.curated.build_primary_source import (
    CURATED_PRIMARY_SOURCE_TABLE,
    PRIMARY_SOURCE_KEY_COLUMNS,
    PRIMARY_SOURCE_PARTITION_COLUMNS,
)
from src.gold.build_gold_case_seriousness_trends import GOLD_CASE_SERIOUSNESS_TRENDS_TABLE
from src.gold.build_gold_drug_reaction_trends import GOLD_DRUG_REACTION_TRENDS_TABLE
from src.gold.build_gold_manufacturer_class_serious_trends import (
    GOLD_MANUFACTURER_CLASS_SERIOUS_TRENDS_TABLE,
)
from src.gold.build_gold_reaction_demographic_trends import GOLD_REACTION_DEMOGRAPHIC_TRENDS_TABLE
from src.gold.build_latest_case_helper import GOLD_LATEST_CASE_HELPER_TABLE
from src.ingestion.extract_openfda import parse_window_date

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CuratedTableSpec:
    table_name: str
    python_file: str
    key_columns: tuple[str, ...]
    partition_columns: tuple[str, ...]


@dataclass(frozen=True)
class RawBatchObject:
    bucket_name: str
    s3_key: str
    s3_uri: str
    source_file_name: str
    ingest_batch_id: str
    query_window_start: str
    query_window_end: str
    size_bytes: int | None
    last_modified: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CuratedJobManifest:
    table_name: str
    python_file: str
    query_window_start: str
    query_window_end: str
    raw_input_s3_uri: str
    raw_input_s3_key: str
    selected_ingest_batch_id: str
    curated_output_s3_uri: str
    key_columns: tuple[str, ...]
    partition_columns: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_job_args(self) -> dict[str, str]:
        return {
            "raw_input_path": self.raw_input_s3_uri,
            "output_path": self.curated_output_s3_uri,
        }


CaseHeaderJobManifest = CuratedJobManifest


CURATED_TABLE_SPECS: dict[str, CuratedTableSpec] = {
    CURATED_CASE_HEADER_TABLE: CuratedTableSpec(
        table_name=CURATED_CASE_HEADER_TABLE,
        python_file="curated/build_case_header.py",
        key_columns=CASE_HEADER_KEY_COLUMNS,
        partition_columns=CASE_HEADER_PARTITION_COLUMNS,
    ),
    CURATED_PRIMARY_SOURCE_TABLE: CuratedTableSpec(
        table_name=CURATED_PRIMARY_SOURCE_TABLE,
        python_file="curated/build_primary_source.py",
        key_columns=PRIMARY_SOURCE_KEY_COLUMNS,
        partition_columns=PRIMARY_SOURCE_PARTITION_COLUMNS,
    ),
    CURATED_PATIENT_DEMO_TABLE: CuratedTableSpec(
        table_name=CURATED_PATIENT_DEMO_TABLE,
        python_file="curated/build_patient_demo.py",
        key_columns=PATIENT_DEMO_KEY_COLUMNS,
        partition_columns=PATIENT_DEMO_PARTITION_COLUMNS,
    ),
    CURATED_CASE_DRUG_TABLE: CuratedTableSpec(
        table_name=CURATED_CASE_DRUG_TABLE,
        python_file="curated/build_case_drug.py",
        key_columns=CASE_DRUG_KEY_COLUMNS,
        partition_columns=CASE_DRUG_PARTITION_COLUMNS,
    ),
    CURATED_CASE_DRUG_OPENFDA_TABLE: CuratedTableSpec(
        table_name=CURATED_CASE_DRUG_OPENFDA_TABLE,
        python_file="curated/build_case_drug_openfda.py",
        key_columns=CASE_DRUG_OPENFDA_KEY_COLUMNS,
        partition_columns=CASE_DRUG_OPENFDA_PARTITION_COLUMNS,
    ),
    CURATED_CASE_REACTION_TABLE: CuratedTableSpec(
        table_name=CURATED_CASE_REACTION_TABLE,
        python_file="curated/build_case_reaction.py",
        key_columns=CASE_REACTION_KEY_COLUMNS,
        partition_columns=CASE_REACTION_PARTITION_COLUMNS,
    ),
}

CURATED_TABLE_BUILD_ORDER = (
    CURATED_CASE_HEADER_TABLE,
    CURATED_PRIMARY_SOURCE_TABLE,
    CURATED_PATIENT_DEMO_TABLE,
    CURATED_CASE_DRUG_TABLE,
    CURATED_CASE_DRUG_OPENFDA_TABLE,
    CURATED_CASE_REACTION_TABLE,
)


def build_raw_window_prefix(raw_prefix: str, window_start: date, window_end: date) -> str:
    return "/".join(
        [
            raw_prefix.strip("/"),
            f"query_year={window_start:%Y}",
            f"query_month={window_start:%m}",
            f"window_start={window_start:%Y-%m-%d}",
            f"window_end={window_end:%Y-%m-%d}",
        ]
    )


def _extract_ingest_batch_id_from_key(s3_key: str) -> str:
    for segment in s3_key.split("/"):
        if segment.startswith("ingest_batch_id="):
            return segment.split("=", 1)[1]
    raise ValueError(f"Could not determine ingest_batch_id from s3 key: {s3_key}")


def list_raw_batch_objects_for_window(
    config: AppConfig,
    window_start: str | date,
    window_end: str | date,
) -> list[RawBatchObject]:
    parsed_window_start = parse_window_date(window_start)
    parsed_window_end = parse_window_date(window_end)
    prefix = build_raw_window_prefix(
        raw_prefix=config.s3.raw_prefix,
        window_start=parsed_window_start,
        window_end=parsed_window_end,
    )
    client = build_s3_client(config.s3)

    paginator = client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=config.s3.bucket_name, Prefix=prefix)

    batch_objects: list[RawBatchObject] = []
    for page in page_iterator:
        for item in page.get("Contents", []):
            s3_key = str(item["Key"])
            if not s3_key.endswith(".ndjson"):
                continue

            batch_objects.append(
                RawBatchObject(
                    bucket_name=config.s3.bucket_name,
                    s3_key=s3_key,
                    s3_uri=build_s3_uri(config.s3.bucket_name, s3_key),
                    source_file_name=s3_key.rsplit("/", 1)[-1],
                    ingest_batch_id=_extract_ingest_batch_id_from_key(s3_key),
                    query_window_start=parsed_window_start.isoformat(),
                    query_window_end=parsed_window_end.isoformat(),
                    size_bytes=int(item.get("Size", 0)),
                    last_modified=item.get("LastModified").isoformat()
                    if item.get("LastModified")
                    else None,
                )
            )

    logger.info(
        "Located %s raw batch object(s) for window_start=%s window_end=%s under prefix=%s",
        len(batch_objects),
        parsed_window_start,
        parsed_window_end,
        prefix,
    )
    return batch_objects


def select_raw_batch_object(
    batch_objects: list[RawBatchObject],
    ingest_batch_id: str | None = None,
) -> RawBatchObject:
    if not batch_objects:
        raise FileNotFoundError("No raw batch objects were found for the requested window.")

    if ingest_batch_id:
        for batch_object in batch_objects:
            if batch_object.ingest_batch_id == ingest_batch_id:
                return batch_object
        raise FileNotFoundError(
            f"No raw batch object matched ingest_batch_id={ingest_batch_id!r} for the requested window."
        )

    return max(batch_objects, key=lambda batch_object: batch_object.ingest_batch_id)


def build_curated_job_manifest_for_table(
    config: AppConfig,
    raw_batch_object: RawBatchObject,
    table_spec: CuratedTableSpec,
) -> CuratedJobManifest:
    return CuratedJobManifest(
        table_name=table_spec.table_name,
        python_file=table_spec.python_file,
        query_window_start=raw_batch_object.query_window_start,
        query_window_end=raw_batch_object.query_window_end,
        raw_input_s3_uri=raw_batch_object.s3_uri,
        raw_input_s3_key=raw_batch_object.s3_key,
        selected_ingest_batch_id=raw_batch_object.ingest_batch_id,
        curated_output_s3_uri=build_table_s3_uri(
            bucket_name=config.s3.bucket_name,
            layer_prefix=config.s3.curated_prefix,
            table_name=table_spec.table_name,
        ),
        key_columns=table_spec.key_columns,
        partition_columns=table_spec.partition_columns,
    )


def build_case_header_job_manifest(
    config: AppConfig,
    raw_batch_object: RawBatchObject,
) -> CuratedJobManifest:
    return build_curated_job_manifest_for_table(
        config=config,
        raw_batch_object=raw_batch_object,
        table_spec=CURATED_TABLE_SPECS[CURATED_CASE_HEADER_TABLE],
    )


def resolve_case_header_job_manifest(
    config: AppConfig,
    window_start: str | date,
    window_end: str | date,
    ingest_batch_id: str | None = None,
) -> CaseHeaderJobManifest:
    batch_objects = list_raw_batch_objects_for_window(
        config=config,
        window_start=window_start,
        window_end=window_end,
    )
    selected_batch = select_raw_batch_object(batch_objects, ingest_batch_id=ingest_batch_id)
    manifest = build_case_header_job_manifest(config=config, raw_batch_object=selected_batch)

    logger.info(
        "Prepared case header job manifest ingest_batch_id=%s raw_input=%s output=%s",
        manifest.selected_ingest_batch_id,
        manifest.raw_input_s3_uri,
        manifest.curated_output_s3_uri,
    )
    return manifest


def resolve_curated_job_manifests(
    config: AppConfig,
    window_start: str | date,
    window_end: str | date,
    ingest_batch_id: str | None = None,
    table_names: tuple[str, ...] | None = None,
) -> list[CuratedJobManifest]:
    batch_objects = list_raw_batch_objects_for_window(
        config=config,
        window_start=window_start,
        window_end=window_end,
    )
    selected_batch = select_raw_batch_object(batch_objects, ingest_batch_id=ingest_batch_id)
    selected_table_names = table_names or CURATED_TABLE_BUILD_ORDER

    manifests = [
        build_curated_job_manifest_for_table(
            config=config,
            raw_batch_object=selected_batch,
            table_spec=CURATED_TABLE_SPECS[table_name],
        )
        for table_name in selected_table_names
    ]

    logger.info(
        "Prepared %s curated job manifest(s) ingest_batch_id=%s raw_input=%s",
        len(manifests),
        selected_batch.ingest_batch_id,
        selected_batch.s3_uri,
    )
    return manifests


def build_databricks_tasks_for_curated_manifests(
    config: AppConfig,
    manifests: list[CuratedJobManifest | dict[str, Any]],
) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []

    for manifest in manifests:
        table_name = manifest["table_name"] if isinstance(manifest, dict) else manifest.table_name
        python_file_value = manifest["python_file"] if isinstance(manifest, dict) else manifest.python_file
        raw_input_s3_uri = (
            manifest["raw_input_s3_uri"] if isinstance(manifest, dict) else manifest.raw_input_s3_uri
        )
        curated_output_s3_uri = (
            manifest["curated_output_s3_uri"]
            if isinstance(manifest, dict)
            else manifest.curated_output_s3_uri
        )
        python_file = "/".join(
            [
                config.databricks.python_file_base_uri.rstrip("/"),
                python_file_value.strip("/"),
            ]
        )
        tasks.append(
            {
                "task_key": f"build_{table_name}",
                "job_cluster_key": "curated_job_cluster",
                "spark_python_task": {
                    "python_file": python_file,
                    "parameters": [
                        "--raw-input-path",
                        raw_input_s3_uri,
                        "--output-path",
                        curated_output_s3_uri,
                    ],
                },
            }
        )

    return tasks


def _table_uri(config: AppConfig, layer_prefix: str, table_name: str) -> str:
    return build_table_s3_uri(
        bucket_name=config.s3.bucket_name,
        layer_prefix=layer_prefix,
        table_name=table_name,
    )


def _python_file(config: AppConfig, relative_path: str) -> str:
    return "/".join([config.databricks.python_file_base_uri.rstrip("/"), relative_path.strip("/")])


def build_databricks_tasks_for_gold(config: AppConfig) -> list[dict[str, Any]]:
    latest_case_path = _table_uri(config, config.s3.gold_prefix, GOLD_LATEST_CASE_HELPER_TABLE)
    case_header_path = _table_uri(config, config.s3.curated_prefix, CURATED_CASE_HEADER_TABLE)
    primary_source_path = _table_uri(config, config.s3.curated_prefix, CURATED_PRIMARY_SOURCE_TABLE)
    patient_demo_path = _table_uri(config, config.s3.curated_prefix, CURATED_PATIENT_DEMO_TABLE)
    case_drug_path = _table_uri(config, config.s3.curated_prefix, CURATED_CASE_DRUG_TABLE)
    case_drug_openfda_path = _table_uri(config, config.s3.curated_prefix, CURATED_CASE_DRUG_OPENFDA_TABLE)
    case_reaction_path = _table_uri(config, config.s3.curated_prefix, CURATED_CASE_REACTION_TABLE)

    return [
        {
            "task_key": f"build_{GOLD_LATEST_CASE_HELPER_TABLE}",
            "job_cluster_key": "curated_job_cluster",
            "depends_on": [{"task_key": f"build_{CURATED_CASE_HEADER_TABLE}"}],
            "spark_python_task": {
                "python_file": _python_file(config, "gold/build_latest_case_helper.py"),
                "parameters": [
                    "--case-header-path",
                    case_header_path,
                    "--output-path",
                    latest_case_path,
                ],
            },
        },
        {
            "task_key": f"build_{GOLD_CASE_SERIOUSNESS_TRENDS_TABLE}",
            "job_cluster_key": "curated_job_cluster",
            "depends_on": [{"task_key": f"build_{GOLD_LATEST_CASE_HELPER_TABLE}"}],
            "spark_python_task": {
                "python_file": _python_file(config, "gold/build_gold_case_seriousness_trends.py"),
                "parameters": ["--latest-case-path", latest_case_path, "--output-path", _table_uri(config, config.s3.gold_prefix, GOLD_CASE_SERIOUSNESS_TRENDS_TABLE)],
            },
        },
        {
            "task_key": f"build_{GOLD_DRUG_REACTION_TRENDS_TABLE}",
            "job_cluster_key": "curated_job_cluster",
            "depends_on": [
                {"task_key": f"build_{GOLD_LATEST_CASE_HELPER_TABLE}"},
                {"task_key": f"build_{CURATED_CASE_DRUG_TABLE}"},
                {"task_key": f"build_{CURATED_CASE_REACTION_TABLE}"},
            ],
            "spark_python_task": {
                "python_file": _python_file(config, "gold/build_gold_drug_reaction_trends.py"),
                "parameters": [
                    "--latest-case-path",
                    latest_case_path,
                    "--case-drug-path",
                    case_drug_path,
                    "--case-reaction-path",
                    case_reaction_path,
                    "--output-path",
                    _table_uri(config, config.s3.gold_prefix, GOLD_DRUG_REACTION_TRENDS_TABLE),
                ],
            },
        },
        {
            "task_key": f"build_{GOLD_REACTION_DEMOGRAPHIC_TRENDS_TABLE}",
            "job_cluster_key": "curated_job_cluster",
            "depends_on": [
                {"task_key": f"build_{GOLD_LATEST_CASE_HELPER_TABLE}"},
                {"task_key": f"build_{CURATED_CASE_REACTION_TABLE}"},
                {"task_key": f"build_{CURATED_PATIENT_DEMO_TABLE}"},
                {"task_key": f"build_{CURATED_PRIMARY_SOURCE_TABLE}"},
            ],
            "spark_python_task": {
                "python_file": _python_file(config, "gold/build_gold_reaction_demographic_trends.py"),
                "parameters": [
                    "--latest-case-path",
                    latest_case_path,
                    "--reaction-path",
                    case_reaction_path,
                    "--patient-demo-path",
                    patient_demo_path,
                    "--primary-source-path",
                    primary_source_path,
                    "--output-path",
                    _table_uri(config, config.s3.gold_prefix, GOLD_REACTION_DEMOGRAPHIC_TRENDS_TABLE),
                ],
            },
        },
        {
            "task_key": f"build_{GOLD_MANUFACTURER_CLASS_SERIOUS_TRENDS_TABLE}",
            "job_cluster_key": "curated_job_cluster",
            "depends_on": [
                {"task_key": f"build_{GOLD_LATEST_CASE_HELPER_TABLE}"},
                {"task_key": f"build_{CURATED_CASE_DRUG_OPENFDA_TABLE}"},
            ],
            "spark_python_task": {
                "python_file": _python_file(config, "gold/build_gold_manufacturer_class_serious_trends.py"),
                "parameters": [
                    "--latest-case-path",
                    latest_case_path,
                    "--case-drug-openfda-path",
                    case_drug_openfda_path,
                    "--output-path",
                    _table_uri(config, config.s3.gold_prefix, GOLD_MANUFACTURER_CLASS_SERIOUS_TRENDS_TABLE),
                ],
            },
        },
    ]

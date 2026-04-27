from __future__ import annotations

import argparse
import logging
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from pathlib import Path
import sys

_repo_anchors = []
if "__file__" in globals():
    _repo_anchors.append(Path(__file__).resolve())
_repo_anchors.append(Path.cwd().resolve())

for _anchor in _repo_anchors:
    _start = _anchor if _anchor.is_dir() else _anchor.parent
    for _parent in (_start, *_start.parents):
        if (_parent / "src").is_dir() and (_parent / "conf").is_dir():
            if str(_parent) not in sys.path:
                sys.path.insert(0, str(_parent))
            break
    else:
        continue
    break

from src.common.databricks_runtime import add_common_databricks_args, log_common_databricks_args
from src.common.delta_write import overwrite_report_month_partitions

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

GOLD_LATEST_CASE_HELPER_TABLE = "gold_latest_case_helper"


@dataclass(frozen=True)
class GoldJobResult:
    output_path: str
    records_written: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_latest_case_df(case_header_df: "DataFrame") -> "DataFrame":
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    latest_window = Window.partitionBy("safetyreportid").orderBy(
        F.col("safetyreportversion").cast("int").desc_nulls_last(),
        F.col("load_timestamp").desc_nulls_last(),
        F.col("ingest_batch_id").desc_nulls_last(),
    )

    return (
        case_header_df.withColumn("_row_num", F.row_number().over(latest_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def run_latest_case_helper_job(
    spark: "SparkSession",
    case_header_path: str,
    output_path: str,
    window_start: str,
    window_end: str,
) -> GoldJobResult:
    case_header_df = spark.read.format("delta").load(case_header_path)
    latest_df = build_latest_case_df(case_header_df)
    records_written = latest_df.count()
    overwrite_report_month_partitions(
        latest_df,
        output_path=output_path,
        window_start=window_start,
        window_end=window_end,
    )
    return GoldJobResult(output_path=output_path, records_written=records_written)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build latest case helper from curated_case_header.")
    add_common_databricks_args(parser)
    parser.add_argument("--case-header-path", required=True)
    parser.add_argument("--output-path", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    log_common_databricks_args(args)
    spark = SparkSession.builder.appName("build_latest_case_helper").getOrCreate()
    logger.info(
        "Job complete: %s",
        run_latest_case_helper_job(
            spark,
            args.case_header_path,
            args.output_path,
            args.window_start,
            args.window_end,
        ).to_dict(),
    )
    return 0


if __name__ == "__main__":
    main()

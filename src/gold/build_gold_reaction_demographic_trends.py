from __future__ import annotations

import argparse
import logging
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

GOLD_REACTION_DEMOGRAPHIC_TRENDS_TABLE = "gold_reaction_demographic_trends"


@dataclass(frozen=True)
class GoldJobResult:
    output_path: str
    records_written: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_gold_reaction_demographic_trends_job(
    spark: "SparkSession",
    latest_case_path: str,
    reaction_path: str,
    patient_demo_path: str,
    primary_source_path: str,
    output_path: str,
) -> GoldJobResult:
    from pyspark.sql import functions as F

    latest = spark.read.format("delta").load(latest_case_path)
    reactions = spark.read.format("delta").load(reaction_path)
    demo = spark.read.format("delta").load(patient_demo_path)
    source = spark.read.format("delta").load(primary_source_path)

    joined = (
        latest.join(reactions, ["safetyreportid", "safetyreportversion"], "inner")
        .join(demo, ["safetyreportid", "safetyreportversion"], "left")
        .join(source, ["safetyreportid", "safetyreportversion"], "left")
    )

    gold_df = joined.groupBy(
        "report_year",
        "report_month",
        "report_quarter",
        "reaction_meddra_pt",
        F.coalesce("patientsex_label", F.lit("Unknown")).alias("patientsex_label"),
        F.coalesce("derived_age_band", F.lit("Unknown")).alias("derived_age_band"),
        F.coalesce("reporter_country", F.lit("Unknown")).alias("reporter_country"),
    ).agg(
        F.countDistinct("safetyreportid").alias("case_count"),
        F.sum("serious_case_ind").alias("serious_case_count"),
    )
    records_written = gold_df.count()
    gold_df.write.format("delta").mode("overwrite").partitionBy("report_year", "report_month").save(output_path)
    return GoldJobResult(output_path=output_path, records_written=records_written)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build gold_reaction_demographic_trends.")
    parser.add_argument("--latest-case-path", required=True)
    parser.add_argument("--reaction-path", required=True)
    parser.add_argument("--patient-demo-path", required=True)
    parser.add_argument("--primary-source-path", required=True)
    parser.add_argument("--output-path", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    spark = SparkSession.builder.appName("build_gold_reaction_demographic_trends").getOrCreate()
    try:
        logger.info(
            "Job complete: %s",
            run_gold_reaction_demographic_trends_job(
                spark,
                args.latest_case_path,
                args.reaction_path,
                args.patient_demo_path,
                args.primary_source_path,
                args.output_path,
            ).to_dict(),
        )
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

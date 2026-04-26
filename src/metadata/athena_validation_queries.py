from __future__ import annotations


VALIDATION_QUERIES = {
    "curated_case_header_row_count": "SELECT COUNT(*) AS row_count FROM curated_case_header;",
    "curated_latest_versions": (
        "SELECT safetyreportid, COUNT(*) AS version_count "
        "FROM curated_case_header GROUP BY safetyreportid ORDER BY version_count DESC LIMIT 20;"
    ),
    "gold_case_seriousness_recent_months": (
        "SELECT report_year, report_month, case_count, serious_case_count "
        "FROM gold_case_seriousness_trends ORDER BY report_year DESC, report_month DESC LIMIT 12;"
    ),
    "gold_top_drug_reactions": (
        "SELECT drug_name, reaction_meddra_pt, SUM(case_count) AS case_count "
        "FROM gold_drug_reaction_trends "
        "GROUP BY drug_name, reaction_meddra_pt ORDER BY case_count DESC LIMIT 25;"
    ),
}


def render_validation_queries(database_name: str) -> dict[str, str]:
    return {
        name: query.replace(" FROM ", f" FROM {database_name}.")
        for name, query in VALIDATION_QUERIES.items()
    }

CREATE OR REPLACE VIEW pharma_cv_dev.vw_gold_case_seriousness_trends AS
SELECT
    report_year,
    report_month,
    report_quarter,
    case_count,
    serious_case_count,
    non_serious_case_count,
    death_case_count,
    hospitalization_case_count,
    lifethreatening_case_count,
    disabling_case_count,
    congenital_anomaly_case_count,
    other_seriousness_case_count
FROM pharma_cv_dev.gold_case_seriousness_trends;

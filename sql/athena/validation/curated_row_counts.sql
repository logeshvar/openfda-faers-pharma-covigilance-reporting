SELECT 'curated_case_header' AS table_name, COUNT(*) AS row_count FROM pharma_cv_dev.curated_case_header
UNION ALL
SELECT 'curated_primary_source' AS table_name, COUNT(*) AS row_count FROM pharma_cv_dev.curated_primary_source
UNION ALL
SELECT 'curated_patient_demo' AS table_name, COUNT(*) AS row_count FROM pharma_cv_dev.curated_patient_demo
UNION ALL
SELECT 'curated_case_drug' AS table_name, COUNT(*) AS row_count FROM pharma_cv_dev.curated_case_drug
UNION ALL
SELECT 'curated_case_drug_openfda' AS table_name, COUNT(*) AS row_count FROM pharma_cv_dev.curated_case_drug_openfda
UNION ALL
SELECT 'curated_case_reaction' AS table_name, COUNT(*) AS row_count FROM pharma_cv_dev.curated_case_reaction;

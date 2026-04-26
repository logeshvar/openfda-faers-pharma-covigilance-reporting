SELECT 'gold_latest_case_helper' AS table_name, COUNT(*) AS row_count FROM pharma_cv_dev.gold_latest_case_helper
UNION ALL
SELECT 'gold_case_seriousness_trends' AS table_name, COUNT(*) AS row_count FROM pharma_cv_dev.gold_case_seriousness_trends
UNION ALL
SELECT 'gold_drug_reaction_trends' AS table_name, COUNT(*) AS row_count FROM pharma_cv_dev.gold_drug_reaction_trends
UNION ALL
SELECT 'gold_reaction_demographic_trends' AS table_name, COUNT(*) AS row_count FROM pharma_cv_dev.gold_reaction_demographic_trends
UNION ALL
SELECT 'gold_manufacturer_class_serious_trends' AS table_name, COUNT(*) AS row_count FROM pharma_cv_dev.gold_manufacturer_class_serious_trends;

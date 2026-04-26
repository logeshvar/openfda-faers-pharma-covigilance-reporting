SELECT
    drug_name,
    reaction_meddra_pt,
    SUM(case_count) AS case_count,
    SUM(serious_case_count) AS serious_case_count
FROM pharma_cv_dev.gold_drug_reaction_trends
GROUP BY drug_name, reaction_meddra_pt
ORDER BY case_count DESC
LIMIT 25;

CREATE OR REPLACE VIEW pharma_cv_dev.vw_gold_top_drug_reactions AS
SELECT
    drug_name,
    reaction_meddra_pt,
    SUM(case_count) AS case_count,
    SUM(serious_case_count) AS serious_case_count
FROM pharma_cv_dev.gold_drug_reaction_trends
GROUP BY drug_name, reaction_meddra_pt;

CREATE OR REPLACE TABLE `{gcp_project_id}.{gcp_dataset_name}.user_behaviour_metrics` AS
SELECT
    up.customer_id,
    SUM(up.quantity * up.unit_price) AS amount_spent,
    SUM(CASE WHEN mr.positive_review THEN 1 ELSE 0 END) AS num_positive_reviews,
    COUNT(mr.cid) AS num_reviews
FROM `{gcp_project_id}.{gcp_dataset_name}.user_purchase` up
JOIN `{gcp_project_id}.{gcp_dataset_name}.movie_review` mr ON up.customer_id = mr.cid
GROUP BY up.customer_id

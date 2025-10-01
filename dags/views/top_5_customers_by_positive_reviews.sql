SELECT
    customer_id,
    num_positive_reviews
FROM
    `{{ gcp_project }}.{{ gcp_dataset }}.user_behaviour_metrics_view`
ORDER BY num_positive_reviews DESC
LIMIT 5;
SELECT
  customer_id,
  num_positive_reviews
FROM
  `{{ var.value.gcp_project }}.{{ var.value.gcp_dataset }}.user_behaviour_metrics_view`
ORDER BY
  num_positive_reviews DESC
LIMIT 5;

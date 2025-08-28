SELECT
  CORR(amount_spent, num_reviews) AS correlation
FROM
  `{{ var.value.gcp_project }}.{{ var.value.gcp_dataset }}.user_behaviour_metrics_view`;

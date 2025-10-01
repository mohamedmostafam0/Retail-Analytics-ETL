SELECT CORR (amount_spent, num_reviews) AS correlation
FROM
    `{{gcp_project }}.{{ gcp_dataset }}.user_behaviour_metrics_view`;
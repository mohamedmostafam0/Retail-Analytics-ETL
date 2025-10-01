SELECT customer_id, amount_spent
FROM
    `{{ gcp_project }}.{{ gcp_dataset }}.user_behaviour_metrics_view`
ORDER BY amount_spent DESC
LIMIT 5;
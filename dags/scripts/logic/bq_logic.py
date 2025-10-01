import os
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def query_user_behaviour_metrics(gcp_project_id, gcp_dataset_name):
    hook = BigQueryHook(gcp_conn_id=os.getenv('GCP_CONN_ID', 'google_cloud_default'))
    client = hook.get_client()

    query = f'''
    CREATE OR REPLACE TABLE `{gcp_project_id}.{gcp_dataset_name}.user_behaviour_metrics` AS
    SELECT
        up.customer_id,
        SUM(up.quantity * up.unit_price) AS amount_spent,
        SUM(CASE WHEN mr.positive_review THEN 1 ELSE 0 END) AS num_positive_reviews,
        COUNT(mr.cid) AS num_reviews
    FROM `{gcp_project_id}.{gcp_dataset_name}.user_purchase` up
    JOIN `{gcp_project_id}.{gcp_dataset_name}.movie_review` mr ON up.customer_id = mr.cid
    GROUP BY up.customer_id
    '''
    
    client.query(query)
    print("Finished creating user behaviour metrics table")

def generate_looker_studio_link(gcp_project_id, gcp_dataset_name):
    base_url = "https://lookerstudio.google.com/datasources/create"
    gcp_conn_id=os.getenv('GCP_CONN_ID', 'google_cloud_default')
    table_id = "user_behaviour_metrics_view"
    
    url = f"{base_url}?connectorId={gcp_conn_id}&projectId={gcp_project_id}&datasetId={gcp_dataset_name}&tableId={table_id}"
    print(f"\n\n*****\nLooker Studio Link:\n{url}\n*****\n\n")
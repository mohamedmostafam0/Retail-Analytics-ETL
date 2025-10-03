import os
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from variables.minio_vars import USER_ANALYTICS_BUCKET

def query_user_behaviour_metrics(gcp_project_id, gcp_dataset_name):
    hook = BigQueryHook(gcp_conn_id=os.getenv('GCP_CONN_ID', 'google_cloud_default'))
    client = hook.get_client()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file_path = os.path.join(script_dir, '..', 'sql', 'create_user_behaviour_metrics.sql')
    with open(sql_file_path, 'r') as f:
        query = f.read().format(
            gcp_project_id=gcp_project_id,
            gcp_dataset_name=gcp_dataset_name
        )

    client.query(query)
    print("Finished creating user behaviour metrics table")

def generate_looker_studio_link(gcp_project_id, gcp_dataset_name):
    base_url = "https://lookerstudio.google.com/datasources/create"
    gcp_conn_id=os.getenv('GCP_CONN_ID', 'google_cloud_default')
    table_id = "user_behaviour_metrics_view"
    
    url = f"{base_url}?connectorId={gcp_conn_id}&projectId={gcp_project_id}&datasetId={gcp_dataset_name}&tableId={table_id}"
    print(f"\n\n*****\nLooker Studio Link:\n{url}\n*****\n\n")

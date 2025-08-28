from datetime import datetime, timedelta
import os

from docker.types import Mount

from variables.minio_vars import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from variables.gcp_vars import GCP_PROJECT_ID, GCP_DATASET_NAME

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from dags.scripts.logic.bq_logic import (
    query_user_behaviour_metrics,
    generate_looker_studio_link,
)

def _generate_spark_output_paths(**kwargs):
    ti = kwargs['ti']
    ds = kwargs['ds'] # execution_date as YYYY-MM-DD

    spark_clean_base_path = f"s3a://{USER_ANALYTICS_BUCKET}/clean"
    ti.xcom_push(key='spark_clean_base_path', value=spark_clean_base_path)

    user_purchase_spark_output_path = f"{spark_clean_base_path}/user_purchase/{ds}"
    movie_review_spark_output_path = f"{spark_clean_base_path}/movie_review/{ds}"

    ti.xcom_push(key='user_purchase_spark_output_path', value=user_purchase_spark_output_path)
    ti.xcom_push(key='movie_review_spark_output_path', value=movie_review_spark_output_path)

    

# -------------------------
# Parameters
# -------------------------
DATA_PATH = "/opt/airflow/data"

USER_ANALYTICS_BUCKET = "user-analytics"
MOVIE_REVIEW_KEY = "movie_review.csv"
USER_PURCHASE_KEY = "raw/user_purchase.csv"

SPARK_IMAGE = "spark-custom"
PROCESS_DATA_SCRIPT = "/app/process_data.py"


default_args = {
    "owner": "mohamed",
    "depends_on_past": False,
    "email_on_failure": False,
    # "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}


with DAG(
    "user_analytics_dag",
    description="Pull user data and movie review data to analyze behaviour",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    template_searchpath=f"/opt/airflow/dags/views",
    user_defined_macros={
        "gcp_project": GCP_PROJECT_ID,
        "gcp_dataset": GCP_DATASET_NAME,
    },
) as dag:

    with TaskGroup("ingestion_tasks", tooltip="S3 Ingestion Tasks") as ingestion_tasks:
        create_s3_bucket = S3CreateBucketOperator(
            task_id="create_s3_bucket",
            bucket_name=USER_ANALYTICS_BUCKET,
            aws_conn_id="minio_conn",
        )

        movie_review_to_s3 = LocalFilesystemToS3Operator(
            task_id="movie_review_to_s3",
            filename=f"{DATA_PATH}/movie_review.csv",
            dest_key=MOVIE_REVIEW_KEY,
            dest_bucket=USER_ANALYTICS_BUCKET,
            replace=True,
            aws_conn_id="minio_conn",
        )

        user_purchase_to_s3 = SqlToS3Operator(
            task_id="user_purchase_to_s3",
            sql_conn_id="postgres_default",
            query="SELECT * FROM retail.user_purchase;",
            s3_bucket=USER_ANALYTICS_BUCKET,
            s3_key=USER_PURCHASE_KEY,
            replace=True,
            aws_conn_id="minio_conn",
        )
        create_s3_bucket >> [user_purchase_to_s3, movie_review_to_s3]

    with TaskGroup("processing_tasks", tooltip="Data Processing Tasks") as processing_tasks:
        generate_spark_output_paths_task = PythonOperator(
            task_id="generate_spark_output_paths",
            python_callable=_generate_spark_output_paths,
            provide_context=True,
        )

        process_data = DockerOperator(
            task_id="process_data",
            image=SPARK_IMAGE,
            api_version="auto",
            auto_remove=True,
            command=f'''
                bash -c "spark-submit \
                    --conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} \
                    --conf spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY} \
                    --conf spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY} \
                    --conf spark.hadoop.fs.s3a.path.style.access=true \
                    {PROCESS_DATA_SCRIPT} \
                    --movie_review_input s3a://{USER_ANALYTICS_BUCKET}/{MOVIE_REVIEW_KEY} \
                    --user_purchase_input s3a://{USER_ANALYTICS_BUCKET}/{USER_PURCHASE_KEY} \
                    --output {{ti.xcom_pull(task_ids='processing_tasks.generate_spark_output_paths', key='spark_clean_base_path')}} \
                    --run-id {{ds}}"
            ''',
            docker_url="unix://var/run/docker.sock",
            network_mode="batch-etl-duckdb_default",
            mounts=[
                Mount(source="/home/mohamed-client/Documents/data-engineering/Batch-ETL-DuckDB/dags/scripts/spark",
                    target="/app", type="bind"),
            ],
            mount_tmp_dir=False,
        )

        
        generate_spark_output_paths_task >> process_data
        process_data >> [wait_for_user_purchase_parquet, wait_for_movie_review_parquet]

    

    with TaskGroup("bq_view_creation_tasks", tooltip="BigQuery View Creation Tasks") as bq_view_creation_tasks:
        query_user_behaviour_metrics_task = PythonOperator(
            task_id="query_user_behaviour_metrics",
            python_callable=query_user_behaviour_metrics,
            op_kwargs={
                "gcp_project_id": GCP_PROJECT_ID,
                "gcp_dataset_name": GCP_DATASET_NAME,
            },
        )

        create_user_behaviour_metrics_view = BigQueryInsertJobOperator(
            task_id="create_user_behaviour_metrics_view",
            configuration={
                "query": {
                    "query": "{{ macros.jinja.get_template('user_behaviour_metrics_view.sql').render() }}",
                    "useLegacySql": False,
                }
            },
        )

        create_top_5_customers_by_amount_spent_view = BigQueryInsertJobOperator(
            task_id="create_top_5_customers_by_amount_spent_view",
            configuration={
                "query": {
                    "query": "{{ macros.jinja.get_template('top_5_customers_by_amount_spent.sql').render() }}",
                    "useLegacySql": False,
                }
            },
        )

        create_top_5_customers_by_positive_reviews_view = BigQueryInsertJobOperator(
            task_id="create_top_5_customers_by_positive_reviews_view",
            configuration={
                "query": {
                    "query": "{{ macros.jinja.get_template('top_5_customers_by_positive_reviews.sql').render() }}",
                    "useLegacySql": False,
                }
            },
        )

        create_correlation_amount_spent_vs_reviews_view = BigQueryInsertJobOperator(
            task_id="create_correlation_amount_spent_vs_reviews_view",
            configuration={
                "query": {
                    "query": "{{ macros.jinja.get_template('correlation_amount_spent_vs_reviews.sql').render() }}",
                    "useLegacySql": False,
                }
            },
        )


        query_user_behaviour_metrics_task >> create_user_behaviour_metrics_view
        create_user_behaviour_metrics_view >> [
            create_top_5_customers_by_amount_spent_view,
            create_top_5_customers_by_positive_reviews_view,
            create_correlation_amount_spent_vs_reviews_view,
    ]

    ingestion_tasks >> processing_tasks
    processing_tasks >> bq_view_creation_tasks
    bq_view_creation_tasks >> generate_looker_studio_link_task
from datetime import datetime, timedelta
import os

from docker.types import Mount

from variables.minio_vars import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
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

# -------------------------
# Parameters
# -------------------------
DATA_PATH = "/opt/airflow/data"

USER_ANALYTICS_BUCKET = "user-analytics"
MOVIE_REVIEW_KEY = "movie_review.csv"
USER_PURCHASE_KEY = "raw/user_purchase.csv"

SPARK_IMAGE = "spark-custom"
PROCESS_DATA_SCRIPT = "/app/process_data.py"

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_DATASET_NAME = os.getenv("GCP_DATASET_NAME")

default_args = {
    "owner": "mohamed",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
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
                --output s3a://{USER_ANALYTICS_BUCKET}/clean \
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

    wait_for_user_purchase_parquet = S3KeySensor(
        task_id="wait_for_user_purchase_parquet",
        bucket_name=USER_ANALYTICS_BUCKET,
        bucket_key=f"clean/user_purchase/{{ds}}/*.parquet",
        wildcard_match=True,
        poke_interval=10,
        timeout=300,
        mode="reschedule",
        aws_conn_id="minio_conn",
    )

    wait_for_movie_review_parquet = S3KeySensor(
        task_id="wait_for_movie_review_parquet",
        bucket_name=USER_ANALYTICS_BUCKET,
        bucket_key=f"clean/movie_review/{{ds}}/*.parquet",
        wildcard_match=True,
        poke_interval=10,
        timeout=300,
        mode="reschedule",
        aws_conn_id="minio_conn",
    )

    load_user_purchase_to_bq = BigQueryInsertJobOperator(
        task_id="load_user_purchase_to_bq",
        configuration={
            "load": {
                "sourceUris": [f"gs://{USER_ANALYTICS_BUCKET}/clean/user_purchase/{{ds}}/*.parquet"],
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": GCP_DATASET_NAME,
                    "tableId": "user_purchase",
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    load_movie_review_to_bq = BigQueryInsertJobOperator(
        task_id="load_movie_review_to_bq",
        configuration={
            "load": {
                "sourceUris": [f"gs://{USER_ANALYTICS_BUCKET}/clean/movie_review/{{ds}}/*.parquet"],
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": GCP_DATASET_NAME,
                    "tableId": "movie_review",
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

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
                "query": f"CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{GCP_DATASET_NAME}.user_behaviour_metrics_view` AS SELECT * FROM `{GCP_PROJECT_ID}.{GCP_DATASET_NAME}.user_behaviour_metrics",
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

    generate_looker_studio_link_task = PythonOperator(
        task_id="generate_looker_studio_link",
        python_callable=generate_looker_studio_link,
        op_kwargs={
            "gcp_project_id": GCP_PROJECT_ID,
            "gcp_dataset_name": GCP_DATASET_NAME,
        },
    )

    create_s3_bucket >> [user_purchase_to_s3, movie_review_to_s3]
    user_purchase_to_s3 >> process_data
    movie_review_to_s3 >> process_data
    process_data >> wait_for_user_purchase_parquet >> load_user_purchase_to_bq
    process_data >> wait_for_movie_review_parquet >> load_movie_review_to_bq
    [load_user_purchase_to_bq, load_movie_review_to_bq] >> query_user_behaviour_metrics_task
    query_user_behaviour_metrics_task >> create_user_behaviour_metrics_view
    create_user_behaviour_metrics_view >> [
        create_top_5_customers_by_amount_spent_view,
        create_top_5_customers_by_positive_reviews_view,
        create_correlation_amount_spent_vs_reviews_view,
    ]
    [
        create_top_5_customers_by_amount_spent_view,
        create_top_5_customers_by_positive_reviews_view,
        create_correlation_amount_spent_vs_reviews_view,
    ] >> generate_looker_studio_link_task
from datetime import datetime, timedelta
import os
import shutil
import boto3
import duckdb

from docker.types import Mount

from variables.minio_vars import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_REGION, AWS_CONN_ID
from variables.postgres_vars import AIRFLOW_CONN_POSTGRES_DEFAULT, POSTGRES_CONN_ID


from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


# -------------------------
# Parameters via Airflow Variables
# -------------------------
DATA_PATH = "/opt/airflow/data"
TEMP_PATH = "/opt/airflow/temp/s3folder"
DASHBOARD_PATH = "/opt/airflow/dags/scripts/dashboard"

USER_ANALYTICS_BUCKET = "user-analytics"
MOVIE_REVIEW_KEY = "movie_review_key"
USER_PURCHASE_KEY = "raw/user_purchase/user_purchase.csv"

# AIRFLOW_CONN_POSTGRES_DEFAULT = Variable.get("AIRFLOW_CONN_POSTGRES_DEFAULT")

MOVIE_CLASSIFIER_IMAGE = "bitnami/spark:latest"
MOVIE_CLASSIFIER_SCRIPT = "/app/random_text_classification.py"

# MINIO_ENDPOINT = Variable.get("minio_endpoint")
# MINIO_ACCESS_KEY = Variable.get("minio_access_key")
# MINIO_SECRET_KEY = Variable.get("minio_secret_key")
# MINIO_REGION = Variable.get("minio_region")


default_args = {
    "owner": "mohamed",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["alerts@yourcompany.com"],
    # "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

# TODO: remove helper functions from dag file
def get_s3_folder(
    s3_bucket, s3_folder, local_folder="/opt/airflow/temp/s3folder/"
):
    s3 = boto3.resource(
        service_name="s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        region_name=os.getenv("MINIO_REGION"),
    )
    bucket = s3.Bucket(s3_bucket)
    local_path = os.path.join(local_folder, s3_folder)
    # Delete the local folder if it exists
    if os.path.exists(local_path):
        shutil.rmtree(local_path)

    for obj in bucket.objects.filter(Prefix=s3_folder):
        target = os.path.join(local_path, os.path.relpath(obj.key, s3_folder))
        os.makedirs(os.path.dirname(target), exist_ok=True)
        bucket.download_file(obj.key, target)
        print(f"Downloaded {obj.key} to {target}")



# -------------------------
# DAG definition
# -------------------------
with DAG(
    "user_analytics_dag",
    description="Pull user data and movie review data to analyze behaviour",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
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

    movie_classifier = DockerOperator(
        task_id="movie_classifier",
        image=MOVIE_CLASSIFIER_IMAGE,
        api_version="auto",
        auto_remove=True,
        command=f"""
            bash -c "pip install numpy &&
            spark-submit \
                --conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} \
                --conf spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY} \
                --conf spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY} \
                --conf spark.hadoop.fs.s3a.path.style.access=true \
                {MOVIE_CLASSIFIER_SCRIPT} \
                --input s3a://{USER_ANALYTICS_BUCKET}/{MOVIE_REVIEW_KEY} \
                --output s3a://{USER_ANALYTICS_BUCKET}/clean/movie_review \
                --run-id {{ds}}"
        """,
        environment={
            "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
            "AWS_REGION": MINIO_REGION,
            "AWS_ENDPOINT": MINIO_ENDPOINT,
        },
        docker_url="unix://var/run/docker.sock",
        network_mode="batch-etl-duckdb_default",
        mounts=[
            # Host path → container path
            Mount(source="/home/mohamed-client/Documents/data-engineering/Batch-ETL-DuckDB/dags/scripts/spark",
                target="/app", type="bind"),
            Mount(source="/home/mohamed-client/Documents/data-engineering/Batch-ETL-DuckDB/data",
                target="/data", type="bind"),
        ],
        mount_tmp_dir=False,
    )

    get_movie_review_to_warehouse = PythonOperator(
        task_id="get_movie_review_to_warehouse",
        python_callable=get_s3_folder,
        op_kwargs={"s3_bucket": USER_ANALYTICS_BUCKET, "s3_folder": "clean/movie_review"},
    )

    get_user_purchase_to_warehouse = PythonOperator(
        task_id="get_user_purchase_to_warehouse",
        python_callable=get_s3_folder,
        op_kwargs={"s3_bucket": USER_ANALYTICS_BUCKET, "s3_folder": os.path.dirname(USER_PURCHASE_KEY)},
    )

    def create_user_behaviour_metric():
        try:
            q = f"""
            with up as (
              select * from '{TEMP_PATH}/raw/user_purchase/user_purchase.csv'
            ),
            mr as (
              select * from '{TEMP_PATH}/clean/movie_review/*.parquet'
            )
            select
              up.customer_id,
              sum(up.quantity * up.unit_price) as amount_spent,
              sum(case when mr.positive_review then 1 else 0 end) as num_positive_reviews,
              count(mr.cid) as num_reviews
            from up
            join mr on up.customer_id = mr.cid
            group by up.customer_id
            """
            duckdb.sql(q).write_csv(f"{DATA_PATH}/behaviour_metrics.csv")
        except Exception as e: 
            raise RuntimeError(f"Failed to create behaviour metrics: {e}")



    wait_for_user_purchase = S3KeySensor(
        task_id="wait_for_user_purchase",
        bucket_name=USER_ANALYTICS_BUCKET,
        bucket_key=USER_PURCHASE_KEY,
        poke_interval=10,
        timeout=300,
        mode="reschedule",
        aws_conn_id="minio_conn",

    )

    wait_for_movie_review = S3KeySensor(
        task_id="wait_for_movie_review",
        bucket_name=USER_ANALYTICS_BUCKET,
        bucket_key="clean/movie_review/*.parquet",
        wildcard_match=True,
        poke_interval=10,
        timeout=300,
        mode="reschedule",
        aws_conn_id="minio_conn",

    )

    get_user_behaviour_metric = PythonOperator(
        task_id="get_user_behaviour_metric",
        python_callable=create_user_behaviour_metric,
    )
    
    gen_dashboard = BashOperator(
        task_id="generate_dashboard",
        bash_command=f"cd {DASHBOARD_PATH} && quarto render {DASHBOARD_PATH}/dashboard.qmd",
    )


    # -------------------------
    # Task dependencies
    # -------------------------
    create_s3_bucket >> [user_purchase_to_s3, movie_review_to_s3]

    user_purchase_to_s3 >> get_user_purchase_to_warehouse
    movie_review_to_s3 >> movie_classifier >> get_movie_review_to_warehouse

    [wait_for_user_purchase, wait_for_movie_review] >> get_user_behaviour_metric >> gen_dashboard
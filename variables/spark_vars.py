import os

SPARK_JARS = "/opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars-extra/hadoop-aws-3.3.4.jar,/opt/spark/jars-extra/spark-bigquery-with-dependencies_2.12-0.42.2.jar"

SPARK_CONF = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    "google.cloud.auth.service.account.enable": "true",
    "google.cloud.auth.service.account.json.keyfile": "/opt/spark/sa-key.json",
}

PROCESS_DATA_SCRIPT = "/opt/jobs/process_data.py"

SPARK_CONN_ID = os.getenv("SPARK_CONN_ID", "spark-conn")
SPARK_CONN_TYPE = os.getenv("SPARK_CONN_TYPE", "spark")
SPARK_HOST = os.getenv("SPARK_HOST", "spark")
SPARK_PORT = int(os.getenv("SPARK_PORT", "7077"))

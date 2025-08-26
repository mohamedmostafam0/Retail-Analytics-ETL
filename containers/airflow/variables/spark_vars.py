import os

SPARK_CONN_ID = os.getenv("SPARK_CONN_ID", "spark-conn")
SPARK_CONN_TYPE = os.getenv("SPARK_CONN_TYPE", "spark")
SPARK_HOST = os.getenv("SPARK_HOST", "spark://192.168.0.1")
SPARK_PORT = int(os.getenv("SPARK_PORT", "7077"))

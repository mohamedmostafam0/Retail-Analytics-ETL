import json
import subprocess
import logging
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

logger = logging.getLogger(__name__)

# Import from variables folder
from variables.minio_vars import (
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_REGION,
    MINIO_CONN_TYPE,
    AWS_REGION,
    AWS_ENDPOINT_URL,
)

from variables.spark_vars import (
    SPARK_HOST,
    SPARK_PORT,
    SPARK_CONN_ID,
    SPARK_CONN_TYPE,
)

logger.info("your minio variables are: %s", json.dumps({
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "MINIO_REGION": MINIO_REGION,
    "MINIO_CONN_TYPE": MINIO_CONN_TYPE,
    "AWS_REGION": AWS_REGION,
    "AWS_ENDPOINT_URL": AWS_ENDPOINT_URL,
}))

logger.info("your spark variables are: %s", json.dumps({
    "SPARK_HOST": SPARK_HOST,
    "SPARK_PORT": SPARK_PORT,
    "SPARK_CONN_ID": SPARK_CONN_ID,
    "SPARK_CONN_TYPE": SPARK_CONN_TYPE,
}))

def add_airflow_connection(conn_id, conn_type, extra=None, host=None, port=None):
    """Helper to create/update an Airflow connection via CLI."""
    cmd = ["airflow", "connections", "add", conn_id, "--conn-type", conn_type]

    if extra:
        cmd.extend(["--conn-extra", json.dumps(extra)])
    if host:
        cmd.extend(["--conn-host", host])
    if port:
        cmd.extend(["--conn-port", str(port)])

    print(f"Creating Airflow connection: {conn_id}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"✅ Successfully added {conn_id}")
    else:
        print(f"❌ Failed to add {conn_id}: {result.stderr}")


# -------------------------
# Dedicated MinIO connection (minio_conn)
# -------------------------
minio_extra = {
    "aws_access_key_id": MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
    "region_name": MINIO_REGION,
    "endpoint_url": AWS_ENDPOINT_URL,
}
add_airflow_connection(
    conn_id="minio_conn",
    conn_type=MINIO_CONN_TYPE,
    extra=minio_extra,
)

# -------------------------
# Spark connection
# -------------------------


spark_extra = {
    "deploy-mode": "client",
    "spark-binary": "spark-submit"
}

# Drop old connection (optional safeguard)
subprocess.run(["airflow", "connections", "delete", SPARK_CONN_ID or "spark-conn"])

# Add new connection
add_airflow_connection(
    conn_id=SPARK_CONN_ID or "spark-conn",
    conn_type=SPARK_CONN_TYPE or "spark",
    host=SPARK_HOST or "spark",     # 👈 must not be None
    port=SPARK_PORT or 7077,        # 👈 must not be None
    extra=spark_extra
)

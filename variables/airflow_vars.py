import os

DATA_PATH = "/opt/airflow/data"

AIRFLOW__CORE__EXECUTOR = os.getenv("AIRFLOW__CORE__EXECUTOR", "LocalExecutor")
AIRFLOW__CORE__SQL_ALCHEMY_CONN = os.getenv(
    "AIRFLOW__CORE__SQL_ALCHEMY_CONN",
    "postgresql+psycopg2://airflow:airflow@postgres/airflow",
)
AIRFLOW__CORE__FERNET_KEY = os.getenv("AIRFLOW__CORE__FERNET_KEY")
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION = os.getenv("AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION", "true")
AIRFLOW__CORE__LOAD_EXAMPLES = os.getenv("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
AIRFLOW__API__AUTH_BACKEND = os.getenv("AIRFLOW__API__AUTH_BACKEND", "airflow.api.auth.backend.basic_auth")
AIRFLOW_CONN_POSTGRES_DEFAULT = os.getenv("AIRFLOW_CONN_POSTGRES_DEFAULT")

AIRFLOW_UID = os.getenv("AIRFLOW_UID", "50000")
AIRFLOW_GID = os.getenv("AIRFLOW_GID", "0")

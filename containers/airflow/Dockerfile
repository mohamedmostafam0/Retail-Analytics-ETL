FROM apache/airflow:2.9.2

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# COPY setup_conn.py $AIRFLOW_HOME


USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk wget curl && \
    rm -rf /var/lib/apt/lists/*


USER airflow

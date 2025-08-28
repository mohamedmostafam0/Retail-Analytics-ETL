FROM bitnami/spark:latest

USER root

RUN pip install numpy

# Install Spark BigQuery Connector and GCS Connector
# Replace versions as needed based on your Spark and Scala versions
ENV SPARK_BIGQUERY_CONNECTOR_VERSION=0.34.0
ENV GCS_CONNECTOR_VERSION=hadoop3-2.2.20

RUN wget https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/${SPARK_BIGQUERY_CONNECTOR_VERSION}/spark-bigquery-with-dependencies_2.12-${SPARK_BIGQUERY_CONNECTOR_VERSION}.jar -P /opt/bitnami/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/google/cloud/hadoop/hadoop-gcs/${GCS_CONNECTOR_VERSION}/hadoop-gcs-${GCS_CONNECTOR_VERSION}.jar -P /opt/bitnami/spark/jars/

USER default

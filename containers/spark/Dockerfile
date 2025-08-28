FROM bitnami/spark:latest

USER root


# Install Spark BigQuery Connector and GCS Connector
# Replace versions as needed based on your Spark and Scala versions
ENV SPARK_BIGQUERY_CONNECTOR_VERSION=0.34.0

RUN install_packages wget && \
    pip install numpy && \
    wget https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.34.0/spark-bigquery-with-dependencies_2.12-0.34.0.jar \
    -P /opt/bitnami/spark/jars/

USER 1001

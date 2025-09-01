FROM bitnami/spark:3.5

USER root

ENV SPARK_BIGQUERY_CONNECTOR_VERSION=0.42.2

RUN install_packages wget && \
    pip install numpy

USER 1001

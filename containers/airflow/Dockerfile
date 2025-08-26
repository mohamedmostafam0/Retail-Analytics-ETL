FROM apache/airflow:2.9.2
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY quarto.sh /
RUN cd / && bash /quarto.sh

COPY setup_conn.py $AIRFLOW_HOME

COPY /variables $AIRFLOW_HOME/variables

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk


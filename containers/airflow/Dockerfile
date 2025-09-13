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
    default-jdk wget curl && \
    rm -rf /var/lib/apt/lists/*

# ✅ Match Hadoop and AWS JAR versions with Spark
ENV HADOOP_VERSION=3.4.0
ENV AWS_JAVA_SDK_BUNDLE_VERSION=1.12.539

RUN mkdir -p /opt/airflow/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P /opt/airflow/jars/ && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_JAVA_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION}.jar -P /opt/airflow/jars/

USER airflow

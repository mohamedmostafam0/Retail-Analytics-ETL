#!/bin/bash

# Set Spark environment
export SPARK_HOME=/opt/spark
export JAVA_HOME=/opt/java/openjdk

# Add extra JARs to classpath
export SPARK_CLASSPATH="$SPARK_HOME/jars-extra/*:$SPARK_CLASSPATH"

echo "SPARK_HOME: $SPARK_HOME"
echo "JAVA_HOME: $JAVA_HOME"
echo "SPARK_MODE: $SPARK_MODE"

# Start Spark master or worker based on SPARK_MODE environment variable
if [ "$SPARK_MODE" = "master" ]; then
    echo "Starting Spark Master..."
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
        --host ${SPARK_MASTER_HOST:-0.0.0.0} \
        --port ${SPARK_MASTER_PORT:-7077} \
        --webui-port ${SPARK_MASTER_WEBUI_PORT:-8080}
elif [ "$SPARK_MODE" = "worker" ]; then
    echo "Starting Spark Worker..."
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
        ${SPARK_MASTER_URL:-spark://spark:7077}
else
    echo "SPARK_MODE not set. Please set it to 'master' or 'worker'"
    exit 1
fi
FROM apache/spark:3.5.6-scala2.12-java11-ubuntu

USER root

# Install required packages
RUN apt-get update && \
    apt-get install -y wget curl python3-pip && \
    pip3 install numpy && \
    rm -rf /var/lib/apt/lists/*

# Set up the start script
COPY spark-start.sh /opt/spark/sbin/spark-start.sh
RUN chmod +x /opt/spark/sbin/spark-start.sh

USER spark

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV SPARK_CLASSPATH=/opt/spark/jars-extra/*   
ENV SPARK_EXTRA_CLASSPATH=/opt/spark/jars-extra/*

EXPOSE 7077 8080

CMD ["/opt/spark/sbin/spark-start.sh"]
FROM apache/airflow:2.8.0-python3.11

# Switch to root user to install system packages
USER root

# Install OpenJDK 11 for Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Download and install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set Spark environment variables
ENV PATH=$PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Create directory for Spark logs
RUN mkdir -p /opt/spark/logs && \
    chown -R airflow:0 /opt/spark && \
    chmod -R g+rw /opt/spark

# Switch back to airflow user
USER airflow

# Install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Create necessary directories
RUN mkdir -p /opt/airflow/data/raw /opt/airflow/data/processed 
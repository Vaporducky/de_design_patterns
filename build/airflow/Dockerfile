FROM apache/airflow:3.0.4
LABEL authors="elianther"
USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set Java env vars
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install a lightweight editor
RUN apt-get install -y vim && apt-get clean;

# Add custom Python module folder to PYTHONPATH
ENV PYTHONPATH="/opt/code/common:${PYTHONPATH}"

USER airflow

# Get Apache Spark and Airflow Spark providers
# Install Python deps in one layer
RUN pip install --no-cache-dir \
    pyspark==3.5.6 \
    apache-airflow-providers-apache-spark==5.3.2 \
    delta-spark==3.3.2
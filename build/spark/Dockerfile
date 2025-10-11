FROM bitnami/spark:3.5.6
LABEL authors="elianther"

# Set the environment variable for Spark version
ENV SPARK_VERSION=3.5.6

# Set the necessary Spark configurations for Delta Lake
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
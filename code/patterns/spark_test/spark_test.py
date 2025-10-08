import logging
from pathlib import Path

from pyspark.sql import SparkSession

from common_utilities import setup_logging


def main():
    logging.info(f"Starting Spark-Test Job.")

    spark = (
        SparkSession.builder
        .appName("PySpark Example")
        .getOrCreate()
    )

    logging.info(f"Reading Data.")
    df = (
        spark.read
        .format("parquet")
        .load("/opt/spark-data/idempotency/fast_metadata_cleaner/table")
    )

    logging.info(f"Showing Data.")
    df.show()

    logging.info(f"Writing Delta Table")
    table_location = "/opt/spark-data/idempotency/fast_metadata_cleaner/delta_table"
    Path(table_location).mkdir(exist_ok=True)
    df.write.format("delta").mode("append").options(path=table_location).saveAsTable("my_table")

    spark.stop()


if __name__ == "__main__":
    setup_logging()
    main()

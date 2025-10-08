import logging
from pathlib import Path
from textwrap import dedent

import delta
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, DateType, StringType, ShortType

from common_utilities import setup_logging

JOB_NAME = "table_partition_creator"
SCHEMA = "idempotent"
TABLE_NAME = "user_activity"
TABLE_NAMESPACE = f"`{SCHEMA}`.`{TABLE_NAME}`"
TABLE_LOCATION = r"/tmp/spark-data/warehouse/idempotent.db/user_activity"


def main():
    logging.info(f"Starting `{JOB_NAME}` Job.")

    spark = (
        SparkSession
        .builder
        .appName("table_partition_creator")
        .getOrCreate()
    )

    # Create schema if it doesn't exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{SCHEMA}`")

    # Create table if it doesn't exist
    if not delta.DeltaTable.isDeltaTable(spark, TABLE_LOCATION):
        logging.info(f"Creating table: {TABLE_NAMESPACE}")
        (
            delta.DeltaTable.create(spark)
            .tableName(TABLE_NAMESPACE)
            .addColumn("user_id", dataType=LongType())
            .addColumn("activity_date", dataType=DateType())
            .addColumn("activity_type", dataType=StringType())
            .addColumn("__year_week_of_year__",
                       dataType=StringType(),
                       nullable=False)
            .partitionedBy("__year_week_of_year__")
            .execute()
        )
        logging.info(f"{TABLE_NAMESPACE} has been created successfully.")

    logging.info(f"Creating weekly view: `idempotency`.`2025_01_user_activity_vw`.")
    spark.sql(f"""
        CREATE OR REPLACE VIEW `{SCHEMA}`.`2025_01_user_activity_vw` AS
            SELECT
                user_id,
                activity_date,
                activity_type
            FROM {TABLE_NAMESPACE}
            WHERE __year_week_of_year__ = '2025_01'
        ;
    """)
    logging.info(f"Weekly view `idempotency`.`2025_01_user_activity_vw` created.")


if __name__ == "__main__":
    setup_logging()
    main()

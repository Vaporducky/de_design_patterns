import logging
from datetime import date, timedelta, datetime
from pathlib import Path
from typing import Any

import dbldatagen as dg
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import (
    LongType,
    DateType,
    StringType
)

# Data presentation
COLUMN_ORDER = ("scenario",
                "user_id",
                "activity_date",
                "activity_type",
                "__year_week_of_year__")

# DATA GENERATION
SHUFFLE_PARTITIONS = 1
RANDOM_SEED = 42

DATASPEC_CONFIGURATION: dict[str, Any] = {
    "data_overwrite/user_activity": {
        "name": "user_activity",
        "partitions": SHUFFLE_PARTITIONS,
        "randomSeed": RANDOM_SEED,
        "random": True,
    },
}


def week_bounds(year: int, week: int):
    """Return the start (Monday) and end (Sunday) dates of an ISO week.

    Args:
        year: The ISO year.
        week: The ISO week number (1–53).

    Returns:
        tuple[str, str]
    """
    start = date.fromisocalendar(year, week, 1)  # Monday
    end = start + timedelta(days=6)              # Sunday
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def build_user_activity_entity(dataspec_config: dict,
                               spark: SparkSession,
                               rows: int) -> DataFrame:
    """
    Generates a synthetic dataset using dbldatagen.

    Parameters:
    - spark (SparkSession): SparkSession instance.
    - config (dict): Data specification (includes number of rows, partitions,
                     etc.)
    """
    data_spec = (
        dg.DataGenerator(
            sparkSession=spark,
            rows=rows,
            **dataspec_config
        )
        .withColumn(
            "user_id",
            LongType(),
            minValue=10_001,
            maxValue=11_000,
        )
        .withColumn(
            "activity_date",
            DateType(),
            data_range=dg.DateRange(begin="2020-01-01 00:00:00",
                                    end="2021-12-31 00:00:00",
                                    interval="days=1")
        )
        .withColumn(
            "activity_type",
            StringType(),
            values=("login", "view_page", "purchase", "logout"),
            weights=(0.15, 0.65, 0.1, 0.1),
            random=True
        )
    )

    return data_spec.build()


def build_user_activity_entity_incoming(dataspec_config: dict,
                                        spark: SparkSession,
                                        rows: int,
                                        start_date: str,
                                        end_date: str) -> DataFrame:
    """
    Generates a synthetic dataset using dbldatagen.

    Parameters:
    - spark (SparkSession): SparkSession instance.
    - config (dict): Data specification (includes number of rows, partitions,
                     etc.)
    """
    data_spec = (
        dg.DataGenerator(
            sparkSession=spark,
            rows=rows,
            **dataspec_config
        )
        .withColumn(
            "user_id",
            LongType(),
            minValue=10_001,
            maxValue=11_000
        )
        .withColumn(
            "activity_date",
            DateType(),
            data_range=dg.DateRange(begin=f"{start_date} 00:00:00",
                                    end=f"{end_date} 00:00:00",
                                    interval="days=1")
        )
        .withColumn(
            "activity_type",
            StringType(),
            values=("login", "view_page", "purchase", "logout"),
            weights=(0.15, 0.65, 0.1, 0.1),
            random=True
        )
    )

    return data_spec.build()


def main():
    """
    The example here will take care of the following scenarios:
        0. New partition (partition which has never been processed before)
        1. Reprocessing of a single partition
        2. Reprocessing of several partitions
        3. Full backfill
    """
    spark = (
        SparkSession.builder
        .getOrCreate()
    )
    # Determine start and end date for different scenarios

    s1_start_date, s1_end_date = week_bounds(
        year=datetime.now().year,
        week=datetime.now().isocalendar()[1]
    )
    s2_start_date, s2_end_date = week_bounds(
        year=datetime(2021, 12, 18).year,
        week=datetime(2021, 12, 18).isocalendar()[1]
    )
    s31_start_date, s31_end_date = week_bounds(
        year=datetime(2021, 11, 1).year,
        week=datetime(2021, 11, 1).isocalendar()[1]
    )
    s32_start_date, s32_end_date = week_bounds(
        year=datetime(2021, 11, 8).year,
        week=datetime(2021, 11, 8).isocalendar()[1]
    )

    user_activity_input = build_user_activity_entity(
        dataspec_config=DATASPEC_CONFIGURATION["data_overwrite/user_activity"],
        spark=spark,
        rows=10_000
    )
    # weekly_active_users = (
    #     user_activity_input
    #     .groupby("__year_week_of_year__")
    #     .agg(F.count_distinct(F.col("user_id")))
    # )

    user_activity_incoming_s1 = build_user_activity_entity_incoming(
        dataspec_config=DATASPEC_CONFIGURATION["data_overwrite/user_activity"],
        spark=spark,
        rows=3_000,
        start_date=s1_start_date,
        end_date=s1_end_date
    )
    user_activity_incoming_s2 = build_user_activity_entity_incoming(
        dataspec_config=DATASPEC_CONFIGURATION["data_overwrite/user_activity"],
        spark=spark,
        rows=3_000,
        start_date=s2_start_date,
        end_date=s2_start_date
    )
    user_activity_incoming_s31 = build_user_activity_entity_incoming(
        dataspec_config=DATASPEC_CONFIGURATION["data_overwrite/user_activity"],
        spark=spark,
        rows=3_000,
        start_date=s31_start_date,
        end_date=s31_start_date
    )
    user_activity_incoming_s32 = build_user_activity_entity_incoming(
        dataspec_config=DATASPEC_CONFIGURATION["data_overwrite/user_activity"],
        spark=spark,
        rows=3_000,
        start_date=s32_start_date,
        end_date=s32_start_date
    )

    logging.info("Displaying data:")
    user_activity_input.show()

    logging.info("Loading base scenario 0.")
    (
        user_activity_input
        .withColumn(
            "__year_week_of_year__",
            F.concat(
                F.datepart(F.lit("YEAR"), F.col("activity_date")),
                F.lit("_"),
                F.datepart(F.lit("week"), F.col("activity_date"))
            )
        )
        .withColumn("scenario", F.lit(0))
        .select(*COLUMN_ORDER)
        .write
        .format("parquet")
        .mode("overwrite")
        .save((Path.cwd() / "user_activity").as_posix())
    )

    logging.info("Loading scenario 1.")
    (
        user_activity_input
        .unionByName(user_activity_incoming_s1)
        .withColumn(
            "__year_week_of_year__",
            F.concat(
                F.datepart(F.lit("YEAR"), F.col("activity_date")),
                F.lit("_"),
                F.datepart(F.lit("week"), F.col("activity_date"))
            )
        )
        .withColumn("scenario", F.lit(1))
        .select(*COLUMN_ORDER)
        .write
        .format("parquet")
        .mode("append")
        .save((Path.cwd() / "user_activity").as_posix())
    )

    logging.info("Loading scenario 2.")
    (
        user_activity_input
        .unionByName(user_activity_incoming_s2)
        .withColumn(
            "__year_week_of_year__",
            F.concat(
                F.datepart(F.lit("YEAR"), F.col("activity_date")),
                F.lit("_"),
                F.datepart(F.lit("week"), F.col("activity_date"))
            )
        )
        .withColumn("scenario", F.lit(2))
        .select(*COLUMN_ORDER)
        .write
        .format("parquet")
        .mode("append")
        .save((Path.cwd() / "user_activity").as_posix())
    )

    logging.info("Loading scenario 3.")
    (
        user_activity_input
        .unionByName(user_activity_incoming_s31)
        .unionByName(user_activity_incoming_s32)
        .withColumn(
            "__year_week_of_year__",
            F.concat(
                F.datepart(F.lit("YEAR"), F.col("activity_date")),
                F.lit("_"),
                F.datepart(F.lit("week"), F.col("activity_date"))
            )
        )
        .withColumn("scenario", F.lit(3))
        .select(*COLUMN_ORDER)
        .write
        .format("parquet")
        .mode("append")
        .save((Path.cwd() / "user_activity").as_posix())
    )


if __name__ == "__main__":
    main()

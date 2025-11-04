from unittest import TestCase
from unittest.mock import patch, MagicMock, ANY

import pytest
from parameterized import parameterized
from pyspark.sql import SparkSession, DataFrame, functions as F, Column
from pyspark.conf import SparkConf

import src.data_overwrite_utilities as utilities
from tests.tests_constants import SPARK_CONFIGURATION


@pytest.mark.unit
class TestDataOverwriteUtilities(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        # Create Spark configuration from dictionary
        _config: SparkConf = SparkConf().setAll([
            (key, value) for key, value in SPARK_CONFIGURATION.items()
        ])

        cls.spark: SparkSession = (
            SparkSession
            .builder
            .appName("test")
            .config(conf=_config)
            .getOrCreate()
        )

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @parameterized.expand([
        ("string_partition", ["scenario"], False),
        ("column_partition", ["scenario"], True),
    ])
    def test_latest_partition_strategy_single_partition(
            self,
            _: str,
            partitions: list[str | Column],
            col_flag: bool
    ):
        # Convert to PySpark column
        if col_flag:
            partitions = [F.col(partition) for partition in partitions]

        df: DataFrame = self.spark.createDataFrame(
            data=([0, 1], [1, 2], [1, 3]),
            schema="scenario int, value int"
        )

        expected_predicate = (
            F.lit(True)
            & (
                F.when(F.col("scenario").isNull(), F.lit(True))
                .otherwise(F.column("scenario") > 1)
            )
        )

        actual_predicate = (
            utilities
            .LatestPartitionStrategy
            .execute(df, partition_cols=partitions)
        )

        self.assertEqual(
            str(expected_predicate),
            str(actual_predicate),
            msg=f"{str(expected_predicate)} <> {str(actual_predicate)}"
        )

    @parameterized.expand([
        ("string_partition", ["scenario", "day"], False),
        ("string_partition", ["scenario", "day"], True)
    ])
    def test_latest_partition_strategy_multiple_partitions(
            self,
            _: str,
            partitions: list[str | Column],
            col_flag: bool
    ):
        # Convert to PySpark column
        if col_flag:
            partitions = [F.col(partition) for partition in partitions]

        df: DataFrame = self.spark.createDataFrame(
            data=([0, 0, 1], [1, 1, 2], [1, 2, 3], [1, 2, 4]),
            schema="scenario int, day int, value int"
        )

        expected_predicate = (
            F.lit(True)
            & (
                F.when(F.col("scenario").isNull(), F.lit(True))
                .otherwise(F.col("scenario") > 1)
            ) &
            (
                F.when(F.col("day").isNull(), F.lit(True))
                .otherwise(F.col("day") > 2)
            )
        )

        actual_predicate = (
            utilities
            .LatestPartitionStrategy
            .execute(df, partition_cols=partitions)
        )

        self.assertEqual(
            str(expected_predicate),
            str(actual_predicate),
            msg=f"{str(expected_predicate)} <> {str(actual_predicate)}"
        )

    def test_latest_partition_strategy_empty_dataframe(self):
        df: DataFrame = self.spark.createDataFrame(data=([]),
                                                   schema="scenario int")

        with self.assertRaisesRegex(ValueError,
                                    "Cannot find the latest partition."):
            utilities.LatestPartitionStrategy.execute(df, ["scenario"])

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
        ("string_partition", ["scenario"], False, "    `scenario` > 1"),
        ("column_partition", ["scenario"], True, "    `scenario` > 1"),
    ])
    def test_latest_partition_strategy_single_partition(
            self,
            _: str,
            partitions: list[str | Column],
            col_flag: bool,
            expected_predicate
    ):
        # Convert to PySpark column
        if col_flag:
            partitions = [F.col(partition) for partition in partitions]

        df: DataFrame = self.spark.createDataFrame(
            data=([0, 1], [1, 2], [1, 3]),
            schema="scenario int, value int"
        )

        actual_predicate: str = (
            utilities
            .LatestPartitionStrategy
            .execute(df, partitions=partitions)
        )

        self.assertEqual(
            expected_predicate,
            actual_predicate,
            msg=f"{expected_predicate} <> {actual_predicate}"
        )

    @parameterized.expand([
        (
            "string_partition",
            ["scenario", "day"],
            False,
            "    `scenario` > 1\nAND `day` > 2"
        ),
        (
            "string_partition",
            ["scenario", "day"],
            True,
            "    `scenario` > 1\nAND `day` > 2"
        ),
    ])
    def test_latest_partition_strategy_multiple_partitions(
            self,
            _: str,
            partitions: list[str | Column],
            col_flag: bool,
            expected_predicate
    ):
        # Convert to PySpark column
        if col_flag:
            partitions = [F.col(partition) for partition in partitions]

        df: DataFrame = self.spark.createDataFrame(
            data=([0, 0, 1], [1, 1, 2], [1, 2, 3], [1, 2, 4]),
            schema="scenario int, day int, value int"
        )

        actual_predicate: str = (
            utilities
            .LatestPartitionStrategy
            .execute(df, partitions=partitions)
        )

        self.assertEqual(
            expected_predicate,
            actual_predicate,
            msg=f"{expected_predicate} <> {actual_predicate}"
        )



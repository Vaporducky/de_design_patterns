import argparse
import json
import logging
import os
from abc import ABC
from dataclasses import dataclass
from distutils.util import strtobool
from typing import Callable, Iterable, Optional

import base.strategy as strategy
import delta
from delta.tables import DeltaTableBuilder
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, LongType, Row, StringType


@dataclass
class JobArguments:
    job_name: str
    scenario: int
    partitions: dict
    table_name: str
    schema_name: str
    backfill: bool

    @staticmethod
    def _build_parser():
        # Initialize parser
        parser = argparse.ArgumentParser(
            description="Generic job argument parser."
        )

        # Add arguments
        parser.add_argument(
            "--job_name", "-j",
            type=str,
            required=True,
            help="Name of the job."
        )
        parser.add_argument(
            "--scenario",
            type=int,
            required=True,
            help="Data scenario."
        )
        parser.add_argument(
            "--partitions", "-p",
            type=json.loads,
            default="",
            required=False,
            help="""Partitions to process e.g. '"year": [2021, ...]' """
        )
        parser.add_argument(
            "--table_name", "-t",
            type=str,
            required=True,
            help="Name of the table"
        )
        parser.add_argument(
            "--schema_name", "-s",
            type=str,
            required=True,
            help="Name of the schema"
        )
        parser.add_argument(
            "--backfill",
            type=lambda x: bool(strtobool(x)),
            default=False,
            required=False,
            help="Flag which indicates if a full backfill will be performed."
        )

        return parser

    @staticmethod
    def _log_args(args: argparse.Namespace):
        # Map namespace into dictionary
        args_to_dict = {arg: value for arg, value in vars(args).items()}
        logging.info(json.dumps(args_to_dict, indent=4, default=str))

    @classmethod
    def from_args(cls, args: Optional[list[str]] = None) -> "JobArguments":
        """
        Parses CLI arguments
        """
        parser = cls._build_parser()
        parsed = parser.parse_args(args)

        logging.info("Parsed arguments:")
        logging.info(cls._log_args(parsed))

        return cls(job_name=parsed.job_name,
                   scenario=parsed.scenario,
                   partitions=parsed.partitions,
                   table_name=parsed.table_name,
                   schema_name=parsed.schema_name,
                   backfill=parsed.backfill)


class TableDefinition(ABC):
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._registry: dict[tuple[str, str], Callable] = None

    def get_table_definition(self,
                             schema: str,
                             table: str,
                             *args,
                             **kwargs) -> DeltaTableBuilder | DataFrame:
        """
        Retrieve the output of a registered table constructor.

        This method looks up a callable registered for the given schema and
        table, then executes it with the provided arguments. The return type
        depends on the implementation of the registered constructor.

        Args:
            schema (str): Schema name of the table.
            table (str): Table name within the schema.
            *args: Positional arguments passed to the table constructor.
            **kwargs: Keyword arguments passed to the table constructor.

        Returns:
            Any: The object produced by the registered constructor, such as
            a table definition, DataFrame, or another object type.

        Raises:
            ValueError: If no constructor is registered for the given schema
            and table.
        """

        try:
            constructor = self._registry[(schema, table)]
        except KeyError:
            raise ValueError(f"No table definition for {schema}.{table}")

        return constructor(*args, **kwargs)


class SourceDeltaTableDefinition(TableDefinition):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self._registry = {
            ("idempotent", "user_activity"): self._idempotent_user_activity_table,
            ("idempotent", "daily_activity"): self._idempotent_daily_activity_table
        }

    def _idempotent_user_activity_table(self) -> DeltaTableBuilder:
        delta_definition: DeltaTableBuilder = (
            delta.DeltaTable.create(self.spark)
            .tableName("`idempotent`.`user_activity`")
            .addColumn("user_id", dataType=LongType())
            .addColumn("activity_date", dataType=DateType())
            .addColumn("activity_type", dataType=StringType())
            .addColumn(
                "__year_week_of_year__",
                dataType=StringType(),
                nullable=False
            )
            .partitionedBy("__year_week_of_year__")
        )

        return delta_definition

    def _idempotent_daily_activity_table(self) -> DeltaTableBuilder:
        delta_definition: DeltaTableBuilder = (
            (
                delta.DeltaTable.create(self.spark)
                .tableName("idempotent.daily_activity")
                .addColumn("year_woy", dataType=StringType())
                .addColumn("distinct_daily_users", dataType=IntegerType())
                .partitionedBy("year_woy")
            )
        )

        return delta_definition


class TargetDeltaTableDefinition(TableDefinition):
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self._registry = {
            ("idempotent", "daily_activity"): self._idempotent_daily_activity_table,
        }

    def _idempotent_daily_activity_table(self,
                                         table_location,
                                         predicate: str) -> DataFrame:
        # Apply predicate on target definition
        target_definition: DataFrame = (
            self.spark.read
            .format("delta")
            .load(table_location)
            .select(
                F.col("__year_week_of_year__").alias("year_woy"),
                F.col("user_id")
            )
            .where(predicate)
            .groupBy("year_woy")
            .agg(
                F.count_distinct("user_id")
                .cast(IntegerType())
                .alias("distinct_daily_users")
            )
        )

        return target_definition


def _get_env_var(name: str) -> str:
    """Retrieve an environment variable or raise a clear error if missing."""
    value: Optional[str] = os.getenv(name)
    if value is None:
        raise EnvironmentError(f"Required environment variable '{name}' is not"
                               f" set.")
    return value


def table_spark_wh_location_builder(schema: str, table: str) -> str:
    """
    Build the physical location of a table in the Spark warehouse.

    This function uses the `SPARK_WAREHOUSE` environment variable as the base
    path and appends the schema and table name.

    Args:
        schema (str): The schema name of the table.
        table (str): The table name.

    Returns:
        str: The fully qualified Spark warehouse path in the form:
             `<SPARK_WAREHOUSE>/<schema>.db/<table>`.
    """
    spark_wh: str = _get_env_var("SPARK_WAREHOUSE")

    return f"{spark_wh}/{schema}.db/{table}"


def table_input_location_builder(schema: str, table: str, use_case: str) -> str:
    """
    Build the input data location for a table.

    This function uses the `DATA_PATH` environment variable as the base
    directory and appends the schema, use case, and table name.

    Args:
        schema (str): The schema name of the table.
        table (str): The table name.
        use_case (str): The use case or data domain.

    Returns:
        str: The full input data path in the form:
             `<DATA_PATH>/<schema>/<use_case>/<table>`.
    """
    data_dir: str = _get_env_var("DATA_PATH")

    return f"{data_dir}/{schema}/{use_case}/{table}"


def table_namespace_builder(schema: str, table: str) -> str:
    """
    Build the SQL namespace identifier for a table.

    The namespace is formatted with backticks to ensure that schema
    and table names containing special characters are valid in SQL queries.

    Args:
        schema (str): The schema name of the table.
        table (str): The table name.

    Returns:
        str: The fully qualified table identifier in the form:
             `<schema>`.`<table>`.
    """
    return f"`{schema}`.`{table}`"


class LatestPartitionStrategy(strategy.Strategy):
    """
    Given a target dataframe `df`, retrieve the latest partition to create a
    predicate to filter out an arbitrary source DataFrame (we would like to
    ingest only data greater than the latest partition)
    """
    @staticmethod
    def execute(df: DataFrame, partition_cols: Iterable[str | Column] = None) -> Column:
        logging.info(f"Retrieving latest partition.")
        # Determine the latest partition
        latest_partition: DataFrame = (
            df
            .select(*partition_cols)
            .sort(*partition_cols, ascending=False)
            .limit(1)
        )

        # Get the latest partition value safely
        latest_partition_value: Row = latest_partition.first()

        if latest_partition_value is None:
            raise ValueError("Cannot find the latest partition.")

        # Extract the value from the row
        latest_partition_value: dict = latest_partition_value.asDict()

        # Logical equivalent of 1=1
        predicate = F.lit(True)
        for partition, value in latest_partition_value.items():
            atomic_predicate = (
                F.when(F.col(partition).isNull(), F.lit(True))
                .otherwise(F.col(partition) > value)
            )
            predicate &= atomic_predicate

        return predicate


class CherryPickPartitionsStrategy(strategy.Strategy):
    @staticmethod
    def execute(partitions_df: DataFrame) -> Column:
        logging.info(f"Starting `Cherry Pick` partition strategy")
        partitions = partitions_df.collect()
        fields = partitions_df.columns

        # Initialize filter condition (since we are using `OR`, we set the
        # initial value as `False`)
        filter_condition = F.lit(False)

        for partition in partitions:
            # Initialize combined partition predicate
            combined_column_condition = F.lit(True)
            for field in fields:
                # Generate atomic predicate
                column_condition = (
                    F.when(F.col(field).isNull(), F.lit(True))
                    .otherwise(F.col(field) == partition[field])
                )

                # Append to combined partition predicate (AND)
                combined_column_condition &= column_condition

            # Append to predicate (OR)
            filter_condition |= combined_column_condition

        return filter_condition


class TrivialPartitionsStrategy(strategy.Strategy):
    @staticmethod
    def execute() -> Column:
        return F.lit(True)


class PartitionStrategyContext:
    _strategies: dict[str, strategy.Strategy] = {
        "latest": LatestPartitionStrategy,
        "cherry_pick": CherryPickPartitionsStrategy,
        "trivial": TrivialPartitionsStrategy,
    }

    @staticmethod
    def _get_strategy(strategy_name: str) -> strategy.Strategy:
        """
        Returns an instance of a strategy based on the given name.

        Args:
            strategy_name (str): The key representing the desired
                strategy.
        Raises:
            ValueError: If the strategy name is not supported.

        Returns:
            Strategy: An instance of the requested strategy.
        """
        strategy_class = PartitionStrategyContext._strategies.get(strategy_name.lower())
        if strategy_class is None:
            raise ValueError(f"Strategy `{strategy_name}` is not supported.")

        return strategy_class

    def execute_strategy(self, strategy_name, *args, **kwargs) -> Column:
        """
        Executes the current strategy with the provided arguments.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Any: The result from the strategy's execution.
        """
        logging.info("Entering partition strategy context.")
        logging.info(f"Executing partition strategy: `{strategy_name}`")
        logging.info(f"With args: {args}")
        logging.info(f"With kwargs: {kwargs}")

        return self._get_strategy(strategy_name).execute(*args, **kwargs)

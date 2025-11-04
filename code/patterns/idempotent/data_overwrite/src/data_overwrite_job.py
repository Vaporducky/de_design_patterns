import json
import logging
from pprint import pformat

import delta
from pyspark.sql import (
    SparkSession,
    DataFrame,
    DataFrameWriter,
    functions as F,
)

import src.data_overwrite_utilities as utilities
import src.data_overwrite_constants as constants


class DataOverwriteJob:
    def __init__(self, job_args: utilities.JobArguments):
        self._job_args: utilities.JobArguments = job_args
        self.spark: SparkSession = (
            SparkSession
            .builder
            .appName(job_args.job_name)
            .getOrCreate()
        )
        self.table_mapping: dict[str, str] = constants.TABLE_MAPPING
        self.src_table_definition = utilities.SourceDeltaTableDefinition(self.spark)
        self.tgt_table_definition = utilities.TargetDeltaTableDefinition(self.spark)

    @property
    def table_wh_location_mapping(self) -> dict[str, str]:
        """
        Map SQL table namespaces to their corresponding Spark warehouse
        locations.

        Constructs a dictionary where each key is a fully qualified table
        namespace (formatted as `<schema>.<table>` using backticks) and each
        value is the corresponding Spark warehouse location for that table.

        The mapping is generated dynamically from a predefined table-to-schema
        mapping in `constants.TABLE_MAPPING`.

        Returns:
            dict[str, str]: A dictionary mapping table namespace identifiers to
                            Spark warehouse paths.
        """
        # Use the following structure:
        # table_namespace: table_warehouse_location
        table_location: dict[str, str] = {}

        for table, schema in self.table_mapping.items():
            namespace = utilities.table_namespace_builder(schema=schema,
                                                          table=table)
            location = utilities.table_spark_wh_location_builder(schema=schema,
                                                                 table=table)
            table_location[namespace] = location

        return table_location

    @property
    def table_input_location_mapping(self) -> dict[str, str]:
        # Use the following structure:
        # table_namespace: table_input_location
        table_location: dict[str, str] = {}

        for table, schema in self.table_mapping.items():
            namespace = utilities.table_namespace_builder(schema=schema,
                                                          table=table)
            location = utilities.table_input_location_builder(
                schema=schema,
                table=table,
                use_case=self._job_args.job_name
            )
            table_location[namespace] = location

        return table_location

    @property
    def table_namespace_mapping(self) -> dict[str, str]:
        """
        Map table names to fully qualified SQL namespaces.

        Constructs a dictionary where each key is a table name and each value
        is the fully qualified table identifier in the form
        `schema`.`table`, suitable for SQL queries.

        The mapping is generated from `self.table_mapping`, which provides the
        schema for each table.

        Returns:
            mapping (dict[str, str]): Mapping of table names to fully qualified
                                      SQL namespaces.
        """
        # Use the following structure:
        # table_name: `schema`.`table_name`
        mapping: dict[str, str] = {}

        for table, schema in self.table_mapping.items():
            mapping[table] = utilities.table_namespace_builder(schema=schema,
                                                               table=table)
        return mapping

    def _get_table_location(self, table: str, wh_flag: bool) -> str:
        """
        Get the warehouse or input location for a specific table.

        Retrieves the fully qualified path for the given table based on the
        specified location type. If `wh_flag` is True, returns the Spark
        warehouse location; otherwise, returns the input data location.

        Args:
            table (str): The name of the table to look up.
            wh_flag (bool): Flag indicating which location to return

        Returns:
            table_location (str): The corresponding path for the table and
                                  location type.

        Raises:
            KeyError: If the table or its namespace is not found in the
                      internal mappings.
        """
        table_namespace = self.table_namespace_mapping[table]
        table_location: str = (
            self.table_wh_location_mapping[table_namespace] if wh_flag
            else self.table_input_location_mapping[table_namespace]
        )

        return table_location

    def _create_schema(self, schema: str):
        # Create schema if it doesn't exist
        logging.info(f"Creating schema `{schema}` if it not exists.")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")

    def _create_tables(self) -> None:
        logging.info(f"Table creation started.")

        for table, schema in self.table_mapping.items():
            self._create_schema(schema)

            table_namespace = self.table_namespace_mapping[table]
            table_exists: bool = delta.DeltaTable.isDeltaTable(
                self.spark,
                self.table_wh_location_mapping[table_namespace]
            )

            # Create table if it doesn't exist
            if not table_exists:
                logging.info(f"Creating table: {table_namespace}")
                (
                    self.src_table_definition
                    .get_table_definition(schema=schema, table=table)
                    .execute()
                )
                logging.info(f"{table_namespace} created successfully.")

        logging.info(f"Table creation ended.")

    def _load_tables(self, scenario: int):
        logging.info(f"Data loading by scenario started.")

        user_activity_namespace = self.table_namespace_mapping["user_activity"]
        logging.info(f"Loading data for {user_activity_namespace}")
        logging.info(f"Reading data for scenario `{scenario}`.")

        # Read from the base data path `./data`
        df = (
            self.spark.read
            .format("parquet")
            .load(self._get_table_location("user_activity", wh_flag=False))
            .where(f"scenario = {scenario}")
        )

        logging.info(f"Writing data for scenario `{scenario}`.")
        # Load required data per scenario
        (
            df.write
            .partitionBy("__year_week_of_year__")
            .format("delta")
            .mode("overwrite")
            .options(overwriteSchema=True)
            .save(self._get_table_location("user_activity", wh_flag=True))
        )

        logging.info(f"Data loading by scenario ended.")

    def _is_freshly_built_delta_table(self, table_location: str) -> bool:
        """Check if a Delta table is freshly created based on its version."""
        logging.info(f"Checking if Delta table is freshly created.")

        history = (delta.DeltaTable
                   .forPath(self.spark, table_location)
                   .history())
        history.show(truncate=False)
        # If history is empty (i.e., no logs exist), the table is freshly
        # created
        if not history.head(1):
            return True

        # Otherwise, check the version of the latest entry in the history
        latest_version = history.head(1)[0]["version"]

        # If the latest version is 0, the table is freshly created
        return latest_version == 0

    def _parse_partitions(self):
        data = [
            (v,)
            for values in self._job_args.partitions.values() for v in values
        ]
        schema = list(self._job_args.partitions.keys())

        partition_df = self.spark.createDataFrame(data=data, schema=schema)

        return partition_df

    def _partition_processing(self,
                              src_location: str,
                              tgt_location: str,
                              partitions: dict) -> None:
        # Create partitioning strategy. It will create a partition predicate
        logging.info("Partition processing started.")
        partition_context = utilities.PartitionStrategyContext()

        logging.info(f"Creating partition predicate.")

        df_partition_source: DataFrame = (
            self.spark.read.format("delta").load(tgt_location)
        )

        if self._job_args.backfill:
            logging.info(f"Fresh build. Processing all the data for the "
                         f"target definition at `{tgt_location}`.")
            predicate = partition_context.execute_strategy("trivial")
        elif partitions:
            logging.info(
                f"Starting reprocessing process of the following partitions:"
                f"{pformat(partitions)}"
            )
            partitions_df = self._parse_partitions()
            predicate = partition_context.execute_strategy("cherry_pick",
                                                           partitions_df)
        else:
            logging.info(f"Processing new partitions.")
            predicate = partition_context.execute_strategy("latest",
                                                           df_partition_source,
                                                           ("year_woy",))

        # Factory table
        logging.info("Retrieving target table definition.")
        daily_activity_df: DataFrame = (
            self.tgt_table_definition.get_table_definition(
                schema="idempotent",
                table="daily_activity",
                table_location=src_location,
                predicate=predicate
            )
        )

        logging.info(f"Target count: `{daily_activity_df.count()}`")
        delta_tgt_table = delta.DeltaTable.forPath(self.spark, tgt_location)
        partition_cols = delta_tgt_table.detail().select("partitionColumns").first()[0]
        logging.info(f"Partitions before writing: {delta_tgt_table.toDF().select(*partition_cols).count()}")

        # Write to table
        logging.info(f"Writing to target table: `{tgt_location}`")
        # delta.DeltaTable.forPath(self.spark, tgt_location).delete(predicate)
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        (
            daily_activity_df
            .write
            .partitionBy("year_woy")
            .format("delta")
            .mode("overwrite")
            .save(tgt_location)
        )
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "STATIC")

        delta_tgt_table = delta.DeltaTable.forPath(self.spark, tgt_location)
        logging.info(f"After before writing: {delta_tgt_table.toDF().select(*partition_cols).count()}")

        logging.info("Partition processing ended.")
    
    def run(self):
        self.spark.sparkContext.setLogLevel("WARN")
        logging.info(f"Table namespace mapping: {self.table_namespace_mapping}")
        logging.info(f"Table warehouse mapping: {self.table_wh_location_mapping}")

        self._create_tables()
        self._load_tables(self._job_args.scenario)
        self._partition_processing(
            partitions=self._job_args.partitions,
            src_location=self._get_table_location("user_activity", True),
            tgt_location=self._get_table_location("daily_activity", True)
        )

__all__ = ["SPARK_CONFIGURATION"]

# Spark Delta Configuration
SPARK_DELTA_CONFIG = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}

# Spark Configuration
JAVA_OPTIONS: str = ("-Ddelta.log.cacheSize=3 "
                     "-XX:+CMSClassUnloadingEnabled -XX:+UseCompressedOops")
SPARK_CONFIGURATION: dict[str, str] = {
    "spark.executor.memory": "2g",
    "spark.driver.memory": "2g",
    "spark.executor.cores": "1",
    "spark.sql.shuffle.partitions": "1",
    "spark.databricks.delta.snapshotPartitions": "1",
    "spark.ui.showConsoleProgress": "false",
    "spark.ui.enabled": "false",
    "spark.ui.dagGraph.retainedRootRDDs": "1",
    "spark.ui.retainedJobs": "1",
    "spark.ui.retainedStages": "1",
    "spark.ui.retainedTasks": "1",
    "spark.sql.ui.retainedExecutions": "1",
    "spark.worker.ui.retainedExecutors": "1",
    "spark.worker.ui.retainedDrivers": "1",
    "spark.dynamicAllocation.enabled": "false",
    "spark.driver.extraJavaOptions": JAVA_OPTIONS,
    **SPARK_DELTA_CONFIG
}

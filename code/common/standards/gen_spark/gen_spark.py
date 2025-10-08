from enum import Enum, auto
from abc import abstractmethod

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from base.singleton import SingletonABCMeta

# Types
SparkConfiguration = dict[str, str]


class BaseSpark(metaclass=SingletonABCMeta):
    @abstractmethod
    def spark(self) -> SparkSession:
        pass


class Strategy(str, Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name.lower()

    DELTA = auto()
    VANILLA = auto()
    HIVE = auto()


class GenSpark(BaseSpark):
    """
    A generic Spark class which aims to speed up the configuration of a
    SparkSession as well as setting "standard" parameters for the SparkContext.
    """
    _delta_config = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    }

    def __init__(self, conf: SparkConfiguration, job_name: str, strategy: str):
        self.spark_config: SparkConf = self._spark_config_parser(conf, strategy)

        self._spark: SparkSession = self._gen_spark_builder(
            spark_config=self.spark_config,
            job_name=job_name,
            strategy=strategy
        )

    def _spark_config_parser(self,
                             config: SparkConfiguration,
                             strategy: str) -> SparkConf:
        if strategy == "delta":
            config = config | self._delta_config

        # Create a list of tuples to parse with SparkConf
        spark_config_tuple: list[tuple[str, str]] = [
            (key, value)
            for key, value in config.items()
        ]

        spark_config = SparkConf().setAll(spark_config_tuple)

        return spark_config

    @staticmethod
    def _gen_spark_builder(spark_config: SparkConf,
                           job_name: str,
                           strategy: str):
        # Initialize Spark session
        builder = (
            SparkSession.builder
            .appName(job_name)
            .config(conf=spark_config)
        )

        # Determine the builder
        if strategy == Strategy.VANILLA.value:
            return builder.getOrCreate()
        elif strategy == Strategy.HIVE:
            return builder.enableHiveSupport().getOrCreate()
        else:
            raise ValueError(f"Unsupported strategy `{strategy}`")

    @property
    def spark(self) -> SparkSession:
        """Expose the Spark session as a read-only property."""
        return self._spark

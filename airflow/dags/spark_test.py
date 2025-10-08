import pendulum

from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "retries": 0,
    },
    tags=["example"],
)
def spark_test():

    read_data = SparkSubmitOperator(
        task_id="spark_data_test",
        application="/opt/code/patterns/spark_test/spark_test.py",
        conn_id="spark_connection",
        verbose=True,
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        },
        packages="io.delta:delta-spark_2.12:3.3.2"
    )

    read_data


spark_test()

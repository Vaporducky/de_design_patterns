from pathlib import Path

import pendulum

from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator
)
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


CODE_PATH = Path("/opt/code/patterns/idempotent/fast_metadata_cleaner")


def analyze_date(**context):
    # Normal use would do something like this
    # dt = pendulum.now().date()

    dt = pendulum.from_format(context["params"]["date"], "YYYY-MM-DD").date()

    # Check if the week is starting or if it's New Year
    if dt.day_of_week == "Monday" or (dt.month == 1 and dt.day == 1):
        return "create_partition_table"
    else:
        return "process_daily_data"


def process_daily_data():
    pass


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "retries": 0,
    },
    params={
        "date": "2025-01-01"
    },
    tags=["de", "design-patterns", "idempotency"],
)
def fast_metadata_cleaner():
    analyze_date_branch = BranchPythonOperator(
        task_id="analyze_date",
        python_callable=analyze_date
    )

    create_partition_table = SparkSubmitOperator(
        task_id="create_partition_table",
        application=(CODE_PATH / "create_partition_table.py").as_posix(),
        conn_id="spark_connection",
        verbose=False,
        packages="io.delta:delta-spark_2.12:3.3.2",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.warehouse.dir": "/tmp/spark-data/warehouse",
        }
    )

    process_daily_data_task = PythonOperator(
        task_id="process_daily_data",
        python_callable=process_daily_data
    )

    analyze_date_branch >> [create_partition_table, process_daily_data_task]


fast_metadata_cleaner()
# CONFIGURE CLI
# airflow dags trigger manual_trigger_only \
#     --conf '{"my_param": "value1", "another_param": 42}'

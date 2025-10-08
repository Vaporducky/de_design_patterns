"""`data_overwrite` DAG

Runs an idempotent Spark job (Delta-based overwrite) with production-grade
configuration: centralized config, retries, failure callback, timezone-aware
start_date, safe templating, and a small wrapper factory to keep DAGs DRY.

Example configuration:
```json
{
  "job_name": "default_job",
  "table": "user_activity",
  "schema": "idempotent",
  "partitions": "",
  "scenario": 0,
  "backfill": false
}
```
"""
import json
import logging
import shutil
from pathlib import Path
from typing import Any

import pendulum
from airflow.sdk import dag
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.email import send_email

# Use Airflow Variables (or Secrets backend) for environment-specific values.
# airflow variables import /path/to/variables.json
DELTA_PACKAGE = Variable.get("delta_package",
                             default_var="io.delta:delta-spark_2.12:3.3.2")
SPARK_CONN_ID = Variable.get("spark_connection_id",
                             default_var="spark_connection")
SPARK_JAR_HOME = Variable.get("spark_jar_home",
                              default_var="/tmp/jars")
# Path Configuration
DATA_MAP = json.loads(Variable.get("data", default_var=json.dumps({})))
SPARK_WAREHOUSE_PATH = DATA_MAP["spark_warehouse"]
CODE_MAP = json.loads(Variable.get("code", json.dumps({})))
PIPELINES_PATH = Path(CODE_MAP["pipelines_path"])

PATTERN = "idempotent"
USE_CASE = "data_overwrite"
DATA_PATH = DATA_MAP["base_data"]

# Module Level Constants
CODE_PATH: Path = Path(f"{PIPELINES_PATH}/{PATTERN}/{USE_CASE}")

# Runtime defaults
DEFAULT_ARGS = {
    "retries": 1,
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(seconds=10),
    "retry_delay": pendulum.duration(seconds=5),
}

SPARK_DELTA_CONFIG = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.warehouse.dir": SPARK_WAREHOUSE_PATH,
}

SPARK_ENV = {
    "SPARK_WAREHOUSE": SPARK_WAREHOUSE_PATH,
    "DATA_PATH": DATA_PATH,
}


def _on_failure_callback(context: dict[str, Any]) -> None:
    """Minimal failure handler: logs and sends an optional email.

    This keeps observable failures and provides an integration point for
    PagerDuty/Slack.
    """
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance")
    msg = (
        f"DAG {dag_run.dag_id} run {dag_run.run_id} failed on task "
        f"{task_instance.task_id} (try {task_instance.try_number})"
    )
    logging.error(msg)
    # Optional: send email (requires SMTP configured in Airflow)
    try:
        send_email(to="data-platform-alerts@example.com",
                   subject="Airflow DAG failed",
                   html_content=msg)
    except Exception as e:
        logging.exception("Failed sending failure notification email",
                          exc_info=e)


@dag(
    dag_id="data_overwrite_processor",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "job_name": "",
        "scenario": 99,
        "table": None,
        "schema": None,
        "partitions": "",
        "backfill": False
    },
    tags=["de", "design-patterns", "idempotency"],
    doc_md=__doc__,
    on_failure_callback=_on_failure_callback
)
def data_overwrite():
    """
    Orchestrates a single Spark job that performs an idempotent overwrite.

    Template usage:
      - Prefer dag_run.conf.get('table', params.table) to avoid KeyError
      - All runtime vars should be validated inside the Spark app as well.
    """

    def _build_spark_args() -> list[str]:
        """
        Return Spark job application arguments with Airflow Jinja templating.
        """
        return [
            "-j", "{{ params.job_name }}",
            "-t", "{{ params.table }}",
            "-s", "{{ params.schema }}",
            "-p", "{{ params.partitions }}",
            "--scenario", "{{ params.scenario }}",
            "--backfill", "{{ params.backfill }}",
        ]

    def _create_build_pyfiles(**context):
        job_name = context["params"]["job_name"]
        # Define outputs
        base_name: Path = CODE_PATH / "build" / f"{job_name}_src"
        root_dir: Path = CODE_PATH / "src"

        if base_name.exists() and base_name.is_file():
            base_name.unlink()
            logging.info(f"Cleaned PyFiles `{base_name.as_posix()}`.")
            logging.info(f"Ready for recreation.")

        logging.info(f"Creating PyFiles `{base_name.as_posix()}`.")
        # Create a PyFiles for Spark application
        shutil.make_archive(base_name=base_name.as_posix(),
                            format="zip",
                            root_dir=root_dir.as_posix())
        logging.info(f"PyFiles created.")

    create_build_pyfiles = PythonOperator(
        task_id="create_build_pyfiles",
        python_callable=_create_build_pyfiles
    )

    spark_job = SparkSubmitOperator(
        task_id="data_overwrite",
        application=(CODE_PATH / "data_overwrite_entrypoint.py").as_posix(),
        conn_id=SPARK_CONN_ID,
        packages=DELTA_PACKAGE,
        py_files=(CODE_PATH / "build" / "data_overwrite_src.zip").as_posix(),
        application_args=_build_spark_args(),
        env_vars=SPARK_ENV,
        conf=SPARK_DELTA_CONFIG,
        verbose=False
    )

    create_build_pyfiles >> spark_job


data_overwrite()

# {
#     "job_name": "data_overwrite",
#     "table": "user_activity",
#     "schema": "idempotent",
#     "scenario": 0
# }

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = "/home/shreya/gcp-ecommerce-data-engineering-pipeline"
PYTHON = f"{PROJECT_ROOT}/venv/bin/python"


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="ecommerce_incremental_pipeline",
    description="Incremental e-commerce data pipeline using Python and PySpark",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "pyspark", "incremental", "data-engineering"],
) as dag:

    create_incremental_batch = BashOperator(
        task_id="create_incremental_batch",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{PYTHON} ingestion/create_incremental_batch.py "
            "{{ dag_run.conf['batch_date'] }}"
        ),
    )

    incremental_orders_silver = BashOperator(
        task_id="incremental_orders_silver",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{PYTHON} ingestion/incremental_orders_silver.py "
            "{{ dag_run.conf['batch_date'] }}"
        ),
    )

    incremental_fact_order_item_gold = BashOperator(
        task_id="incremental_fact_order_item_gold",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{PYTHON} spark_jobs/incremental_fact_order_item_gold.py "
            "{{ dag_run.conf['batch_date'] }}"
        ),
    )

    incremental_sales_daily_gold = BashOperator(
        task_id="incremental_sales_daily_gold",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"{PYTHON} spark_jobs/incremental_sales_daily_gold.py "
            "{{ dag_run.conf['batch_date'] }}"
        ),
    )

    (
        create_incremental_batch
        >> incremental_orders_silver
        >> incremental_fact_order_item_gold
        >> incremental_sales_daily_gold
    )
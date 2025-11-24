from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "luan",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="Pipeline ETL Ecommerce - Bronze > Silver > Gold > Load DB",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    concurrency=1,
) as dag:

    # ----------------------
    # BRONZE LAYER
    # ----------------------
    bronze_task = BashOperator(
        task_id="bronze_layer",
        bash_command="python /opt/airflow/dags/01_bronze_layer.py",
    )

    # ----------------------
    # SILVER LAYER
    # ----------------------
    silver_task = BashOperator(
        task_id="silver_layer",
        bash_command="python /opt/airflow/dags/02_silver_layer.py",
    )

    # ----------------------
    # GOLD LAYER
    # ----------------------
    gold_task = BashOperator(
        task_id="gold_layer",
        bash_command="python /opt/airflow/dags/03_gold_layer.py",
    )

    # ----------------------
    # LOAD INTO DATABASE
    # ----------------------
    load_db_task = BashOperator(
        task_id="load_database",
        bash_command="python /opt/airflow/dags/04_load_database.py",
    )

    # ORDEM
    bronze_task >> silver_task >> gold_task >> load_db_task

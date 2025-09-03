from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from exporters.all_bets_exporter import run_export

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bets_exporter",
    default_args=default_args,
    start_date=datetime(2025, 8, 27),
    schedule_interval="*/5 * * * *",  # каждые 5 минут
    catchup=False,
) as dag:
    export_task = PythonOperator(
        task_id="export_bets_to_s3",
        python_callable=run_export
    )

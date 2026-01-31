from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from scripts.weather_pipeline import fetch_and_load

default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="weather_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 30),
    catchup=False,
) as dag:

    weather_task = PythonOperator(
        task_id="fetch_weather_and_load",
        python_callable=fetch_and_load
    )

    weather_task
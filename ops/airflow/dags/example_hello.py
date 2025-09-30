from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="example_hello",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["check"],
):
    EmptyOperator(task_id="start")

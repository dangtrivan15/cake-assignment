from cake_airflow_custom_package.utils import test
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
        dag_id="test_dag",
        start_date=datetime(2021, 1, 1),
        schedule=None
):
    PythonOperator(
        task_id="test_task",
        python_callable=test,
    )

from airflow import DAG
from airflow.operators.python import PythonOperator
from cake_airflow_custom_package.api import Pipeline
from datetime import datetime
from cake_airflow_custom_package.sftp import SFTPSource, SFTPDest, ReplacingWithATransformer
from cake_airflow_custom_package.pg_state_machine import PostgresStateMachine
from os import getenv

with DAG(
        dag_id="test_dag",
        start_date=datetime(2021, 1, 1),
        schedule=None
):
    source = SFTPSource(
        connection_id="sftp_source",
        dir_path=getenv("SFTP_SOURCE_DIR_PATH")
    )
    destination = SFTPDest(
        connection_id="sftp_dest",
        dir_path=getenv("SFTP_DEST_DIR_PATH")
    )
    state_machine = PostgresStateMachine(
        connection_id=source.connection_id,
        uri=getenv("PG_URI")
    )
    transformer = ReplacingWithATransformer()
    pipeline = Pipeline(
        source=source,
        destination=destination,
        state_machine=state_machine,
        transformer=transformer
    )
    PythonOperator(
        task_id="cake_health_check",
        python_callable=pipeline.health_check,
    ) >> PythonOperator(
        task_id="sync",
        python_callable=pipeline.sync,
    )

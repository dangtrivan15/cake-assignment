from airflow import DAG
from airflow.operators.python import PythonOperator
from cake_airflow_custom_package.api import Pipeline
from datetime import datetime
from cake_airflow_custom_package.sftp import SFTPSource, SFTPDest, ReplacingSpaceWithUnderScoreTransformer
from cake_airflow_custom_package.pg_state_machine import PostgresStateMachine
from os import getenv

with DAG(
        dag_id="cake_sftp_pipeline",
        start_date=datetime(2021, 1, 1),
        schedule=None
) as d:
    source = SFTPSource(
        connection_id="sftp_source",
        dir_path=getenv("SFTP_SOURCE_DIR_PATH")
    )
    destination = SFTPDest(
        connection_id="sftp_dest",
        dir_path=getenv("SFTP_DEST_DIR_PATH")
    )
    state_machine = PostgresStateMachine(
        pipeline_id=d.dag_id,
        uri=getenv("PG_URI")
    )
    transformer = ReplacingSpaceWithUnderScoreTransformer()
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

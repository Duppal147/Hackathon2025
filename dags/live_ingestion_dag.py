import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ingestion.live_ingestion import main as live_ingestion  
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="live_ingestion",
    default_args=default_args,
    schedule_interval="*/5 * * * *", 
    start_date=datetime(2025, 9, 10),
    catchup=False,
)

run_live_ingestion = PythonOperator(
    task_id="run_live_ingestion",
    python_callable=live_ingestion,
    dag=dag,
)

trigger_dbt = TriggerDagRunOperator(
    task_id='trigger_dbt_dag',
    trigger_dag_id='dbt_snowflake_dag',
    reset_dag_run=True,
    wait_for_completion=False,
    dag=dag,
)

run_live_ingestion >> trigger_dbt
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from reg_models.time_log_regression import run_regression_model  

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="regression_model_dag",
    default_args=default_args,
    schedule_interval=None, 
    start_date=datetime(2025, 9, 10),
    catchup=False,
)

wait_for_dbt = ExternalTaskSensor(
    task_id="wait_for_dbt",
    external_dag_id="dbt_snowflake_dag",   
    external_task_id="end_task",            
    timeout=600,
    poke_interval=60,
    mode="reschedule",
    dag=dag,
)

run_regression = PythonOperator(
    task_id="run_regression_model",
    python_callable=run_regression_model,
    dag=dag,
)


wait_for_dbt >> run_regression

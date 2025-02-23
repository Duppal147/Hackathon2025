import os
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator



profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={
            "database": "maddata_db",
            "schema": "md_schema"
        },
    )
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig("/usr/local/airflow/dags/dbt/data_pipeline"),
    operator_args={"install_deps": True,
                    "vars": '{"live_mode": true}'},
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    schedule_interval="@daily",
    start_date=datetime(2025, 9, 10),
    catchup=False,
    dag_id="dbt_snowflake_dag",
)

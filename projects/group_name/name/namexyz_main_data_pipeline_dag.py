# projects/group_name/name/namexyz_main_data_pipeline_dag.py

import json
import os
import sys
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Add pipeline_framework to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))

from pipeline_framework.configs_handler_func import get_final_config
from pipeline_framework.task_handlers import (
    record_generator_task,
    pick_pending_record_task,
    validate_record_task,
    source_to_stage_task,
    stage_to_target_task,
    audit_task,
    cleanup_stale_locks_task
)

# Load config
root_project_path = os.path.join(os.path.dirname(__file__), '../../..')
drive_defaults_relative_path = "pipeline_logic/config/drive_table_defaults.json"
index_config_relative_path = "projects/group_name/name/name.json"

final_config = get_final_config(
    root_project_path=root_project_path,
    drive_defaults_relative_path=drive_defaults_relative_path,
    index_config_relative_path=index_config_relative_path
)

# Email settings from config
email_recipients = final_config.get("email_recipients", ["alerts@company.com"])

# DAG configuration
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz=final_config.get("timezone", "UTC")),
    'email_on_failure': final_config.get("email_on_failure", True),
    'email_on_retry': final_config.get("email_on_retry", False),
    'email': email_recipients,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id="namexyz_main_data_pipeline",
    default_args=default_args,
    description='Ingestion process ES→S3→SF ',
    schedule_interval="0 * * * *",  # Every hour
    catchup=False,
    max_active_runs=1,
    params={'final_config': final_config}
)

# Tasks
record_gen = PythonOperator(task_id='record_generator_task', python_callable=record_generator_task, dag=dag)
pick_record = PythonOperator(task_id='pick_pending_record_task', python_callable=pick_pending_record_task, dag=dag)
# validate = PythonOperator(task_id='validate_record_task', python_callable=validate_record_task, dag=dag)
source_stage = PythonOperator(task_id='source_to_stage_task', python_callable=source_to_stage_task, dag=dag)
stage_target = PythonOperator(task_id='stage_to_target_task', python_callable=stage_to_target_task, dag=dag)
audit_data = PythonOperator(task_id='audit_task', python_callable=audit_task, dag=dag)
cleanup = PythonOperator(task_id='cleanup_stale_locks_task', python_callable=cleanup_stale_locks_task, trigger_rule='all_done', dag=dag)

# Dependencies
record_gen >> pick_record >> source_stage >> stage_target >> audit_data >> cleanup
# pipeline_framework/task_handlers.py

from airflow.exceptions import AirflowSkipException
from pipeline_framework import source_to_stage, stage_to_target, audit
from pipeline_framework.record_generator import record_generator, validate_record  
from pipeline_framework.drive_record_adapter import update_record_fields
from pipeline_framework.utils.time_utility import get_current_time_iso
from pipeline_framework.utils.log_generator import setup_pipeline_logger
from pipeline_framework.drive_record_adapter import cleanup_stale_locks
from pipeline_framework.drive_record_adapter import get_oldest_pending_record
from pipeline_framework.utils.time_utils import get_current_time_iso

log = setup_pipeline_logger(logger_name="TaskHandlers")

def record_generator_task(**context):
    """Generate one record per DAG run"""
    final_config = context['params']['final_config']
    count = record_generator(final_config, **context)
    return count

def validate_record_task(**context):
    """Validate generated record for future data and processing status"""
    final_config = context['params']['final_config']
    record = validate_record(final_config, **context)
    return record

def source_to_stage_task(**context):
    """Lock record and execute source to stage transfer"""
    final_config = context['params']['final_config']
    record = context['task_instance'].xcom_pull(task_ids='validate_record_task')
    dag_run_id = context['dag_run'].dag_id + "_" + context['dag_run'].run_id
    
    # Lock record (Airflow-specific)
    update_record_fields(record['pipeline_id'], {
        'dag_run_id': dag_run_id,
        'pipeline_start_time': get_current_time_iso(final_config['timezone']),
        'pipeline_status': 'IN_PROGRESS'
    }, final_config)
    
    # Execute transfer
    result = source_to_stage.transfer(final_config, record)
    context['task_instance'].xcom_push(key='record', value=record)
    return result

def stage_to_target_task(**context):
    """Execute stage to target transfer"""
    final_config = context['params']['final_config']
    record = context['task_instance'].xcom_pull(task_ids='source_to_stage_task', key='record')
    
    result = stage_to_target.transfer(final_config, record)
    context['task_instance'].xcom_push(key='record', value=record)
    return result

def audit_task(**context):
    """Execute audit validation"""
    final_config = context['params']['final_config']
    record = context['task_instance'].xcom_pull(task_ids='stage_to_target_task', key='record')
    
    result = audit.audit(final_config, record)
    return result

def cleanup_stale_locks_task(**context):
    """Cleanup stale locks implementation"""
    final_config = context['params']['final_config']
    
    stale_hours = final_config.get('stale_lock_hours', 2)
    count = cleanup_stale_locks(final_config, stale_hours)
    
    log.info(f"Cleaned {count} stale locks", status="COMPLETED")
    return count



def pick_pending_record_task(**context):
    """Pick the oldest valid PENDING record. Skip if none or if record is for the future."""
    from pipeline_framework.drive_record_adapter import get_oldest_pending_record
    from airflow.exceptions import AirflowSkipException
    from pipeline_framework.utils.time_utility import get_current_time_iso, compare_times

    final_config = context['params']['final_config']
    priority = final_config.get("pipeline_priority", "xyz")

    # Step 1: Find oldest pending record
    record = get_oldest_pending_record(final_config, priority)

    if not record:
        log.info(" No pending record found", log_key="PickPendingRecord", status="SKIPPED")
        raise AirflowSkipException("No pending record found")

    # Step 2: Check if record is from the future
    current_time_iso = get_current_time_iso(final_config["timezone"])
    if compare_times(record['window_start_time'], current_time_iso) > 0:
        log.info(
            "⏳ Record window_start_time is in the future — skipping",
            log_key="PickPendingRecord",
            status="FUTURE_SKIPPED",
            record_start=record['window_start_time'],
            current=current_time_iso
        )
        raise AirflowSkipException("Record is not yet eligible (future window_start_time)")

    # ✅ Valid record — proceed
    log.info("✅ Valid pending record selected", log_key="PickPendingRecord", status="SELECTED", pipeline_id=record['pipeline_id'])
    context['task_instance'].xcom_push(key='validated_record', value=record)
    return record






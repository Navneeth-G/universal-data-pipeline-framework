# pipeline_framework/audit_operations.py

import time
from typing import Dict, Any, Tuple
from pipeline_framework.source import count as source_count
from pipeline_framework.target import count as target_count, delete as target_delete
from pipeline_framework.stage import delete as stage_delete
from pipeline_framework.utils.log_generator import setup_pipeline_logger
from pipeline_framework.utils.time_utils import get_current_time_iso
from pipeline_framework.drive_record_adapter import update_record_in_drive_table


log = setup_pipeline_logger(logger_name="AuditOps")





def reset_record_for_retry(record: Dict[str, Any]) -> None:
    """Reset all status and timing fields for retry"""
    # Increment retry attempt
    record['retry_attempt'] = record.get('retry_attempt', 0) + 1
    
    # Reset pipeline level
    record['pipeline_status'] = 'PENDING'
    record['pipeline_start_time'] = None
    record['pipeline_end_time'] = None
    record['dag_run_id'] = None
    
    # Reset source to stage
    record['source_to_stage_ingestion_status'] = 'PENDING'
    record['source_to_stage_ingestion_start_time'] = None
    record['source_to_stage_ingestion_end_time'] = None
    
    # Reset stage to target
    record['stage_to_target_ingestion_status'] = 'PENDING'
    record['stage_to_target_ingestion_start_time'] = None
    record['stage_to_target_ingestion_end_time'] = None
    
    # Reset audit
    record['audit_status'] = 'PENDING'
    record['audit_start_time'] = None
    record['audit_end_time'] = None
    record['audit_result'] = None
    
    # Reset progress tracking
    record['completed_phase'] = None



def audit_data_transfer(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Main audit function with adaptive waiting for Snowflake data loading"""
    try:
        if record.get('audit_status') == 'COMPLETED' and record.get('audit_result') == 'MATCH':
            log.info("Audit already completed - skipping", pipeline_id=record['pipeline_id'])
            return True
            
        timezone = final_config.get('timezone', 'UTC')
        record['audit_start_time'] = get_current_time_iso(timezone)
        
        log.info("Starting audit validation with adaptive waiting", pipeline_id=record['pipeline_id'])
        
        source_count, target_count, success = wait_for_target_data_load(final_config, record)
        # Update record with counts
        count_difference = target_count - source_count
        if source_count == 0:
            percentage_difference = 0
        else:
            percentage_difference = (count_difference / source_count) * 100 if source_count else 0
        if success:
            # Success - mark completed (keep dag_run_id to show which DAG completed it)
            record.update({
                'audit_end_time': get_current_time_iso(timezone),
                'audit_status': 'COMPLETED',
                'audit_result': 'MATCH',
                'pipeline_status': 'SUCCESS',
                'pipeline_end_time': get_current_time_iso(timezone),
                'completed_phase': 'AUDIT',
                'source_count': source_count,
                'target_count': target_count,
                'count_difference': count_difference,
                'percentage_difference': percentage_difference
            })

            # Keep dag_run_id for successful completion tracking
            
            update_record_in_drive_table(record, final_config)
            
            log.info("Audit passed - pipeline completed successfully", pipeline_id=record['pipeline_id'])
            return True
        else:
            # Mismatch or timeout - cleanup and retry

            record.update({
                'pipeline_status': 'PENDING',
                'pipeline_start_time': None,
                'pipeline_end_time': None,
                'dag_run_id': None,
                'retry_attempt': record.get('retry_attempt', 0) + 1,
                'completed_phase': None,
                'audit_start_time': None,
                'audit_end_time': None,
                'source_to_stage_ingestion_status': 'PENDING',
                'source_to_stage_ingestion_start_time': None,
                'source_to_stage_ingestion_end_time': None,
                'stage_to_target_ingestion_status': 'PENDING',
                'stage_to_target_ingestion_start_time': None,
                'stage_to_target_ingestion_end_time': None,
                'audit_status': 'PENDING',
                'audit_result': None,
                'audit_start_time': None,
                'audit_end_time': None,
                'record_last_updated_time': get_current_time_iso(final_config['timezone']),
                'source_count': None,
                'target_count': None,
                'count_difference': None,
                'percentage_difference': None
            })
            update_record_in_drive_table(record, final_config)
            
            log.error(
                f"Audit failed - count mismatch or timeout",
                pipeline_id=record['pipeline_id'],
                source_count=source_count,
                target_count=target_count
            )
            
            # Cleanup stage and target
            cleanup_on_audit_failure(final_config, record)
            return False
            
    except Exception as e:
        log.exception("Audit validation failed", pipeline_id=record['pipeline_id'])
        record['audit_status'] = 'ERROR'
        record['audit_result'] = 'ERROR'
        update_record_in_drive_table(record, final_config)
        raise




def wait_for_target_data_load(final_config: Dict[str, Any], record: Dict[str, Any], max_wait_minutes: int = 10) -> Tuple[int, int, bool]:
    """Wait for Snowflake data loading with adaptive checks"""
    max_attempts = max_wait_minutes
    wait_seconds = 60

    for attempt in range(1, max_attempts + 1):
        log.info(f"Checking counts (attempt {attempt}/{max_attempts})", pipeline_id=record['pipeline_id'])

        source_count_val = source_count(final_config, record)
        target_count_val = target_count(final_config, record)

        log.info(
            f"Count check: Source={source_count_val}, Target={target_count_val}",
            pipeline_id=record['pipeline_id'],
            attempt=attempt
        )

        if source_count_val == target_count_val:
            log.info("Counts match - data load complete", pipeline_id=record['pipeline_id'])
            return source_count_val, target_count_val, True

        if target_count_val > source_count_val:
            log.warning("Target > Source - data integrity issue", pipeline_id=record['pipeline_id'])
            return source_count_val, target_count_val, False

        # target_count < source_count
        if attempt < max_attempts:
            log.info(f"Target loading in progress - waiting {wait_seconds}s", pipeline_id=record['pipeline_id'])
            time.sleep(wait_seconds)

    log.error(f"Data load timeout after {max_wait_minutes} minutes", pipeline_id=record['pipeline_id'])
    return source_count_val, target_count_val, False



def cleanup_on_audit_failure(final_config: Dict[str, Any], record: Dict[str, Any]) -> None:
    """Clean up stage and target data on audit failure"""
    try:
        log.info("Cleaning up stage and target data due to audit failure", pipeline_id=record['pipeline_id'])

        stage_delete(final_config, record)
        target_delete(final_config, record)

        log.info("Cleanup completed successfully", pipeline_id=record['pipeline_id'])

        # Reset record for retry (includes incrementing retry_attempt and releasing lock)
        reset_record_for_retry(record)

        log.info(f"Record reset for retry - attempt #{record['retry_attempt']}", pipeline_id=record['pipeline_id'])

        # Update record after cleanup and reset
        update_record_in_drive_table(record, final_config)

    except Exception as e:
        log.exception("Cleanup failed during audit failure", pipeline_id=record['pipeline_id'])
        # Still try to reset and update record
        try:
            reset_record_for_retry(record)
            update_record_in_drive_table(record, final_config)
        except:
            log.exception("Failed to update record after cleanup failure", pipeline_id=record['pipeline_id'])








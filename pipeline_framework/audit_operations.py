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
    record['RETRY_ATTEMPT'] = record.get('RETRY_ATTEMPT', 0) + 1
    
    # Reset pipeline level
    record['PIPELINE_STATUS'] = 'PENDING'
    record['PIPELINE_START_TIME'] = None
    record['PIPELINE_END_TIME'] = None
    record['DAG_RUN_ID'] = None
    
    # Reset source to stage
    record['SOURCE_TO_STAGE_INGESTION_STATUS'] = 'PENDING'
    record['STAGE_TO_STAGE_INGESTION_START_TIME'] = None
    record['STAGE_TO_TARGET_INGESTION_END_TIME'] = None
    
    # Reset stage to target
    record['STAGE_TO_TARGET_INGESTION_STATUS'] = 'PENDING'
    record['STAGE_TO_TARGET_INGESTION_START_TIME'] = None
    record['STAGE_TO_TARGET_INGESTION_END_TIME'] = None

    # Reset audit
    record['AUDIT_STATUS'] = 'PENDING'
    record['AUDIT_START_TIME'] = None
    record['AUDIT_END_TIME'] = None
    record['AUDIT_RESULT'] = None
    
    # Reset progress tracking
    record['COMPLETED_PHASE'] = None



def audit_data_transfer(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Main audit function with adaptive waiting for Snowflake data loading"""
    try:
        if record.get('AUDIT_STATUS') == 'COMPLETED' and record.get('AUDIT_RESULT') == 'MATCH':
            log.info("Audit already completed - skipping", PIPELINE_ID=record['PIPELINE_ID'])
            return True
            
        timezone = final_config.get('timezone', 'UTC')
        record['AUDIT_START_TIME'] = get_current_time_iso(timezone)

        log.info("Starting audit validation with adaptive waiting", PIPELINE_ID=record['PIPELINE_ID'])

        source_count, target_count, success = wait_for_target_data_load(final_config, record)
        # Update record with counts
        count_difference = target_count - source_count
        if source_count == 0:
            percentage_difference = 0
        else:
            percentage_difference = (count_difference / source_count) * 100 if source_count else 0
        if success:
            # Success - mark completed (keep DAG_RUN_ID to show which DAG completed it)
            record.update({
                'AUDIT_END_TIME': get_current_time_iso(timezone),
                'AUDIT_STATUS': 'COMPLETED',
                'AUDIT_RESULT': 'MATCH',
                'PIPELINE_STATUS': 'SUCCESS',
                'PIPELINE_END_TIME': get_current_time_iso(timezone),
                'COMPLETED_PHASE': 'AUDIT',
                'SOURCE_COUNT': source_count,
                'TARGET_COUNT': target_count,
                'COUNT_DIFFERENCE': count_difference,
                'PERCENTAGE_DIFFERENCE': percentage_difference
            })

            # Keep DAG_RUN_ID for successful completion tracking
            
            update_record_in_drive_table(record, final_config)
            
            log.info("Audit passed - pipeline completed successfully", PIPELINE_ID=record['PIPELINE_ID'])
            return True
        else:
            # Mismatch or timeout - cleanup and retry

            record.update({
                'PIPELINE_STATUS': 'PENDING',
                'PIPELINE_START_TIME': None,
                'PIPELINE_END_TIME': None,
                'DAG_RUN_ID': None,
                'RETRY_ATTEMPT': record.get('RETRY_ATTEMPT', 0) + 1,
                'COMPLETED_PHASE': None,
                'AUDIT_STATUS': 'PENDING',                
                'AUDIT_START_TIME': None,
                'AUDIT_END_TIME': None,
                'AUDIT_RESULT': None,                
                'SOURCE_TO_STAGE_INGESTION_STATUS': 'PENDING',
                'SOURCE_TO_STAGE_INGESTION_START_TIME': None,
                'SOURCE_TO_STAGE_INGESTION_END_TIME': None,
                'STAGE_TO_TARGET_INGESTION_STATUS': 'PENDING',
                'STAGE_TO_TARGET_INGESTION_START_TIME': None,
                'STAGE_TO_TARGET_INGESTION_END_TIME': None,
                'RECORD_LAST_UPDATED_TIME': get_current_time_iso(final_config['timezone']),
                'SOURCE_COUNT': None,
                'TARGET_COUNT': None,
                'COUNT_DIFFERENCE': None,
                'PERCENTAGE_DIFFERENCE': None
            })
            update_record_in_drive_table(record, final_config)
            
            log.error(
                f"Audit failed - count mismatch or timeout",
                PIPELINE_ID=record['PIPELINE_ID'],
                source_count=source_count,
                target_count=target_count
            )
            
            # Cleanup stage and target
            cleanup_on_audit_failure(final_config, record)
            return False
            
    except Exception as e:
        log.exception("Audit validation failed", PIPELINE_ID=record['PIPELINE_ID'])
        record['AUDIT_STATUS'] = 'ERROR'
        record['AUDIT_RESULT'] = 'ERROR'
        update_record_in_drive_table(record, final_config)
        raise


def wait_for_target_data_load(final_config: Dict[str, Any], record: Dict[str, Any], max_wait_minutes: int = 5) -> Tuple[int, int, bool]:
    """Wait for Snowflake data loading with adaptive checks and stagnation detection"""
    wait_seconds = 60  # seconds
    max_attempts = int((max_wait_minutes * 60) / wait_seconds)
    prev_target_count = -1
    source_count_val = source_count(final_config, record)
    
    for attempt in range(1, max_attempts + 1):
        log.info(f"Checking counts (attempt {attempt}/{max_attempts})", PIPELINE_ID=record['PIPELINE_ID'])


        target_count_val = target_count(final_config, record)

        log.info(
            f"Count check: Source={source_count_val}, Target={target_count_val}",
            PIPELINE_ID=record['PIPELINE_ID'],
            attempt=attempt
        )

        if source_count_val == target_count_val:
            log.info("Counts match - data load complete", PIPELINE_ID=record['PIPELINE_ID'])
            return source_count_val, target_count_val, True

        if target_count_val > source_count_val:
            log.warning("Target > Source - data integrity issue", PIPELINE_ID=record['PIPELINE_ID'])
            return source_count_val, target_count_val, False

        if target_count_val == prev_target_count:
            log.warning("Target count not increasing - possible stall detected", PIPELINE_ID=record['PIPELINE_ID'])
            return source_count_val, target_count_val, False

        prev_target_count = target_count_val

        if attempt < max_attempts:
            log.info(f"Target loading in progress - waiting {wait_seconds}s", PIPELINE_ID=record['PIPELINE_ID'])
            time.sleep(wait_seconds)

    log.error(f"Data load timeout after {max_wait_minutes} minutes", PIPELINE_ID=record['PIPELINE_ID'])
    return source_count_val, target_count_val, False



def cleanup_on_audit_failure(final_config: Dict[str, Any], record: Dict[str, Any]) -> None:
    """Clean up stage and target data on audit failure"""
    try:
        log.info("Cleaning up stage and target data due to audit failure", PIPELINE_ID=record['PIPELINE_ID'])

        stage_delete(final_config, record)
        target_delete(final_config, record)

        log.info("Cleanup completed successfully", PIPELINE_ID=record['PIPELINE_ID'])

        # Reset record for retry (includes incrementing RETRY_ATTEMPT and releasing lock)
        reset_record_for_retry(record)

        log.info(f"Record reset for retry - attempt #{record['RETRY_ATTEMPT']}", PIPELINE_ID=record['PIPELINE_ID'])

        # Update record after cleanup and reset
        update_record_in_drive_table(record, final_config)

    except Exception as e:
        log.exception("Cleanup failed during audit failure", PIPELINE_ID=record['PIPELINE_ID'])
        # Still try to reset and update record
        try:
            reset_record_for_retry(record)
            update_record_in_drive_table(record, final_config)
        except:
            log.exception("Failed to update record after cleanup failure", PIPELINE_ID=record['PIPELINE_ID'])








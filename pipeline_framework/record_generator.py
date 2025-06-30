# pipeline_framework/records_generation/record_generator.py

"""
RECORD GENERATOR FLOW:
1. Parse x_time_back and granularity from final_config
2. Calculate TARGET_DAY = (now - x_time_back).date()
3. Check if existing records exist for TARGET_DAY
4. If none: start_time = start_of_TARGET_DAY + granularity
5. If exist: start_time = max(end_time) from existing records
6. Calculate end_time = start_time + granularity
7. Apply boundary check: cap end_time at TARGET_DAY_end (next day 00:00)
8. If start_time >= TARGET_DAY_end: return 0 (no record created)
9. Create record from drive_table_default_record template
10. Update with time fields and generate unique IDs
11. Insert record and return 1
"""

import re
import pendulum
import hashlib
import pandas as pd
from pipeline_framework.utils.log_generator import setup_pipeline_logger
from pipeline_framework.utils.time_utility import (
    get_current_time_iso,
    to_iso_string,
    add_duration_to_iso,
    get_start_of_day_iso,
    get_end_of_day_iso,
    get_date_only,
    calculate_duration_seconds,
    compare_times
)

from pipeline_framework.source import count as source_count
from pipeline_framework.target import count as target_count

from airflow.exceptions import AirflowSkipException
from pipeline_framework.drive_record_adapter import get_existing_drive_records, insert_drive_record

# Setup logger for this module
log = setup_pipeline_logger(logger_name="RecordGenerator")


def parse_time_string(time_str):
    """Parse time strings like '1d2h30m', '1w', '30s' into total seconds"""
    if not time_str:
        return 0
    
    pattern = r'(\d+)([wdhms])'
    matches = re.findall(pattern, time_str.lower())
    
    total_seconds = 0
    for value, unit in matches:
        value = int(value)
        if unit == 'w':
            total_seconds += value * 7 * 24 * 3600
        elif unit == 'd':
            total_seconds += value * 24 * 3600
        elif unit == 'h':
            total_seconds += value * 3600
        elif unit == 'm':
            total_seconds += value * 60
        elif unit == 's':
            total_seconds += value
    
    return total_seconds


def seconds_to_time_string(total_seconds):
    """Convert seconds back to time string format like '1d2h30m'"""
    if total_seconds == 0:
        return "0s"
    
    days = total_seconds // (24 * 3600)
    total_seconds %= (24 * 3600)
    hours = total_seconds // 3600
    total_seconds %= 3600
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    
    parts = []
    if days > 0:
        parts.append(f"{int(days)}d")
    if hours > 0:
        parts.append(f"{int(hours)}h")
    if minutes > 0:
        parts.append(f"{int(minutes)}m")
    if seconds > 0:
        parts.append(f"{int(seconds)}s")
    
    return "".join(parts)


def create_base_record(final_config):
    """Create base record dict from drive_table_default_record template in final_config"""
    record = final_config["drive_table_default_record"].copy()
    return record


def update_time_fields(record, start_time_iso, end_time_iso, TARGET_DAY, final_config):
    """Update record with time-related fields (all as ISO strings)"""
    timezone = final_config["timezone"]
    current_time = get_current_time_iso(timezone)
    
    # Parse time window for hour-minute formatting
    start_dt = pendulum.parse(start_time_iso)
    hour_min = start_dt.format('HH-mm')

    # Build S3 path components
    s3_prefix_list = final_config["s3_prefix_list"]
    s3_prefix_subpath = '/'.join(s3_prefix_list)
    s3_uri = f"s3://{final_config['s3_bucket']}/{s3_prefix_subpath}/{TARGET_DAY}/{hour_min}/"


    # Calculate actual granularity achieved
    actual_duration_seconds = calculate_duration_seconds(start_time_iso, end_time_iso)
    record['granularity'] = seconds_to_time_string(actual_duration_seconds)
    record.update({
    'WINDOW_START_TIME': start_time_iso,
    'WINDOW_END_TIME': end_time_iso,
    'TARGET_DAY': TARGET_DAY,
    'RECORD_FIRST_CREATED_TIME': current_time,
    'RECORD_LAST_UPDATED_TIME': current_time,
    'SOURCE_CATEGORY': final_config["index_group"],
    'SOURCE_SUB_CATEGORY': final_config["index_name"],

    'STAGE_CATEGORY': final_config["s3_bucket"],
    'STAGE_SUB_CATEGORY': s3_uri,
    
    'TARGET_CATEGORY': f"{final_config['target_database']}.{final_config['target_schema']}.{final_config['target_table']}",
    'TARGET_SUB_CATEGORY': f"{s3_uri}%",

    'MISCELLANEOUS': None
    })

    
    return record


def generate_pipeline_id(record, final_config):
    """Generate unique IDs using name/category/subcategory + time window"""
    
    # Get time window strings for ID generation
    window_start_str = record['WINDOW_START_TIME']
    window_end_str = record['WINDOW_END_TIME']
    
    # Generate SOURCE_ID
    source_hash_input = f"{record.get('SOURCE_NAME', 'unknown')}*{record.get('SOURCE_CATEGORY', '')}*{record.get('SOURCE_SUB_CATEGORY', '')}*{window_start_str}*{window_end_str}"
    SOURCE_ID = hashlib.md5(source_hash_input.encode()).hexdigest()[:16]
    
    # Generate STAGE_ID  
    stage_hash_input = f"{record.get('STAGE_NAME', 'unknown')}*{record.get('STAGE_CATEGORY', '')}*{record.get('STAGE_SUB_CATEGORY', '')}*{window_start_str}*{window_end_str}"
    STAGE_ID = hashlib.md5(stage_hash_input.encode()).hexdigest()[:16]
    
    # Generate TARGET_ID
    target_hash_input = f"{record.get('TARGET_NAME', 'unknown')}*{record.get('TARGET_CATEGORY', '')}*{record.get('TARGET_SUB_CATEGORY', '')}*{window_start_str}*{window_end_str}"
    TARGET_ID = hashlib.md5(target_hash_input.encode()).hexdigest()[:16]
    
    # Generate PIPELINE_ID from the three IDs
    pipeline_hash_input = f"{SOURCE_ID}*{STAGE_ID}*{TARGET_ID}"
    PIPELINE_ID = hashlib.md5(pipeline_hash_input.encode()).hexdigest()[:16]
    
    # Update record with all IDs
    record['SOURCE_ID'] = SOURCE_ID
    record['STAGE_ID'] = STAGE_ID  
    record['TARGET_ID'] = TARGET_ID
    record['PIPELINE_ID'] = PIPELINE_ID
    
    return record


def record_generator(final_config, **context):
    """Main record generator function for pipeline"""
    try:
        log.info(
            "Starting record generation process",
            log_key="Record Generator",
            status="STARTED"
        )
        
        x_time_back_seconds = parse_time_string(final_config['x_time_back'])
        granularity_seconds = parse_time_string(final_config['granularity'])
        timezone = final_config["timezone"]
        
        # Calculate target day using time utility
        now_iso = get_current_time_iso(timezone)
        TARGET_DAY_start_iso = add_duration_to_iso(now_iso, -x_time_back_seconds)
        TARGET_DAY = get_date_only(TARGET_DAY_start_iso)
        
        log.info(
            f"Calculated target day from current time",
            log_key="Record Generator",
            status="TARGET_DAY_CALCULATED",
            TARGET_DAY=TARGET_DAY,
            x_time_back=final_config['x_time_back'],
            granularity=final_config['granularity']
        )
        
        existing_max_end_time = get_existing_drive_records(final_config, TARGET_DAY)
        
        if not existing_max_end_time:
            # Start from beginning of target day + granularity
            TARGET_DAY_start = get_start_of_day_iso(TARGET_DAY, timezone)
            start_time_iso = TARGET_DAY_start
            log.info(
                "No existing records found - starting fresh",
                log_key="Record Generator",
                status="FRESH_START",
                start_time=start_time_iso
            )
        else:
            # Continue from max end time of existing records
            start_time_iso = to_iso_string(existing_max_end_time, timezone) 
            log.info(
                "Found existing records - continuing from last end time",
                log_key="Record Generator",
                status="CONTINUING",
                start_time=start_time_iso
            )
        
        # Calculate end time
        end_time_iso = add_duration_to_iso(start_time_iso, granularity_seconds)
        TARGET_DAY_end_iso = get_end_of_day_iso(TARGET_DAY, timezone)
        
        # Boundary check
        if compare_times(end_time_iso, TARGET_DAY_end_iso) > 0:
            end_time_iso = TARGET_DAY_end_iso
            log.warning(
                "End time exceeds target day boundary - capping at midnight",
                log_key="Record Generator",
                status="BOUNDARY_CAPPED",
                original_end_time=add_duration_to_iso(start_time_iso, granularity_seconds),
                capped_end_time=end_time_iso
            )
        
        # Check if past target day
        if compare_times(start_time_iso, TARGET_DAY_end_iso) >= 0:
            log.info(
                "Start time is past target day - no record needed",
                log_key="Record Generator",
                status="PAST_TARGET_DAY",
                start_time=start_time_iso,
                TARGET_DAY_end=TARGET_DAY_end_iso
            )
            context['task_instance'].xcom_push(key='generated_count', value=0)
            return 0
        
        # Create and populate record
        record = create_base_record(final_config)
        record = update_time_fields(record, start_time_iso, end_time_iso, TARGET_DAY, final_config)
        record = generate_pipeline_id(record, final_config)
        insert_drive_record(record, final_config)
        
        log.info(
            "Record generation completed successfully",
            log_key="Record Generator",
            status="SUCCESS",
            PIPELINE_ID=record['PIPELINE_ID'],
            window_start=start_time_iso,
            window_end=end_time_iso
        )
        
        context['task_instance'].xcom_push(key='generated_count', value=1)
        return 1
        
    except Exception as e:
        log.exception(
            "Error occurred during record generation",
            log_key="Record Generator",
            status="ERROR"
        )
        context['task_instance'].xcom_push(key='generated_count', value=0)
        raise


def validate_record(final_config, **context):
    """Validate generated record for future data and already processed checks"""
    try:
        log.info(
            "Starting record validation",
            log_key="Record Validation",
            status="STARTED"
        )
        
        # Get generated record from XCom
        generated_count = context['task_instance'].xcom_pull(task_ids='record_generator', key='generated_count')
        
        if generated_count == 0:
            log.info(
                "No record generated - skipping validation",
                log_key="Record Validation",
                status="NO_RECORD"
            )
            raise AirflowSkipException("No record to validate")
        
        # Get the actual record (need to fetch from drive table by latest PIPELINE_ID)
        # For now, we'll reconstruct the logic - this could be optimized
        timezone = final_config["timezone"]
        x_time_back_seconds = parse_time_string(final_config['x_time_back'])
        granularity_seconds = parse_time_string(final_config['granularity'])
        
        now_iso = get_current_time_iso(timezone)
        TARGET_DAY_start_iso = add_duration_to_iso(now_iso, -x_time_back_seconds)
        TARGET_DAY = get_date_only(TARGET_DAY_start_iso)
        
        existing_max_end_time = get_existing_drive_records(final_config, TARGET_DAY)
        
        if not existing_max_end_time:
            TARGET_DAY_start = get_start_of_day_iso(TARGET_DAY, timezone)
            start_time_iso = add_duration_to_iso(TARGET_DAY_start, granularity_seconds)
        else:
            start_time_iso = to_iso_string(existing_max_end_time, timezone)
        
        end_time_iso = add_duration_to_iso(start_time_iso, granularity_seconds)
        TARGET_DAY_end_iso = get_end_of_day_iso(TARGET_DAY, timezone)
        
        if compare_times(end_time_iso, TARGET_DAY_end_iso) > 0:
            end_time_iso = TARGET_DAY_end_iso
        
        # Create record object for validation
        record = {
            'WINDOW_START_TIME': start_time_iso,
            'WINDOW_END_TIME': end_time_iso,
            'TARGET_DAY': TARGET_DAY
        }
        
        # CHECK 1: Future data check
        current_time_iso = get_current_time_iso(timezone)
        if compare_times(start_time_iso, current_time_iso) > 0:
            log.info(
                "Future data detected - skipping record",
                log_key="Record Validation",
                status="FUTURE_DATA",
                window_start=start_time_iso,
                current_time=current_time_iso
            )
            raise AirflowSkipException("Record requests future data")       
      
        try:
            source_total = source_count(final_config, record)
            target_total = target_count(final_config, record)
            
            log.info(
                f"Count comparison: Source={source_total}, Target={target_total}",
                log_key="Record Validation",
                status="COUNT_CHECK"
            )
            
            if source_total == target_total and source_total > 0:
                log.info(
                    "Record already processed - marking as SUCCESS",
                    log_key="Record Validation",
                    status="ALREADY_PROCESSED",
                    source_count=source_total,
                    target_count=target_total
                )
                
                # TODO: Update record status to SUCCESS in drive table
                # mark_record_as_completed(record, final_config)
                
                raise AirflowSkipException("Record already processed successfully")
                
        except Exception as count_error:
            log.warning(
                f"Count check failed, proceeding with pipeline: {count_error}",
                log_key="Record Validation",
                status="COUNT_CHECK_FAILED"
            )
        
        # Record is valid - pass to next task
        log.info(
            "Record validation passed",
            log_key="Record Validation",
            status="VALID",
            window_start=start_time_iso,
            window_end=end_time_iso
        )
        
        context['task_instance'].xcom_push(key='validated_record', value=record)
        return record
        
    except AirflowSkipException:
        # Re-raise skip exceptions
        raise
    except Exception as e:
        log.exception(
            "Error during record validation",
            log_key="Record Validation",
            status="ERROR"
        )
        raise







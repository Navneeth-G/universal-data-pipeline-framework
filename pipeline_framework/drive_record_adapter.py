# storage_adapters/drive_record_adapter.py

import json
from typing import Dict, Any, Optional
from pipeline_framework.utils.snowflake_utils import SnowflakeQueryClient
from pipeline_framework.utils.log_generator import setup_pipeline_logger
import pandas as pd
from pipeline_framework.utils.time_utils import get_current_time_iso
log = setup_pipeline_logger(logger_name="DriveRecordAdapter")


def get_existing_drive_records(final_config: Dict[str, Any], target_day: str) -> Optional[str]:
    """
    Get max window_end_time for existing drive records.
    
    Args:
        final_config: Configuration containing Snowflake connection details
        target_day: Target day in YYYY-MM-DD format
        
    Returns:
        Max window_end_time as ISO string or None if no records exist
    """
    try:
        log.info(
            f"Querying max window_end_time for target day: {target_day}",
            log_key="Drive Record Query",
            status="STARTED"
        )
        
        # Extract Snowflake configuration
        snowflake_creds = {
            'account': final_config['snowflake_account'],
            'user': final_config['snowflake_user'],
            'password': final_config['snowflake_password'],
            'role': final_config['snowflake_role'],
            'warehouse': final_config['snowflake_warehouse']
        }
        
        snowflake_config = {
            'database': final_config['drive_database'],
            'schema': final_config['drive_schema'],
            'table': final_config['drive_table']
        }
        
        # Extract pipeline components
        default_record = final_config['drive_table_default_record']
        source_name = default_record.get('source_name')
        source_category = default_record.get('source_category')
        source_sub_category = default_record.get('source_sub_category')                
        # stage_name = default_record.get('stage_name')
        # target_name = default_record.get('target_name')
        
        query = f"""
        SELECT MAX(window_end_time) as max_end_time
        FROM {snowflake_config['database']}.{snowflake_config['schema']}.{snowflake_config['table']}
        WHERE 
          source_name = %(source_name)s
          AND source_category = %(source_category)s
          AND source_sub_category = %(source_sub_category)s
          AND target_day = %(target_day)s 
        """
        
        query_params = {
            'source_name': source_name,
            'source_category': source_category,
            'source_sub_category': source_sub_category,
            'target_day': target_day
        }
        
        with SnowflakeQueryClient(snowflake_creds, snowflake_config) as client:
            result = client.execute_scalar_query(query, query_params)
            max_end_time = result['data']
            
            if max_end_time:
                log.info(
                    f"Found max window_end_time: {max_end_time}",
                    log_key="Drive Record Query",
                    status="SUCCESS",
                    query_id=result['query_id']
                )
                
                return max_end_time
            else:
                log.info(
                    "No existing drive records found",
                    log_key="Drive Record Query",
                    status="SUCCESS",
                    query_id=result['query_id']
                )
                return None
                
    except Exception as e:
        log.exception(
            "Error querying max window_end_time",
            log_key="Drive Record Query",
            status="ERROR"
        )
        raise


def insert_drive_record(record: Dict[str, Any], final_config: Dict[str, Any]) -> None:
    """
    Insert drive record into Snowflake table.
    
    Args:
        record: Complete record dictionary with all 32 fields
        final_config: Configuration containing Snowflake connection details
    """
    try:
        log.info(
            f"Inserting drive record: {record.get('pipeline_id', 'unknown')}",
            log_key="Drive Record Insert",
            status="STARTED"
        )
        
        # Extract Snowflake configuration
        snowflake_creds = {
            'account': final_config['snowflake_account'],
            'user': final_config['snowflake_user'],
            'password': final_config['snowflake_password'],
            'role': final_config['snowflake_role'],
            'warehouse': final_config['snowflake_warehouse']
        }
        
        snowflake_config = {
            'database': final_config['drive_database'],
            'schema': final_config['drive_schema'],
            'table': final_config['drive_table']
        }
        
        # Handle miscellaneous field as JSON
        record_copy = record.copy()
        # use_parse_json = False

        # if record_copy.get('miscellaneous') is not None:
        #     if isinstance(record_copy['miscellaneous'], dict):
        #         record_copy['miscellaneous'] = json.dumps(record_copy['miscellaneous'])
        #         use_parse_json = True
        #     elif isinstance(record_copy['miscellaneous'], str):
        #         use_parse_json = True  # Assume it's already JSON string

        # # Build INSERT query
        # for field in field_names:
        #     if field == 'miscellaneous' and use_parse_json:
        #         field_placeholders.append(f"PARSE_JSON(%({field})s)")
        #     else:
        #         field_placeholders.append(f"%({field})s")
        

                
        with SnowflakeQueryClient(snowflake_creds, snowflake_config) as sf_client:
            result = sf_client.insert_one_record_only(record_copy)

            log.info(
                "Drive record inserted successfully",
                log_key="Drive Record Insert",
                status="SUCCESS",
                query_id=result['query_id'],
                rows_affected=result['rows_affected']
            )
            
    except Exception as e:
        log.exception(
            "Error inserting drive record",
            log_key="Drive Record Insert",
            status="ERROR"
        )
        raise


def cleanup_stale_locks(final_config: Dict[str, Any], stale_hours: int = 2) -> int:
    """Clean up stale locks based on pipeline_status and duration"""
    try:
        log.info(f"Starting stale lock cleanup for duration > {stale_hours} hours",
                log_key="Stale Lock Cleanup", status="STARTED")
        
        snowflake_creds = {
            'account': final_config['snowflake_account'],
            'user': final_config['snowflake_user'],
            'password': final_config['snowflake_password'],
            'role': final_config['snowflake_role'],
            'warehouse': final_config['snowflake_warehouse']
        }
        
        snowflake_config = {
            'database': final_config['drive_database'],
            'schema': final_config['drive_schema'],
            'table': final_config['drive_table']
        }
        
        # Find stale records: IN_PROGRESS + dag_run_id + duration check
        query = f"""
        SELECT pipeline_id, dag_run_id, pipeline_start_time,
               source_to_stage_ingestion_status,
               stage_to_target_ingestion_status,
               audit_status
        FROM {snowflake_config['database']}.{snowflake_config['schema']}.{snowflake_config['table']}
        WHERE pipeline_status = 'IN_PROGRESS'
          AND dag_run_id IS NOT NULL
          AND pipeline_start_time IS NOT NULL
          AND DATEDIFF('hour', pipeline_start_time, CURRENT_TIMESTAMP()) > %(stale_hours)s
        """
        
        with SnowflakeQueryClient(snowflake_creds, snowflake_config) as client:
            result = client.fetch_all_rows_as_tuples(query, {'stale_hours': stale_hours})
            stale_records = result['data']
            
            if not stale_records:
                log.info("No stale locks found", log_key="Stale Lock Cleanup", status="NO_STALE_LOCKS")
                return 0
            
            # Reset each stale record
            cleaned_count = 0
            for record_tuple in stale_records:
                pipeline_id, dag_run_id, start_time, s2s_status, s2t_status, audit_status = record_tuple
                
                log.warning(f"Cleaning stale lock: pipeline_id={pipeline_id}, duration exceeded",
                           log_key="Stale Lock Cleanup", status="CLEANING")
                
                # Reset pipeline level
                reset_fields = {
                    'pipeline_status': 'PENDING',
                    'pipeline_start_time': None,
                    'pipeline_end_time': None,
                    'dag_run_id': None,
                    'completed_phase': None
                }
                
                # Reset incomplete phases only
                if s2s_status != 'COMPLETED':
                    reset_fields.update({
                        'source_to_stage_ingestion_status': 'PENDING',
                        'source_to_stage_ingestion_start_time': None,
                        'source_to_stage_ingestion_end_time': None
                    })
                
                if s2t_status != 'COMPLETED':
                    reset_fields.update({
                        'stage_to_target_ingestion_status': 'PENDING',
                        'stage_to_target_ingestion_start_time': None,
                        'stage_to_target_ingestion_end_time': None
                    })
                
                if audit_status != 'COMPLETED':
                    reset_fields.update({
                        'audit_status': 'PENDING',
                        'audit_start_time': None,
                        'audit_end_time': None,
                        'audit_result': None
                    })
                
                update_record_fields(pipeline_id, reset_fields, final_config)
                cleaned_count += 1
            
            log.info(f"Cleaned up {cleaned_count} stale locks",
                    log_key="Stale Lock Cleanup", status="SUCCESS")
            return cleaned_count
            
    except Exception as e:
        log.exception("Error during stale lock cleanup",
                     log_key="Stale Lock Cleanup", status="ERROR")
        raise


def update_record_fields(pipeline_id: str, fields_to_update: Dict[str, Any], final_config: Dict[str, Any]) -> None:
    """
    Update specific fields for a drive record identified by pipeline_id.

    Args:
        pipeline_id: Unique identifier for the pipeline record.
        fields_to_update: Dict of fields to update and their new values.
        final_config: Configuration dict with Snowflake credentials and table info.
    """
    try:
        log.info(f"Updating record fields for pipeline_id: {pipeline_id}", log_key="Drive Record Update", status="STARTED")

        snowflake_creds = {
            'account': final_config['snowflake_account'],
            'user': final_config['snowflake_user'],
            'password': final_config['snowflake_password'],
            'role': final_config['snowflake_role'],
            'warehouse': final_config['snowflake_warehouse']
        }

        snowflake_config = {
            'database': final_config['drive_database'],
            'schema': final_config['drive_schema'],
            'table': final_config['drive_table']
        }

        set_clauses = []
        params = {}
        for i, (field, value) in enumerate(fields_to_update.items()):
            param_key = f"val_{i}"
            set_clauses.append(f"{field} = %({param_key})s")
            params[param_key] = value

        query = f"""
        UPDATE {snowflake_config['database']}.{snowflake_config['schema']}.{snowflake_config['table']}
        SET {', '.join(set_clauses)}
        WHERE pipeline_id = %(pipeline_id)s
        """

        params['pipeline_id'] = pipeline_id

        with SnowflakeQueryClient(snowflake_creds, snowflake_config) as client:
            result = client.execute_dml_query(query, params)

        log.info("Record fields updated successfully",
                 log_key="Drive Record Update",
                 status="SUCCESS",
                 query_id=result['query_id'],
                 rows_affected=result['rows_affected'])

    except Exception as e:
        log.exception("Error updating drive record fields", log_key="Drive Record Update", status="ERROR")
        raise


def update_record_in_drive_table(record: Dict[str, Any], final_config: Dict[str, Any]) -> None:
    """Update record in drive table/central location"""
    try:
        timezone = final_config.get('timezone', 'UTC')
        record['record_last_updated_time'] = get_current_time_iso(timezone)

        update_record_fields(
            pipeline_id=record['pipeline_id'],
            fields_to_update=record,
            final_config=final_config
        )

        log.info("Record updated in drive table", pipeline_id=record['pipeline_id'])
        
    except Exception as e:
        log.exception("Failed to update record in drive table", pipeline_id=record['pipeline_id'])
        raise



def get_oldest_pending_record(final_config: Dict[str, Any], priority: str) -> Optional[Dict[str, Any]]:
    query = f"""
    SELECT * FROM {final_config['drive_database']}.{final_config['drive_schema']}.{final_config['drive_table']}
    WHERE pipeline_status = 'PENDING'
      AND source_name = %(source_name)s
      AND source_category = %(source_category)s
      AND source_sub_category = %(source_sub_category)s
      AND pipeline_priority = %(priority)s
    ORDER BY window_start_time ASC
    LIMIT 1
    """
    params = {
        "source_name": final_config.get("source_name"),
        "source_category": final_config.get("index_group"),
        "source_sub_category": final_config.get("index_name"),
        "priority": priority
    }
    # Extract Snowflake configuration
    snowflake_creds = {
            'account': final_config['snowflake_account'],
            'user': final_config['snowflake_user'],
            'password': final_config['snowflake_password'],
            'role': final_config['snowflake_role'],
            'warehouse': final_config['snowflake_warehouse']
            }
        
    snowflake_config = {
            'database': final_config['drive_database'],
            'schema': final_config['drive_schema'],
            'table': final_config['drive_table']
        }

    with SnowflakeQueryClient(snowflake_creds, snowflake_config) as client:
        result = client.fetch_all_rows_as_dataframe(query, params)
        df = result.get('data')
        if df is not None and not df.empty: 
            return df.iloc[0].to_dict()
        return None






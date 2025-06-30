# storage_adapters/drive_record_adapter.py

import json
from typing import Dict, Any, Optional
from pipeline_framework.utils.snowflake_utils import SnowflakeQueryClient
from pipeline_framework.utils.log_generator import setup_pipeline_logger
import pandas as pd
from pipeline_framework.utils.time_utils import get_current_time_iso
log = setup_pipeline_logger(logger_name="DriveRecordAdapter")


def get_existing_drive_records(final_config: Dict[str, Any], TARGET_DAY: str) -> Optional[str]:
    """
    Get max WINDOW_END_TIME for existing drive records.
    
    Args:
        final_config: Configuration containing Snowflake connection details
        TARGET_DAY: Target day in YYYY-MM-DD format
        
    Returns:
        Max WINDOW_END_TIME as ISO string or None if no records exist
    """
    try:
        log.info(
            f"Querying max WINDOW_END_TIME for target day: {TARGET_DAY}",
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
        SOURCE_NAME = default_record.get('SOURCE_NAME')
        SOURCE_CATEGORY = default_record.get('SOURCE_CATEGORY')
        SOURCE_SUB_CATEGORY = default_record.get('SOURCE_SUB_CATEGORY')                
        # stage_name = default_record.get('stage_name')
        # target_name = default_record.get('target_name')
        
        query = f"""
        SELECT MAX(WINDOW_END_TIME) as max_end_time
        FROM {snowflake_config['database']}.{snowflake_config['schema']}.{snowflake_config['table']}
        WHERE 
          SOURCE_NAME = %(SOURCE_NAME)s
          AND SOURCE_CATEGORY = %(SOURCE_CATEGORY)s
          AND SOURCE_SUB_CATEGORY = %(SOURCE_SUB_CATEGORY)s
          AND TARGET_DAY = %(TARGET_DAY)s 
        """
        
        query_params = {
            'SOURCE_NAME': SOURCE_NAME,
            'SOURCE_CATEGORY': SOURCE_CATEGORY,
            'SOURCE_SUB_CATEGORY': SOURCE_SUB_CATEGORY,
            'TARGET_DAY': TARGET_DAY
        }
        
        with SnowflakeQueryClient(snowflake_creds, snowflake_config) as client:
            result = client.execute_scalar_query(query, query_params)
            max_end_time = result['data']
            
            if max_end_time:
                log.info(
                    f"Found max WINDOW_END_TIME: {max_end_time}",
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
            "Error querying max WINDOW_END_TIME",
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
            f"Inserting drive record: {record.get('PIPELINE_ID', 'unknown')}",
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
            return result
            
    except Exception as e:
        log.exception(
            "Error inserting drive record",
            log_key="Drive Record Insert",
            status="ERROR"
        )
        raise


def cleanup_stale_locks(final_config: Dict[str, Any], stale_hours: int = 2) -> int:
    """Clean up stale locks based on PIPELINE_STATUS and duration"""
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
        
        # Find stale records: IN_PROGRESS + DAG_RUN_ID + duration check
        query = f"""
        SELECT PIPELINE_ID, DAG_RUN_ID, PIPELINE_START_TIME,
               SOURCE_TO_STAGE_INGESTION_STATUS,
               STAGE_TO_TARGET_INGESTION_STATUS,
               AUDIT_STATUS
        FROM {snowflake_config['database']}.{snowflake_config['schema']}.{snowflake_config['table']}
        WHERE PIPELINE_STATUS = 'IN_PROGRESS'
          AND DAG_RUN_ID IS NOT NULL
          AND PIPELINE_START_TIME IS NOT NULL
          AND DATEDIFF('hour', PIPELINE_START_TIME, CURRENT_TIMESTAMP()) > %(stale_hours)s
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
                PIPELINE_ID, DAG_RUN_ID, PIPELINE_START_TIME, SOURCE_TO_STAGE_INGESTION_STATUS, STAGE_TO_TARGET_INGESTION_STATUS, AUDIT_STATUS = record_tuple

                log.warning(f"Cleaning stale lock: PIPELINE_ID={PIPELINE_ID}, duration exceeded",
                           log_key="Stale Lock Cleanup", status="CLEANING")
                
                # Reset pipeline level
                reset_fields = {
                    'PIPELINE_STATUS': 'PENDING',
                    'PIPELINE_START_TIME': None,
                    'PIPELINE_END_TIME': None,
                    'DAG_RUN_ID': None,
                    'COMPLETED_PHASE': None
                }
                
                # Reset incomplete phases only
                if s2s_status != 'COMPLETED':
                    reset_fields.update({
                        'SOURCE_TO_STAGE_INGESTION_STATUS': 'PENDING',
                        'SOURCE_TO_STAGE_INGESTION_START_TIME': None,
                        'SOURCE_TO_STAGE_INGESTION_END_TIME': None
                    })
                
                if s2t_status != 'COMPLETED':
                    reset_fields.update({
                        'STAGE_TO_TARGET_INGESTION_STATUS': 'PENDING',
                        'STAGE_TO_TARGET_INGESTION_START_TIME': None,
                        'STAGE_TO_TARGET_INGESTION_END_TIME': None
                    })
                
                if AUDIT_STATUS != 'COMPLETED':
                    reset_fields.update({
                        'AUDIT_STATUS': 'PENDING',
                        'AUDIT_START_TIME': None,
                        'AUDIT_END_TIME': None,
                        'AUDIT_RESULT': None
                    })
                
                update_record_fields(PIPELINE_ID, reset_fields, final_config)
                cleaned_count += 1
            
            log.info(f"Cleaned up {cleaned_count} stale locks",
                    log_key="Stale Lock Cleanup", status="SUCCESS")
            return cleaned_count
            
    except Exception as e:
        log.exception("Error during stale lock cleanup",
                     log_key="Stale Lock Cleanup", status="ERROR")
        raise


def update_record_fields(PIPELINE_ID: str, fields_to_update: Dict[str, Any], final_config: Dict[str, Any]) -> None:
    """
    Update specific fields for a drive record identified by PIPELINE_ID.

    Args:
        PIPELINE_ID: Unique identifier for the pipeline record.
        fields_to_update: Dict of fields to update and their new values.
        final_config: Configuration dict with Snowflake credentials and table info.
    """
    try:
        log.info(f"Updating record fields for PIPELINE_ID: {PIPELINE_ID}", log_key="Drive Record Update", status="STARTED")

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
        WHERE PIPELINE_ID = %(PIPELINE_ID)s
        """

        params['PIPELINE_ID'] = PIPELINE_ID

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
        record['RECORD_LAST_UPDATED_TIME'] = get_current_time_iso(timezone)

        update_record_fields(
            PIPELINE_ID=record['PIPELINE_ID'],
            fields_to_update=record,
            final_config=final_config
        )

        log.info("Record updated in drive table", PIPELINE_ID=record['PIPELINE_ID'])
        
    except Exception as e:
        log.exception("Failed to update record in drive table", PIPELINE_ID=record['PIPELINE_ID'])
        raise



def get_oldest_pending_record(final_config: Dict[str, Any], PIPELINE_PRIORITY: str) -> Optional[Dict[str, Any]]:
    query = f"""
    SELECT * FROM {final_config['drive_database']}.{final_config['drive_schema']}.{final_config['drive_table']}
    WHERE PIPELINE_STATUS = 'PENDING'
      AND SOURCE_NAME = %(SOURCE_NAME)s
      AND SOURCE_CATEGORY = %(SOURCE_CATEGORY)s
      AND SOURCE_SUB_CATEGORY = %(SOURCE_SUB_CATEGORY)s
      AND PIPELINE_PRIORITY = %(PIPELINE_PRIORITY)s
    ORDER BY WINDOW_START_TIME ASC
    LIMIT 1
    """
    params = {
        "SOURCE_NAME": final_config.get("SOURCE_NAME"),
        "SOURCE_CATEGORY": final_config.get("index_group"),
        "SOURCE_SUB_CATEGORY": final_config.get("index_name"),
        "PIPELINE_PRIORITY": PIPELINE_PRIORITY
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






# pipeline_framework/snowflake_operations.py
from typing import Dict, Any
from pipeline_framework.utils.snowflake_utils import SnowflakeQueryClient
from pipeline_framework.utils.log_generator import setup_pipeline_logger

log = setup_pipeline_logger(logger_name="SnowflakeOps")

def snowflake_connection(final_config: Dict[str, Any]) -> SnowflakeQueryClient:
    """Create and validate Snowflake connection"""
    try:
        snowflake_creds = {
            'account': final_config['snowflake_account'],
            'user': final_config['snowflake_user'],
            'password': final_config['snowflake_password'],
            'role': final_config['snowflake_role'],
            'warehouse': final_config['snowflake_warehouse']
        }
        
        snowflake_config = {
            'database': final_config['target_database'],
            'schema': final_config['target_schema'],
            'table': final_config['target_table']
        }
        
        # Create client and test connection
        client = SnowflakeQueryClient(snowflake_creds, snowflake_config)
        
        # Test connection with simple query
        test_result = client.execute_scalar_query("SELECT 1 as test")
        if test_result['data'] != 1:
            raise ConnectionError("Snowflake connection test failed")
        
        log.info("Snowflake connection successful")
        return client
        
    except Exception as e:
        log.error(f"Snowflake connection failed: {e}")
        raise ConnectionError(f"Snowflake connection failed: {e}")

def snowflake_count(final_config: Dict[str, Any], record: Dict[str, Any]) -> int:
    """Count records in Snowflake target table"""
    try:
        with snowflake_connection(final_config) as client:
            query = f"""
            SELECT COUNT(*) as count
            FROM {final_config['target_database']}.{final_config['target_schema']}.{final_config['target_table']}
            WHERE FILENAME LIKE  '{record["target_subcategory"]}'
            """
            
            result = client.execute_scalar_query(query, {})
            count = result['data'] or 0
            
        log.info(f"Snowflake count: {count}", pipeline_id=record['pipeline_id'])
        return count
        
    except Exception as e:
        log.exception("Snowflake count failed", pipeline_id=record['pipeline_id'])
        raise

def snowflake_check_exists(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Check if data exists in Snowflake target table"""
    try:
        count = snowflake_count(final_config, record)
        return count > 0
    except Exception as e:
        log.exception("Snowflake exists check failed", pipeline_id=record['pipeline_id'])
        raise

def snowflake_delete(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Delete records from Snowflake target table"""
    try:
        with snowflake_connection(final_config) as client:
            query = f"""
            DELETE FROM {final_config['target_database']}.{final_config['target_schema']}.{final_config['target_table']}
            WHERE FILENAME LIKE  '{record["target_subcategory"]}'
            """
            
            result = client.execute_dml_query(query, {})
            
        log.info(f"Snowflake delete: {result['rows_affected']} rows", pipeline_id=record['pipeline_id'])
        return True
        
    except Exception as e:
        log.exception("Snowflake delete failed", pipeline_id=record['pipeline_id'])
        raise
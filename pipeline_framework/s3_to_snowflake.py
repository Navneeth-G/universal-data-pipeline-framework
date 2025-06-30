# pipeline_framework/s3_to_snowflake.py

import time
from typing import Dict, Any
from pipeline_framework.utils.snowflake_utils import SnowflakeQueryClient
from pipeline_framework.utils.log_generator import setup_pipeline_logger
from pipeline_framework.utils.log_retry_decorators import retry, log_execution_time

log = setup_pipeline_logger(logger_name="S3ToSnowflake")

@retry(max_attempts=3, delay_seconds=30)
def execute_snowflake_task(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Execute Snowflake task that wraps snowpipe"""
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
    
    task_name = final_config['snowflake_task_name']
    execute_query = f"EXECUTE TASK {task_name}"
    
    with SnowflakeQueryClient(snowflake_creds, snowflake_config) as client:
        result = client.execute_control_command(execute_query)
        
        log.info(
            f"Snowflake task executed: {task_name}",
            pipeline_id=record['pipeline_id'],
            query_id=result['query_id']
        )
        
        return True

@log_execution_time
def transfer_s3_to_snowflake(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Main transfer function with 2-minute wait"""
    try:
        log.info(
            "Starting Snowflake task execution",
            pipeline_id=record['pipeline_id'],
            task_name=final_config['snowflake_task_name']
        )
        
        success = execute_snowflake_task(final_config, record)
        
        if success:
            log.info(
                "Snowflake task triggered - waiting 2 minutes for snowpipe completion",
                pipeline_id=record['pipeline_id']
            )
            
            time.sleep(120)  # 2 minutes
            
            log.info(
                "Snowpipe wait completed - transfer assumed successful",
                pipeline_id=record['pipeline_id']
            )
        
        return success
        
    except Exception as e:
        log.exception(
            "Snowflake task execution failed",
            pipeline_id=record['pipeline_id']
        )
        raise




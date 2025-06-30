# pipeline_framework/elasticsearch_to_s3.py

import subprocess
import json
from typing import Dict, Any
from pipeline_framework.utils.log_generator import setup_pipeline_logger
from pipeline_framework.utils.time_utility import to_elasticsearch_format, get_current_epoch_time

log = setup_pipeline_logger(logger_name="ElasticsearchToS3")

def build_elasticdump_command(final_config: Dict[str, Any], record: Dict[str, Any]) -> list:
    """Build elasticdump command with configurable parameters"""
    
    # Get config values
    es_format = final_config.get('es_format', 'iso')
    timezone = final_config.get('timezone', 'UTC')

    # Convert timestamps to ES format with timezone
    start_time = to_elasticsearch_format(record['window_start_time'], es_format, timezone)
    end_time = to_elasticsearch_format(record['window_end_time'], es_format, timezone)
    
    # Build query
    search_body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            final_config['elasticsearch_timestamp_field']: {
                                "gte": start_time,
                                "lt": end_time
                            }
                        }
                    }
                ]
            }
        }
    }
    
    # Add additional filters if specified
    if 'elasticsearch_filters' in final_config:
        search_body["query"]["bool"]["must"].extend(final_config['elasticsearch_filters'])
    
    # ES source URL
    es_input = f"http://{final_config['es_username']}:{final_config['es_password']}@{final_config['es_hostname']}:{final_config['es_port']}/{final_config['index_pattern']}"
    
    # Get epoch time from utility
    epoch_time = get_current_epoch_time(timezone)
    
    # Build clean S3 output path
    s3_uri_complete_path = f"{record['stage_sub_category']}{final_config['index_pattern']}_{epoch_time}.json"
    
    cmd = [
        "elasticdump",
        f"--input={es_input}",
        f"--output={s3_uri_complete_path}",
        f"--searchBody={json.dumps(search_body)}",
        "--type=data",
        f"--s3AccessKeyId={final_config['aws_access_key_id']}",
        f"--s3SecretAccessKey={final_config['aws_secret_access_key']}",
        f"--s3Region={final_config['aws_region']}",
        f"--limit={final_config.get('elasticdump_limit', 10000)}",
        f"--fileSize={final_config.get('elasticdump_file_size', '250mb')}",
        f"--retryAttempts={final_config.get('elasticdump_retry_attempts', 3)}",
        f"--retryDelay={final_config.get('elasticdump_retry_delay', 5000)}",
        "--toLog=true"
    ]
    
    return cmd

def execute_transfer(cmd: list, record: Dict[str, Any], final_config: Dict[str, Any]) -> bool:
    """Execute elasticdump with extended timeout for large datasets"""
    try:
        # Extended timeout for large transfers (default 4 hours)
        timeout_seconds = final_config.get('transfer_timeout_seconds', 14400)  # 4 hours
        
        log.info(
            "Starting elasticdump ES→S3 transfer (large dataset mode)",
            pipeline_id=record['pipeline_id'],
            timeout_hours=timeout_seconds/3600
        )
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Track progress with periodic logging
        output_lines = []
        last_progress_log = 0
        line_count = 0
        
        for line in iter(process.stdout.readline, ''):
            line = line.strip()
            if line:
                line_count += 1
                output_lines.append(line)
                
                # Log progress every 100 lines or if line contains transfer info
                if (line_count - last_progress_log >= 100) or any(keyword in line.lower() for keyword in ['written', 'dumped', 'transferred', 'complete']):
                    log.info(
                        f"Elasticdump progress: {line}",
                        pipeline_id=record['pipeline_id'],
                        lines_processed=line_count
                    )
                    last_progress_log = line_count
        
        process.wait(timeout=timeout_seconds)
        
        if process.returncode == 0:
            log.info(
                "Large elasticdump transfer completed successfully",
                pipeline_id=record['pipeline_id'],
                total_output_lines=line_count
            )
            return True
        else:
            log.error(
                "Large elasticdump transfer failed",
                pipeline_id=record['pipeline_id'],
                return_code=process.returncode,
                total_output_lines=line_count
            )
            return False
            
    except subprocess.TimeoutExpired:
        process.kill()
        log.error(
            "Elasticdump timeout after extended period",
            pipeline_id=record['pipeline_id'],
            timeout_hours=timeout_seconds/3600
        )
        return False
    except Exception as e:
        log.exception(
            "Large elasticdump execution failed",
            pipeline_id=record['pipeline_id']
        )
        return False

def transfer_elasticsearch_to_s3(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Main transfer function called by source_to_stage.py"""
    try:
        cmd = build_elasticdump_command(final_config, record)
        return execute_transfer(cmd, record, final_config)
        
    except Exception as e:
        log.exception(
            "Error in elasticsearch to S3 transfer",
            pipeline_id=record['pipeline_id']
        )
        return False





# pipeline_framework/s3_operations.py
from typing import Dict, Any
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from pipeline_framework.utils.log_generator import setup_pipeline_logger

log = setup_pipeline_logger(logger_name="S3Ops")

def s3_connection(final_config: Dict[str, Any]) -> boto3.client:
    """Create and validate S3 connection"""
    try:
        # Create S3 client
        s3 = boto3.client(
            's3',
            aws_access_key_id=final_config['aws_access_key_id'],
            aws_secret_access_key=final_config['aws_secret_access_key'],
            region_name=final_config['aws_region']
        )
        
        # Test connection by listing buckets
        s3.list_buckets()
        
        log.info("S3 connection successful")
        return s3
        
    except NoCredentialsError:
        log.error("S3 credentials not found")
        raise ConnectionError("S3 credentials not found")
    except ClientError as e:
        log.error(f"S3 connection failed: {e}")
        raise ConnectionError(f"S3 connection failed: {e}")
    except Exception as e:
        log.error(f"Unexpected S3 connection error: {e}")
        raise ConnectionError(f"S3 connection error: {e}")

def s3_count(final_config: Dict[str, Any], record: Dict[str, Any]) -> int:
    """Count objects in S3 for given pipeline"""
    try:
        s3 = s3_connection(final_config)
        
        # Get S3 path from record
        s3_path = record['STAGE_SUB_CATEGORY']
        if not s3_path.startswith('s3://'):
            raise ValueError(f"Invalid S3 path format: {s3_path}")
        
        # Extract bucket and prefix
        path_parts = s3_path[5:].split('/', 1)  # Remove 's3://'
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ""
        
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix
        )
        
        count = 0
        for page in page_iterator:
            if 'Contents' in page:
                count += len(page['Contents'])  # Counting NUMBER OF FILES
        
        log.info(f"S3 file count: {count}", PIPELINE_ID=record['PIPELINE_ID'])
        return count
        
    except Exception as e:
        log.exception("S3 count failed", PIPELINE_ID=record['PIPELINE_ID'])
        raise

def s3_check_exists(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Check if objects exist in S3 using STAGE_SUB_CATEGORY from record"""
    try:
        s3 = s3_connection(final_config)
        
        # Get S3 path from record
        s3_path = record['STAGE_SUB_CATEGORY']
        if not s3_path.startswith('s3://'):
            raise ValueError(f"Invalid S3 path format: {s3_path}")
        
        # Extract bucket and prefix
        path_parts = s3_path[5:].split('/', 1)  # Remove 's3://'
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ""
        
        # Check if any objects exist with this prefix
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1
        )
        
        exists = 'Contents' in response and len(response['Contents']) > 0
        log.info(f"S3 exists check: {exists}", PIPELINE_ID=record['PIPELINE_ID'])
        return exists
        
    except Exception as e:
        log.exception("S3 exists check failed", PIPELINE_ID=record['PIPELINE_ID'])
        raise

def s3_delete(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Delete objects from S3 using STAGE_SUB_CATEGORY from record"""
    try:
        s3 = s3_connection(final_config)
        
        # Get S3 path from record
        s3_path = record['STAGE_SUB_CATEGORY']
        if not s3_path.startswith('s3://'):
            raise ValueError(f"Invalid S3 path format: {s3_path}")
        
        # Extract bucket and prefix
        path_parts = s3_path[5:].split('/', 1)  # Remove 's3://'
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ""
        
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix
        )
        
        delete_count = 0
        for page in page_iterator:
            if 'Contents' in page:
                objects = [{'Key': obj['Key']} for obj in page['Contents']]
                if objects:
                    s3.delete_objects(
                        Bucket=bucket,
                        Delete={'Objects': objects}
                    )
                    delete_count += len(objects)
        
        log.info(f"S3 delete: {delete_count} objects", PIPELINE_ID=record['PIPELINE_ID'])
        return True
        
    except Exception as e:
        log.exception("S3 delete failed", PIPELINE_ID=record['PIPELINE_ID'])
        raise



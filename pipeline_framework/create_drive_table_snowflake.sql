CREATE TABLE PIPELINE_CONTROL.ORCHESTRATION.PIPELINE_RECORDS (
    source_id VARCHAR(50),
    source_name VARCHAR(100),
    source_category VARCHAR(100),
    source_sub_category VARCHAR(500),
    
    stage_id VARCHAR(50),
    stage_name VARCHAR(100),
    stage_category VARCHAR(100),
    stage_sub_category VARCHAR(500),
    
    target_id VARCHAR(50),
    target_name VARCHAR(100),
    target_category VARCHAR(100),
    target_sub_category VARCHAR(500),
    
    source_to_stage_ingestion_start_time TIMESTAMP_TZ,
    source_to_stage_ingestion_end_time TIMESTAMP_TZ,
    source_to_stage_ingestion_status VARCHAR(20) DEFAULT 'PENDING',
    
    stage_to_target_ingestion_start_time TIMESTAMP_TZ,
    stage_to_target_ingestion_end_time TIMESTAMP_TZ,
    stage_to_target_ingestion_status VARCHAR(20) DEFAULT 'PENDING',
    
    audit_start_time TIMESTAMP_TZ,
    audit_end_time TIMESTAMP_TZ,
    audit_status VARCHAR(20) DEFAULT 'PENDING',
    
    pipeline_id VARCHAR(50) PRIMARY KEY,
    pipeline_start_time TIMESTAMP_TZ,
    pipeline_end_time TIMESTAMP_TZ,
    pipeline_status VARCHAR(20) DEFAULT 'PENDING',
    pipeline_priority DECIMAL(5,2) DEFAULT 1.1,
    
    dag_run_id VARCHAR(200),
    audit_result VARCHAR(50),
    source_count INTEGER,
    target_count INTEGER,
    count_difference INTEGER,
    percentage_difference DECIMAL(10,4),
    completed_phase VARCHAR(50),
    retry_attempt INTEGER DEFAULT 0,
    granularity VARCHAR(20),
    window_start_time TIMESTAMP_TZ,
    window_end_time TIMESTAMP_TZ,
    target_day VARCHAR(20),
    miscellaneous VARIANT,
    
    record_first_created_time TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    record_last_updated_time TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);
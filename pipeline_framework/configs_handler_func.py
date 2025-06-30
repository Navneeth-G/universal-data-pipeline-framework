# pipeline_framework/simple_config_handler.py
import json
import os
from typing import Dict, Any

def load_json_file(file_path: str) -> Dict[str, Any]:
    """Load and validate JSON file"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    with open(file_path, 'r') as f:
        return json.load(f)

def get_airflow_variables(host_name: str) -> Dict[str, Any]:
    """Get ALL Airflow variables using host_name or return fallback"""
    try:
        from airflow.models import Variable
        
        env = Variable.get("env", default_var="dev")
        
        # Get ALL environment-specific variables 
        airflow_vars = {
            "env": env,
            
            # Elasticsearch credentials using host_name from config
            "es_username": Variable.get(f"{host_name}_user"),
            "es_password": Variable.get(f"{host_name}_password"),
            "es_ca_certs_path": Variable.get(f"{host_name}_ca_certs_path"),
            "es_header": Variable.get(f"{host_name}_header"),
            
            # AWS S3 config
            "aws_access_key_id": Variable.get(f"s3_access_key_id"),
            "aws_secret_access_key": Variable.get(f"s3_secret_access_key"),
            "s3_region": Variable.get(f"s3_region"),
            "s3_bucket_name": Variable.get(f"s3_bucket_name"),
            
            # Snowflake credentials
            "sf_username": Variable.get(f"snowflake_user"),
            "sf_password": Variable.get(f"snowflake_password"),
            "sf_account": Variable.get(f"snowflake_account"),
            "sf_warehouse": Variable.get(f"snowflake_warehouse"),
            "sf_role": Variable.get(f"snowflake_role")
        }
        
        print(f" Successfully loaded Airflow variables for host: {host_name}, env: {env}")
        
    except (ModuleNotFoundError, Exception) as e:
        print(f"  Airflow not available ({e}), using dev fallback credentials")
        
        # Complete fallback credentials for local development
        airflow_vars = {
            "env": "dev",
            
            # Elasticsearch dev credentials
            "es_username": "dev_es_user",
            "es_password": "dev_es_password", 
            "es_ca_certs_path": "/path/to/dev/ca_certs.pem",
            "es_header": '{"Authorization": "Basic ZGV2X3VzZXI6ZGV2X3Bhc3M="}',
            
            # AWS S3 dev credentials
            "aws_access_key_id": "AKIA_DEV_ACCESS_KEY",
            "aws_secret_access_key": "dev_secret_access_key_12345",
            "s3_region": "us-west-1",
            "s3_bucket_name": "dev-test-data-bucket",
            
            # Snowflake dev credentials  
            "sf_username": "dev_sf_user",
            "sf_password": "dev_sf_password",
            "sf_account": "dev_account.us-west-1",
            "sf_warehouse": "DEV_COMPUTE_WH",
            "sf_role": "DEV_DATA_LOADER_ROLE"
        }
        
    return airflow_vars

def map_airflow_to_config(airflow_vars: Dict[str, Any]) -> Dict[str, Any]:
    """Map airflow variable names to config keys"""
    mapping = {}
    
    # Direct mappings - add ALL airflow variables
    for key, value in airflow_vars.items():
        if value is not None and value != "":
            mapping[key] = value
    
    # Additional mappings for different naming conventions
    if airflow_vars.get("s3_bucket_name"):
        mapping["bucket_name"] = airflow_vars["s3_bucket_name"]
        mapping["s3_bucket"] = airflow_vars["s3_bucket_name"]
    if airflow_vars.get("s3_region"):
        mapping["aws_region"] = airflow_vars["s3_region"]
        
    # Elasticsearch mappings
    if airflow_vars.get("es_username"):
        mapping["es_username"] = airflow_vars["es_username"]
    if airflow_vars.get("es_password"):
        mapping["es_password"] = airflow_vars["es_password"]
    if airflow_vars.get("es_ca_certs_path"):
        mapping["es_ca_certs_path"] = airflow_vars["es_ca_certs_path"]
    if airflow_vars.get("es_header"):
        mapping["es_header"] = airflow_vars["es_header"]
        
    # Snowflake mappings
    if airflow_vars.get("sf_username"):
        mapping["snowflake_user"] = airflow_vars["sf_username"]
    if airflow_vars.get("sf_password"):
        mapping["snowflake_password"] = airflow_vars["sf_password"]
    if airflow_vars.get("sf_account"):
        mapping["snowflake_account"] = airflow_vars["sf_account"]
    if airflow_vars.get("sf_warehouse"):
        mapping["snowflake_warehouse"] = airflow_vars["sf_warehouse"]
    if airflow_vars.get("sf_role"):
        mapping["snowflake_role"] = airflow_vars["sf_role"]
        
    # AWS credentials
    if airflow_vars.get("aws_access_key_id"):
        mapping["aws_access_key_id"] = airflow_vars["aws_access_key_id"]
    if airflow_vars.get("aws_secret_access_key"):
        mapping["aws_secret_access_key"] = airflow_vars["aws_secret_access_key"]
        
    return mapping

def replace_placeholders(config: Dict[str, Any]) -> Dict[str, Any]:
    """Replace {env}, {index_group}, {index_name}, {index_pattern} placeholders"""
    env = config.get("env", "dev")
    index_group = config.get("index_group", "")
    index_name = config.get("index_name", "")
    index_pattern = config.get("index_pattern", "")
    
    def replace_value(value):
        if isinstance(value, str):
            return (value.replace("{env}", env)
                        .replace("{index_group}", index_group)
                        .replace("{index_name}", index_name)
                        .replace("{index_pattern}", index_pattern))
        elif isinstance(value, list):
            return [replace_value(item) for item in value]
        elif isinstance(value, dict):
            return {k: replace_value(v) for k, v in value.items()}
        return value
    
    # Apply placeholder replacement to all values
    replaced_config = {}
    for key, value in config.items():
        replaced_config[key] = replace_value(value)
        
    return replaced_config

def update_drive_table_keys_only(drive_table: Dict[str, Any], updated_config: Dict[str, Any]) -> Dict[str, Any]:
    """Update drive table with only keys that exist in drive table"""
    updated_drive_table = drive_table.copy()
    
    # Only update keys that already exist in drive table
    for key in drive_table.keys():
        if key in updated_config:
            updated_drive_table[key] = updated_config[key]
    
    return updated_drive_table

def get_final_config(root_project_path: str, team_config_relative_path: str, drive_template_relative_path: str) -> Dict[str, Any]:
    """Complete config merging with placeholder replacement"""
    
    # Build absolute paths
    team_config_path = os.path.join(root_project_path, team_config_relative_path)
    drive_template_path = os.path.join(root_project_path, drive_template_relative_path)
    
    # Step 1: Load team config
    team_config = load_json_file(team_config_path)
    print(f" Loaded team config: {len(team_config)} keys")
    
    # Step 2: Get host_name and load Airflow variables
    host_name = team_config.get("host_name")
    if not host_name:
        raise ValueError("host_name not found in team config")
    
    airflow_vars = get_airflow_variables(host_name)
    print(f" Loaded Airflow variables: {len(airflow_vars)} keys")
    
    # Step 3: Map and merge all Airflow variables  
    airflow_mapped = map_airflow_to_config(airflow_vars)
    print(f" Mapped Airflow variables: {len(airflow_mapped)} keys")
    
    # Step 4: Merge configs (Airflow priority)
    merged_config = team_config.copy()
    merged_config.update(airflow_mapped)  # Airflow overrides team config
    print(f" Merged config: {len(merged_config)} keys")
    
    # Step 5: Replace placeholders
    final_config = replace_placeholders(merged_config)
    print(f" Replaced placeholders in config")
    
    # Step 6: Load drive table template
    drive_template = load_json_file(drive_template_path)
    print(f" Loaded drive template: {len(drive_template)} keys")
    
    # Step 7: Update drive table with only existing keys
    drive_table_default_record = update_drive_table_keys_only(drive_template, final_config)
    
    # Step 8: Add drive_table_default_record to final config
    final_config["drive_table_default_record"] = drive_table_default_record
    
    print(f"Final config ready: {len(final_config)} keys")
    return final_config
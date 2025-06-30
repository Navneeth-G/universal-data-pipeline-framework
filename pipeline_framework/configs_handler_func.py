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
    """Get Airflow variables using host_name or return fallback"""
    try:
        from airflow.models import Variable
        
        env = Variable.get("env", default_var="dev")
        
        return {
            "env": env,
            # Elasticsearch using host_name
            "es_username": Variable.get(f"{host_name}_user"),
            "es_password": Variable.get(f"{host_name}_password"),
            "es_ca_certs_path": Variable.get(f"{host_name}_ca_certs_path"),
            "es_header": Variable.get(f"{host_name}_header"),
            # AWS using env
            "aws_access_key_id": Variable.get(f"{env}_s3_access_key_id"),
            "aws_secret_access_key": Variable.get(f"{env}_s3_secret_access_key"),
            "aws_region": Variable.get(f"{env}_s3_region"),
            "s3_bucket": Variable.get(f"{env}_s3_bucket"),
            # Snowflake using env
            "snowflake_user": Variable.get(f"{env}_snowflake_user"),
            "snowflake_password": Variable.get(f"{env}_snowflake_password"),
            "snowflake_account": Variable.get(f"{env}_snowflake_account"),
            "snowflake_warehouse": Variable.get(f"{env}_snowflake_warehouse"),
            "snowflake_role": Variable.get(f"{env}_snowflake_role")
        }
    except:
        print(" Airflow not available, using team config values")
        return {}

def update_drive_table_keys_only(drive_table: Dict[str, Any], updated_config: Dict[str, Any]) -> Dict[str, Any]:
    """Update drive table with only keys that exist in drive table"""
    updated_drive_table = drive_table.copy()
    
    # Only update keys that already exist in drive table
    for key in drive_table.keys():
        if key in updated_config:
            updated_drive_table[key] = updated_config[key]
    
    return updated_drive_table




def get_final_config(root_project_path: str, team_config_relative_path: str, drive_template_relative_path: str) -> Dict[str, Any]:
    """Simple config merging - Airflow variables always included"""
    
    # Build absolute paths
    team_config_path = os.path.join(root_project_path, team_config_relative_path)
    drive_template_path = os.path.join(root_project_path, drive_template_relative_path)
    
    # Step 1: Load name.json
    team_config = load_json_file(team_config_path)
    print(f" Loaded team config: {len(team_config)} keys")
    
    # Step 2: Get host_name and load Airflow variables
    host_name = team_config.get("host_name")
    if not host_name:
        raise ValueError("host_name not found in team config")
    
    airflow_vars = get_airflow_variables(host_name)
    print(f" Loaded Airflow variables: {len(airflow_vars)} keys for host: {host_name}")
    
    # Step 3: ALWAYS merge Airflow variables (union operation)
    updated_config = team_config.copy()
    
    # Add ALL Airflow variables, regardless of whether they exist in team config
    for key, value in airflow_vars.items():
        if value is not None and value != "":
            updated_config[key] = value  # Always add/override
    
    print(f" Updated config after Airflow merge: {len(updated_config)} keys")
    
    # Step 4: Load drive table template
    drive_template = load_json_file(drive_template_path)
    print(f" Loaded drive template: {len(drive_template)} keys")
    
    # Step 5: For drive table - only update keys that exist in template
    drive_table_default_record = update_drive_table_keys_only(drive_template, updated_config)
    
    # Step 6: Add drive_table_default_record to final config
    updated_config["drive_table_default_record"] = drive_table_default_record
    
    print(f" Final config ready: {len(updated_config)} keys")
    return updated_config
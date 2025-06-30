# pipeline_framework/simple_config_handler.py
import json
import os
from typing import Dict, Any
from airflow.models import Variable
from pathlib import Path


def load_json_file(file_path: str) -> Dict[str, Any]:
    """Load and validate JSON file"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    with open(file_path, 'r') as f:
        return json.load(f)

def get_airflow_variables(host_name: str) -> Dict[str, Any]:
    """Get ALL Airflow variables using host_name or return fallback"""
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
        "sf_role": Variable.get(f"snowflake_role")}

    print(f" Successfully loaded Airflow variables for host: {host_name}, env: {env}")
    return airflow_vars



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


def update_default_config_with_airflow_vars(default_config: Dict[str, Any]) -> Dict[str, Any]:

    es_hostname = default_config["es_hostname"]
    airflow_vars = get_airflow_variables(host_name=es_hostname)

    default_config.update(airflow_vars)

    updated_config = replace_placeholders(config=default_config)
    return updated_config


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
    root_project_path = Path(root_project_path.strip('/'))
    team_config_relative_path = Path(team_config_relative_path.strip('/'))
    drive_template_relative_path = Path(drive_template_relative_path.strip('/'))

    abs_config_path = root_project_path / team_config_relative_path
    abs_drive_template_path = str(root_project_path / drive_template_relative_path)

    # Step 1: Load team config
    team_config = load_json_file(abs_config_path)
    drive_config = load_json_file(abs_drive_template_path)
    print(f" Loaded team config: {len(team_config)} keys")

    updated_config = update_default_config_with_airflow_vars(team_config)

    updated_drive_config = update_drive_table_keys_only(drive_config, updated_config)

    final_config["drive_table_default_record"] = updated_drive_config
    
    print(f"Final config ready: {len(final_config)} keys")
    return final_config
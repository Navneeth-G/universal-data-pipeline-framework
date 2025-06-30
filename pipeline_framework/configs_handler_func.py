# pipeline_framework/configs_handler_func.py
import json
import os
from pathlib import Path
from typing import Dict, Any

class ConfigHandler:
    def __init__(self, root_project_path: str):
        self.root_project_path = Path(root_project_path).resolve()
        self.airflow_vars = {}
        self.drive_defaults = {}
        self.team_config = {}
        self.host_name = None  # ✅ Added back
        
    def load_team_config(self, team_config_relative_path: str) -> Dict[str, Any]:
        """Load team configuration (name.json) and extract host_name"""
        team_config_absolute_path = self.root_project_path / team_config_relative_path
        
        if not team_config_absolute_path.exists():
            raise FileNotFoundError(f"Team config not found: {team_config_absolute_path}")
            
        try:
            with open(team_config_absolute_path, 'r') as f:
                self.team_config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in team config: {e}")
        except Exception as e:
            raise RuntimeError(f"Error loading team config: {e}")
        
        # ✅ Extract host_name from team config
        self.host_name = self.team_config.get("host_name")
        if not self.host_name:
            raise ValueError("host_name not found in team config")
            
        print(f"✅ Team config loaded: {len(self.team_config)} keys, host_name: {self.host_name}")
        return self.team_config
    
    def load_drive_defaults(self, drive_defaults_relative_path: str) -> Dict[str, Any]:
        """Load drive table defaults template"""
        drive_defaults_absolute_path = self.root_project_path / drive_defaults_relative_path
        
        if not drive_defaults_absolute_path.exists():
            raise FileNotFoundError(f"Drive defaults not found: {drive_defaults_absolute_path}")
            
        try:
            with open(drive_defaults_absolute_path, 'r') as f:
                self.drive_defaults = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in drive defaults: {e}")
        except Exception as e:
            raise RuntimeError(f"Error loading drive defaults: {e}")
            
        # Validate required fields in template
        required_fields = ["source_name", "stage_name", "target_name", "pipeline_status"]
        missing_fields = [field for field in required_fields if field not in self.drive_defaults]
        if missing_fields:
            raise ValueError(f"Missing required fields in drive template: {missing_fields}")
            
        print(f"✅ Drive defaults loaded: {len(self.drive_defaults)} fields")
        return self.drive_defaults
        
    def load_airflow_variables(self) -> Dict[str, Any]:
        """Load variables from Airflow using host_name from team config"""
        if not self.host_name:
            raise ValueError("host_name not found. Load team config first.")
            
        try:
            from airflow.models import Variable
            
            env = Variable.get("env", default_var="dev")
            
            # ✅ Get environment-specific variables using extracted host_name
            airflow_vars = {
                "env": env,
                
                # Elasticsearch credentials using host_name from config
                "es_username": Variable.get(f"{self.host_name}_user"),
                "es_password": Variable.get(f"{self.host_name}_password"),
                "es_ca_certs_path": Variable.get(f"{self.host_name}_ca_certs_path"),
                "es_header": Variable.get(f"{self.host_name}_header"),
                
                # AWS S3 config
                "aws_access_key_id": Variable.get(f"{env}_s3_access_key_id"),
                "aws_secret_access_key": Variable.get(f"{env}_s3_secret_access_key"),
                "aws_region": Variable.get(f"{env}_s3_region"),
                "s3_bucket": Variable.get(f"{env}_s3_bucket"),
                
                # Snowflake credentials
                "snowflake_user": Variable.get(f"{env}_snowflake_user"),
                "snowflake_password": Variable.get(f"{env}_snowflake_password"),
                "snowflake_account": Variable.get(f"{env}_snowflake_account"),
                "snowflake_warehouse": Variable.get(f"{env}_snowflake_warehouse"),
                "snowflake_role": Variable.get(f"{env}_snowflake_role")
            }
            
            print(f"✅ Successfully loaded Airflow variables for host: {self.host_name}, env: {env}")
            
        except (ModuleNotFoundError, Exception) as e:
            print(f"⚠️  Airflow not available ({e}), using dev fallback credentials")
            
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
                "aws_region": "us-west-1",
                "s3_bucket": "dev-test-data-bucket",
                
                # Snowflake dev credentials  
                "snowflake_user": "dev_sf_user",
                "snowflake_password": "dev_sf_password",
                "snowflake_account": "dev_account.us-west-1",
                "snowflake_warehouse": "DEV_COMPUTE_WH",
                "snowflake_role": "DEV_DATA_LOADER_ROLE"
            }
            
        self.airflow_vars = airflow_vars
        return airflow_vars
    
    def merge_configs(self) -> Dict[str, Any]:
        """Simple merge: Drive Defaults > Team Config > Airflow"""
        
        if not self.drive_defaults:
            raise ValueError("Drive defaults not loaded. Call load_drive_defaults() first.")
        if not self.team_config:
            raise ValueError("Team config not loaded. Call load_team_config() first.")
        
        # Start with drive defaults template
        merged_config = self.drive_defaults.copy()
        
        # Update with team config values
        merged_config.update(self.team_config)
        
        # Update with airflow variables (highest priority)
        for key, value in self.airflow_vars.items():
            if value is not None and value != "":
                merged_config[key] = value
        
        # Create drive_table_default_record from merged values
        drive_record = {}
        for key in self.drive_defaults.keys():
            drive_record[key] = merged_config.get(key, self.drive_defaults[key])
        
        merged_config["drive_table_default_record"] = drive_record
        
        # Validate final config has required keys
        required_keys = ["timezone", "granularity", "x_time_back", "source_name", "target_name"]
        missing_keys = [key for key in required_keys if key not in merged_config]
        if missing_keys:
            raise ValueError(f"Missing required configuration keys: {missing_keys}")
        
        print(f"✅ Config merged successfully: {len(merged_config)} total keys")
        return merged_config

# Usage function
def get_final_config(root_project_path: str, drive_defaults_relative_path: str, team_config_relative_path: str) -> Dict[str, Any]:
    """Get final merged configuration"""
    try:
        handler = ConfigHandler(root_project_path)
        
        # ✅ Load team config first to get host_name
        handler.load_team_config(team_config_relative_path)
        handler.load_drive_defaults(drive_defaults_relative_path)
        handler.load_airflow_variables()  # Uses host_name from team config
        
        # Merge with priority: Airflow > Team > Drive Defaults
        final_config = handler.merge_configs()
        
        print(f"🎉 Configuration loading completed successfully!")
        return final_config
        
    except Exception as e:
        print(f"❌ Configuration loading failed: {e}")
        raise
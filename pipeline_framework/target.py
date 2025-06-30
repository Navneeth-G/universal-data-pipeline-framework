# pipeline_framework/target.py

"""
Target system operations interface.
Teams replace the import line with their specific target implementation.
"""

# TEAM CUSTOMIZATION: Replace this import with your target operations
from pipeline_framework.snowflake_operations import snowflake_count, snowflake_check_exists, snowflake_delete

from pipeline_framework.utils.log_generator import setup_pipeline_logger

log = setup_pipeline_logger(logger_name="Target")


def count(final_config, record):
    """Count records in target system for the given time window."""
    return snowflake_count(final_config, record)


def check_exists(final_config, record):
    """Check if data exists in target system for the given time window."""
    return snowflake_check_exists(final_config, record)


def delete(final_config, record):
    """Delete data from target system for the given time window."""
    return snowflake_delete(final_config, record)
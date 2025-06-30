# pipeline_framework/stage.py

"""
Stage system operations interface.
Teams replace the import line with their specific stage implementation.
"""

# TEAM CUSTOMIZATION: Replace this import with your stage operations
from pipeline_framework.s3_operations import s3_count, s3_check_exists, s3_delete

from pipeline_framework.utils.log_generator import setup_pipeline_logger

log = setup_pipeline_logger(logger_name="Stage")


def count(final_config, record):
    """Count records in stage system for the given time window."""
    return s3_count(final_config, record)


def check_exists(final_config, record):
    """Check if data exists in stage system for the given time window."""
    return s3_check_exists(final_config, record)


def delete(final_config, record):
    """Delete data from stage system for the given time window."""
    return s3_delete(final_config, record)
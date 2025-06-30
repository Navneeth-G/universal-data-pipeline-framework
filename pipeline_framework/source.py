# pipeline_framework/source.py

"""
Source system operations interface.
Teams replace the import line with their specific source implementation.
"""


from pipeline_framework.elasticsearch_operations import elasticsearch_count, elasticsearch_check_exists, elasticsearch_delete

from pipeline_framework.utils.log_generator import setup_pipeline_logger

log = setup_pipeline_logger(logger_name="Source")


def count(final_config, record):
    """Count records in source system for the given time window."""
    return elasticsearch_count(final_config, record)


def check_exists(final_config, record):
    """Check if data exists in source system for the given time window."""
    return elasticsearch_check_exists(final_config, record)

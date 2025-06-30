# pipeline_framework/audit.py

"""
Audit operations interface.
Teams replace the import line with their specific audit implementation.
"""

# TEAM CUSTOMIZATION: Replace this import with your audit implementation
from pipeline_framework.audit_operations import audit_data_transfer
from pipeline_framework.utils.time_utils import get_current_time_iso
from pipeline_framework.utils.log_generator import setup_pipeline_logger
from pipeline_framework.utils.time_utility import get_current_time_iso
from pipeline_framework.drive_record_adapter import update_record_in_drive_table

log = setup_pipeline_logger(logger_name="Audit")

def audit(final_config, record):
   """Audit data transfer with phase management."""
   try:       
       # Call team-specific audit implementation
       result = audit_data_transfer(final_config, record)
       
       if result:
           # Success is handled inside audit_data_transfer
           log.info("Audit completed successfully", pipeline_id=record['pipeline_id'])
       
       return result
       
   except Exception as e:
       # Reset audit phase on failure

       record.update({
           'audit_status': 'PENDING',
           'audit_start_time': None,
           'audit_end_time': None,
           'audit_result': None,
           'pipeline_status': 'PENDING',
           'pipeline_start_time': None,
           'pipeline_end_time': None,
           'dag_run_id': None,
           'retry_attempt': record.get('retry_attempt', 0) + 1
       })
       update_record_in_drive_table(record, final_config)
       log.exception("Audit failed", pipeline_id=record['pipeline_id'])
       raise




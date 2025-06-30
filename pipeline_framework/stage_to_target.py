# pipeline_framework/stage_to_target.py

"""
Stage to Target transfer interface.
Teams replace the import line with their specific transfer implementation.
"""
from pipeline_framework.s3_to_snowflake import transfer_s3_to_snowflake
from pipeline_framework.target import delete as delete_target
from pipeline_framework.utils.time_utility import get_current_time_iso
from pipeline_framework.utils.log_generator import setup_pipeline_logger
from pipeline_framework.drive_record_adapter import update_record_in_drive_table

log = setup_pipeline_logger(logger_name="StageToTarget")

def transfer(final_config, record):
    """Transfer data from stage to target with pre-cleanup, retry support, and consistent status tracking."""
    try:
        #  1. Skip if already completed
        if record.get('stage_to_target_ingestion_status') == 'COMPLETED':
            log.info("Stage to Target already marked COMPLETED. Skipping.", pipeline_id=record['pipeline_id'])
            return True

        #  2. Clean up target before starting new transfer
        delete_target(final_config, record)

        #  3. Run the transfer (no IN_PROGRESS update)
        result = transfer_s3_to_snowflake(final_config, record)

        if result:

            #  4. On success, mark completed
            record.update({
                'stage_to_target_ingestion_end_time': get_current_time_iso(final_config['timezone']),
                'stage_to_target_ingestion_status': 'COMPLETED',
                'completed_phase': 'STAGE_TO_TARGET',
                'record_last_updated_time': get_current_time_iso(final_config['timezone'])
            })

            update_record_in_drive_table(record, final_config)



            
  
            log.info("Stage to Target transfer completed successfully.", pipeline_id=record['pipeline_id'])
            return True
        else:
            raise RuntimeError("Stage to Target transfer returned failure result.")

    except Exception as e:
        log.exception("Stage to Target transfer failed", pipeline_id=record['pipeline_id'])

        try:
            #  5. Try to clean up again in case partial writes occurred
            delete_target(final_config, record)
        except Exception as cleanup_error:
            log.warning("Target cleanup after failure also failed", pipeline_id=record['pipeline_id'])

        #  6. Reset for retry
        record.update({
            'stage_to_target_ingestion_status': 'PENDING',
            'stage_to_target_ingestion_start_time': None,
            'stage_to_target_ingestion_end_time': None,
            'pipeline_start_time': None,
            'pipeline_status': 'PENDING',
            'dag_run_id': None,
            'retry_attempt': record.get('retry_attempt', 0) + 1,
            'record_last_updated_time': get_current_time_iso(final_config['timezone'])
        })

        update_record_in_drive_table(record, final_config)

        raise



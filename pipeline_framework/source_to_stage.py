# pipeline_framework/source_to_stage.py

"""
Source to Stage transfer interface.
Teams replace the import line with their specific transfer implementation.
"""

from pipeline_framework.utils.time_utils import get_current_time_iso
from pipeline_framework.stage import delete as delete_stage
from pipeline_framework.utils.time_utility import get_current_time_iso
from pipeline_framework.drive_record_adapter import update_record_in_drive_table
from pipeline_framework.elasticsearch_to_s3 import transfer_elasticsearch_to_s3
from pipeline_framework.utils.log_generator import setup_pipeline_logger

log = setup_pipeline_logger(logger_name="SourceToStage")

def transfer(final_config, record):
    """Transfer data from source to stage with cleanup, idempotency, and minimal writes."""
    try:
        #  1. Skip if already completed
        if record.get('SOURCE_TO_STAGE_INGESTION_STATUS') == 'COMPLETED':
            log.info("Source to Stage already marked COMPLETED. Skipping.", PIPELINE_ID=record['PIPELINE_ID'])
            return True
        record['SOURCE_TO_STAGE_INGESTION_START_TIME'] = get_current_time_iso(final_config['timezone'])

        #  2. Clean existing stage files before transfer
        delete_stage(final_config, record)

        #  3. Execute transfer (no intermediate status writes)
        result = transfer_elasticsearch_to_s3(final_config, record)

        if result:
            #  4. On success, mark completed
            record.update({
                'SOURCE_TO_STAGE_INGESTION_STATUS': 'COMPLETED',
                'COMPLETED_PHASE': 'SOURCE_TO_STAGE',
                'SOURCE_TO_STAGE_INGESTION_END_TIME': get_current_time_iso(final_config['timezone']),
                'RECORD_LAST_UPDATED_TIME': get_current_time_iso(final_config['timezone'])
            })
            # Update record in drive table
            update_record_in_drive_table(record, final_config)

            log.info("Source to Stage transfer completed successfully.", PIPELINE_ID=record['PIPELINE_ID'])
            return True
        else:
            raise RuntimeError("Elasticdump transfer returned failure result.")

    except Exception as e:
        log.exception("Source to Stage transfer failed", PIPELINE_ID=record['PIPELINE_ID'])

        try:
            #  5. Attempt to clean up again (e.g. if elasticdump partially wrote)
            delete_stage(final_config, record)
        except Exception as cleanup_error:
            log.warning("Stage cleanup after failure also failed", PIPELINE_ID=record['PIPELINE_ID'])

        #  6. Reset record for retry
        record.update({
            'SOURCE_TO_STAGE_INGESTION_STATUS': 'PENDING',
            'SOURCE_TO_STAGE_INGESTION_END_TIME': None,
            'SOURCE_TO_STAGE_INGESTION_START_TIME': None,
            'PIPELINE_START_TIME': None,
            'PIPELINE_STATUS': 'PENDING',
            'DAG_RUN_ID': None,
            'RETRY_ATTEMPT': record.get('RETRY_ATTEMPT', 0) + 1,
            'RECORD_LAST_UPDATED_TIME': get_current_time_iso(final_config['timezone'])
        })
        update_record_in_drive_table(record, final_config)


        raise





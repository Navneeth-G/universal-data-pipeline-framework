# pipeline_framework/elasticsearch_operations.py

from pipeline_framework.utils.time_utility import to_iso_string
from pipeline_framework.utils.time_utility import to_elasticsearch_format
from typing import Dict, Any
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import (
    ConnectionError as ESConnectionError,
    AuthenticationException,
    TransportError,
    RequestError
)

from pipeline_framework.utils.log_generator import setup_pipeline_logger

log = setup_pipeline_logger(logger_name="ElasticsearchOps")


def elasticsearch_connection(final_config: Dict[str, Any]) -> Elasticsearch:
    """Create and validate Elasticsearch connection."""
    try:
        es = Elasticsearch(
            [{
                'host': final_config['es_hostname'],
                'port': final_config['es_port']
            }],
            http_auth=(
                final_config['es_username'],
                final_config['es_password']
            )
        )

        # Test connection
        es.info()
        log.info("Elasticsearch connection successful")
        return es

    except AuthenticationException:
        log.error("Elasticsearch authentication failed", status="FAILED")
        raise

    except ESConnectionError as e:
        log.error(f"Elasticsearch connection failed: {e}", status="FAILED")
        raise

    except TransportError as e:
        log.error(f"Transport-level error while connecting to Elasticsearch: {e}", status="FAILED")
        raise

    except RequestError as e:
        log.error(f"Bad request when connecting to Elasticsearch: {e}", status="FAILED")
        raise

    except Exception as e:
        log.error(f"Unexpected Elasticsearch connection error: {e}", status="FAILED")
        raise



def elasticsearch_count(final_config: Dict[str, Any], record: Dict[str, Any]) -> int:
    """Count documents in Elasticsearch for given time window"""
    try:
        es = elasticsearch_connection(final_config)

        # Get config values
        es_format = final_config.get('es_format', 'iso')
        timezone = final_config.get('timezone', 'UTC')
        # Ensure timestamps are proper ISO strings
        start_time = to_elasticsearch_format(record['window_start_time'], es_format, timezone)
        end_time = to_elasticsearch_format(record['window_end_time'], es_format, timezone)

        # Build proper query with bool/must structure
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                final_config['elasticsearch_timestamp_field']: {
                                    "gte": start_time,
                                    "lt": end_time
                                }
                            }
                        }
                    ]
                }
            }
        }

        # Add additional filters if specified
        if 'elasticsearch_additional_filters' in final_config:
            query["query"]["bool"]["must"].extend(final_config['elasticsearch_additional_filters'])

        result = es.count(
            index=final_config['index_pattern'],
            body=query
        )
        count = result['count']

        log.info(f"Elasticsearch count: {count}", PIPELINE_ID=record['PIPELINE_ID'])
        return count

    except Exception as e:
        log.exception("Elasticsearch count failed", PIPELINE_ID=record['PIPELINE_ID'])
        raise

def elasticsearch_check_exists(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Check if data exists in Elasticsearch for time window"""
    try:
        count = elasticsearch_count(final_config, record)
        return count > 0
    except Exception as e:
        log.exception("Elasticsearch exists check failed", PIPELINE_ID=record['PIPELINE_ID'])
        raise







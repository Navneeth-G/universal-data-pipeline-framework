# pipeline_framework/utils/time_utility.py

"""
Centralized time utility to decouple time library dependencies.
All timestamps stored as ISO strings for safety.
Supports: pendulum, datetime, arrow, dateutil, pandas, numpy datetime
"""

import pendulum
from typing import Union, Optional, Any
from datetime import datetime, date
from utils.log_generator import setup_pipeline_logger

log = setup_pipeline_logger(logger_name="TimeUtility")


def get_current_time_iso(timezone: str = "UTC") -> str:
    """Get current time as ISO string in specified timezone"""
    try:
        return pendulum.now(timezone).to_iso8601_string()
    except Exception as e:
        log.warning(f"Timezone '{timezone}' failed, using UTC", error=str(e))
        return pendulum.now("UTC").to_iso8601_string()


def to_iso_string(time_input: Any, timezone: str = "UTC") -> str:
    """
    Convert any timestamp format to ISO string.
    Supports: pendulum, datetime, arrow, dateutil, pandas, numpy, string
    """
    if time_input is None:
        return None
    
    try:
        # Already a string - validate and return
        if isinstance(time_input, str):
            # Check if already ISO format
            if _is_iso_format(time_input):
                return time_input
            # Try parsing as date/time string
            return pendulum.parse(time_input, tz=timezone).to_iso8601_string()
        
        # Pendulum objects
        elif hasattr(time_input, 'to_iso8601_string'):
            return time_input.to_iso8601_string()
        
        # Python datetime/date objects
        elif isinstance(time_input, datetime):
            if time_input.tzinfo is None:
                # Naive datetime - assume timezone
                return pendulum.instance(time_input, tz=timezone).to_iso8601_string()
            else:
                # Timezone-aware datetime
                return pendulum.instance(time_input).to_iso8601_string()
        
        elif isinstance(time_input, date):
            # Date only - convert to start of day
            return pendulum.parse(str(time_input), tz=timezone).to_iso8601_string()
        
        # Arrow objects (if available)
        elif hasattr(time_input, 'isoformat') and hasattr(time_input, 'timestamp'):
            try:
                return time_input.isoformat()
            except:
                return pendulum.from_timestamp(time_input.timestamp(), tz=timezone).to_iso8601_string()
        
        # Pandas Timestamp objects
        elif hasattr(time_input, 'isoformat') and hasattr(time_input, 'tz'):
            return time_input.isoformat()
        
        # NumPy datetime64
        elif hasattr(time_input, 'astype') and 'datetime64' in str(type(time_input)):
            # Convert numpy datetime64 to timestamp
            timestamp = time_input.astype('datetime64[s]').astype('int')
            return pendulum.from_timestamp(timestamp, tz=timezone).to_iso8601_string()
        
        # Unix timestamp (int/float)
        elif isinstance(time_input, (int, float)):
            return pendulum.from_timestamp(time_input, tz=timezone).to_iso8601_string()
        
        # Generic objects with isoformat method
        elif hasattr(time_input, 'isoformat'):
            return time_input.isoformat()
        
        # Generic objects with strftime method
        elif hasattr(time_input, 'strftime'):
            return time_input.strftime('%Y-%m-%dT%H:%M:%S%z')
        
        # Last resort - try string conversion and parse
        else:
            time_str = str(time_input)
            return pendulum.parse(time_str, tz=timezone).to_iso8601_string()
            
    except Exception as e:
        log.error(
            f"Failed to convert timestamp to ISO string",
            input_type=str(type(time_input)),
            input_value=str(time_input),
            error_message=str(e)
        )
        raise ValueError(f"Cannot convert {type(time_input)} to ISO string: {e}")


def _is_iso_format(time_str: str) -> bool:
    """Check if string is already in ISO format"""
    try:
        # Basic ISO format checks
        if 'T' in time_str and (':' in time_str):
            # Try parsing to validate
            pendulum.parse(time_str)
            return True
        return False
    except:
        return False


def parse_to_iso(time_input: Any, timezone: str = "UTC") -> str:
    """Alias for to_iso_string for backward compatibility"""
    return to_iso_string(time_input, timezone)


def add_duration_to_iso(iso_time: str, seconds: int) -> str:
    """Add seconds to ISO time string, return ISO string"""
    dt = pendulum.parse(iso_time)
    return dt.add(seconds=seconds).to_iso8601_string()


def subtract_duration_from_iso(iso_time: str, seconds: int) -> str:
    """Subtract seconds from ISO time string, return ISO string"""
    dt = pendulum.parse(iso_time)
    return dt.subtract(seconds=seconds).to_iso8601_string()


def get_start_of_day_iso(date_input: Any, timezone: str = "UTC") -> str:
    """Get start of day (00:00:00) as ISO string"""
    iso_time = to_iso_string(date_input, timezone)
    dt = pendulum.parse(iso_time)
    return dt.start_of('day').to_iso8601_string()


def get_end_of_day_iso(date_input: Any, timezone: str = "UTC") -> str:
    """Get end of day (next day 00:00:00) as ISO string"""
    iso_time = to_iso_string(date_input, timezone)
    dt = pendulum.parse(iso_time)
    return dt.add(days=1).start_of('day').to_iso8601_string()


def get_date_only(time_input: Any) -> str:
    """Extract date portion (YYYY-MM-DD) from any timestamp"""
    iso_time = to_iso_string(time_input)
    return pendulum.parse(iso_time).date().isoformat()


def calculate_duration_seconds(start_time: Any, end_time: Any) -> int:
    """Calculate duration in seconds between two timestamps"""
    start_iso = to_iso_string(start_time)
    end_iso = to_iso_string(end_time)
    
    start_dt = pendulum.parse(start_iso)
    end_dt = pendulum.parse(end_iso)
    return int((end_dt - start_dt).total_seconds())


def compare_times(time1: Any, time2: Any) -> int:
    """Compare two timestamps. Returns: -1 (time1 < time2), 0 (equal), 1 (time1 > time2)"""
    iso1 = to_iso_string(time1)
    iso2 = to_iso_string(time2)
    
    dt1 = pendulum.parse(iso1)
    dt2 = pendulum.parse(iso2)
    
    if dt1 < dt2:
        return -1
    elif dt1 > dt2:
        return 1
    else:
        return 0


def is_timezone_aware(time_input: Any) -> bool:
    """Check if timestamp has timezone information"""
    try:
        iso_time = to_iso_string(time_input)
        dt = pendulum.parse(iso_time)
        return dt.tzinfo is not None
    except:
        return False


def convert_timezone(time_input: Any, target_timezone: str) -> str:
    """Convert timestamp to different timezone, return ISO string"""
    iso_time = to_iso_string(time_input)
    dt = pendulum.parse(iso_time)
    return dt.in_timezone(target_timezone).to_iso8601_string()


def format_for_display(time_input: Any, timestamp_format_str: str = "%Y-%m-%d %H:%M:%S %Z") -> str:
    """Format timestamp for human-readable display"""
    iso_time = to_iso_string(time_input)
    dt = pendulum.parse(iso_time)
    return dt.strftime(timestamp_format_str)


def to_elasticsearch_format(time_input: Any, es_format: str = "iso", timezone: str = "UTC") -> str:
    """
    Convert timestamp to Elasticsearch-compatible format
    
    Args:
        time_input: Any timestamp format
        es_format: 'iso', 'epoch_ms', 'epoch_s', 'custom_pdt'
        timezone: Target timezone for conversion
    """
    try:
        iso = to_iso_string(time_input, timezone)
        dt = pendulum.parse(iso)
        
        if es_format == "iso":
            return iso
        elif es_format == "epoch_ms":
            return str(int(dt.timestamp() * 1000))
        elif es_format == "epoch_s":
            return str(int(dt.timestamp()))
        elif es_format == "custom_pdt":
            return dt.format("%Y-%m-%d %H:%M:%S%Z")
        else:
            log.warning(f"Unknown ES format '{es_format}', defaulting to ISO")
            return iso
            
    except Exception as e:
        log.error(f"Failed to convert to ES format: {e}")
        raise ValueError(f"Cannot convert to ES format {es_format}: {e}")




def get_current_epoch_time(timezone: str = "UTC") -> int:
    """Get current time as epoch seconds in specified timezone"""
    try:
        return int(pendulum.now(timezone).timestamp())
    except Exception as e:
        log.error(f"Failed to get epoch time for timezone '{timezone}': {e}")
        # Fallback to UTC
        return int(pendulum.now("UTC").timestamp())






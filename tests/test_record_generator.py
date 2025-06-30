# test_record_generator.py

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'pipeline_framework'))

from rich.table import Table
from rich import print as rprint
from records_generation.record_generator import record_generator
from utils.time_utility import to_iso_string, get_current_time_iso

# Your utility functions
def print_records_with_highlight(records, highlight_keys):
    if not records:
        rprint("[red]No records to display[/red]")
        return
    table = Table(show_header=True, header_style="bold cyan")
    for key in records[0].keys():
        table.add_column(key)
    for record in records:
        row = []
        for key in record:
            val = str(record[key])
            if key in highlight_keys:
                val = f"[green]{val}[/green]"
            row.append(val)
        table.add_row(*row)
    rprint(table)

# Mock context for Airflow
class MockTaskInstance:
    def xcom_push(self, key, value):
        print(f"XCom Push - {key}: {value}")

class MockContext:
    def __init__(self):
        self.task_instance = MockTaskInstance()

# Updated final_config with new schema
final_config = {
    "x_time_back": "1d",
    "granularity": "2h", 
    "timezone": "America/New_York",
    "drive_table_default_record": {
        "source_id": None,
        "source_name": "elasticsearch",
        "source_category": "search_engine",
        "source_sub_category": "document_store",
        
        "stage_id": None,
        "stage_name": "aws_s3",
        "stage_category": "cloud_storage",
        "stage_sub_category": "object_storage",
        
        "target_id": None,
        "target_name": "snowflake",
        "target_category": "data_warehouse",
        "target_sub_category": "cloud_warehouse",
        
        "source_to_stage_ingestion_start_time": None,
        "source_to_stage_ingestion_end_time": None,
        "source_to_stage_ingestion_status": "PENDING",
        
        "stage_to_target_ingestion_start_time": None,
        "stage_to_target_ingestion_end_time": None,
        "stage_to_target_ingestion_status": "PENDING",
        
        "audit_start_time": None,
        "audit_end_time": None,
        "audit_status": "PENDING",
        
        "pipeline_id": None,
        "pipeline_start_time": None,
        "pipeline_end_time": None,
        "pipeline_status": "PENDING",
        
        "dag_run_id": None,
        "audit_result": None,
        "completed_phase": None,
        "retry_attempt": 0,
        "granularity": None,
        "window_start_time": None,
        "window_end_time": None,
        "target_day": None,
        "miscellaneous": None,
        "record_first_created_time": None,
        "record_last_updated_time": None
    }
}

# Test data for existing records (using ISO strings)
existing_records_sample = [
    {
        'window_start_time': '2025-06-27T02:00:00-04:00',
        'window_end_time': '2025-06-27T04:00:00-04:00',
        'pipeline_id': 'abc123_test',
        'source_id': 'src_test_1',
        'stage_id': 'stg_test_1',
        'target_id': 'tgt_test_1'
    },
    {
        'window_start_time': '2025-06-27T04:00:00-04:00', 
        'window_end_time': '2025-06-27T06:00:00-04:00',
        'pipeline_id': 'abc123_test',
        'source_id': 'src_test_2',
        'stage_id': 'stg_test_2',
        'target_id': 'tgt_test_2'
    }
]

def test_case_1():
    """Test Case 1: No existing records"""
    print("\n=== TEST CASE 1: No Existing Records ===")
    
    import records_generation.record_generator as rg
    rg.get_existing_records = lambda config, day: []
    
    created_records = []
    
    def mock_insert(record, config):
        # Convert any non-string timestamps to ISO for display
        display_record = record.copy()
        for key, value in display_record.items():
            if key.endswith('_time') and value is not None:
                display_record[key] = to_iso_string(value)
        created_records.append(display_record)
        print(f"✅ Record inserted: {record['pipeline_id']}")
    
    rg.insert_record = mock_insert
    
    context = MockContext()
    result = record_generator(final_config, **{'task_instance': context.task_instance})
    
    print(f"Return value: {result}")
    print_records_with_highlight(created_records, ['window_start_time', 'window_end_time', 'pipeline_id', 'granularity', 'source_id', 'stage_id', 'target_id'])

def test_case_2():
    """Test Case 2: Existing records present"""
    print("\n=== TEST CASE 2: Existing Records Present ===")
    
    import records_generation.record_generator as rg
    rg.get_existing_records = lambda config, day: existing_records_sample
    
    created_records = []
    
    def mock_insert(record, config):
        # Convert any non-string timestamps to ISO for display
        display_record = record.copy()
        for key, value in display_record.items():
            if key.endswith('_time') and value is not None:
                display_record[key] = to_iso_string(value)
        created_records.append(display_record)
        print(f"✅ Record inserted: {record['pipeline_id']}")
    
    rg.insert_record = mock_insert
    
    context = MockContext()
    result = record_generator(final_config, **{'task_instance': context.task_instance})
    
    print(f"Return value: {result}")
    print("\nExisting records:")
    print_records_with_highlight(existing_records_sample, ['window_start_time', 'window_end_time', 'pipeline_id'])
    print("\nNew record created:")
    print_records_with_highlight(created_records, ['window_start_time', 'window_end_time', 'pipeline_id', 'granularity', 'source_id', 'stage_id', 'target_id'])

def test_boundary_cases():
    """Test corner cases with boundary conditions"""
    
    # Test Case 3: Boundary capping
    print("\n=== TEST CASE 3: Boundary Capping ===")
    
    boundary_records = [
        {
            'window_start_time': '2025-06-27T22:00:00-04:00',
            'window_end_time': '2025-06-27T22:00:00-04:00',
            'pipeline_id': 'boundary_test_1',
            'source_id': 'src_boundary',
            'stage_id': 'stg_boundary',
            'target_id': 'tgt_boundary'
        }
    ]
    
    boundary_config = final_config.copy()
    boundary_config['granularity'] = '4h'
    
    test_with_config_and_records(boundary_config, boundary_records, "Boundary Capping")
    
    # Test Case 4: Very short remaining time  
    print("\n=== TEST CASE 4: Short Remaining Time ===")
    
    short_records = [
        {
            'window_start_time': '2025-06-27T23:30:00-04:00',
            'window_end_time': '2025-06-27T23:30:00-04:00',
            'pipeline_id': 'short_test_1',
            'source_id': 'src_short',
            'stage_id': 'stg_short',
            'target_id': 'tgt_short'
        }
    ]
    
    short_config = final_config.copy()
    short_config['granularity'] = '2h'
    
    test_with_config_and_records(short_config, short_records, "Short Time")

def test_with_config_and_records(config, existing_records, test_name):
    """Helper function to test with specific config and records"""
    import records_generation.record_generator as rg
    rg.get_existing_records = lambda cfg, day: existing_records
    
    created_records = []
    
    def mock_insert(record, cfg):
        # Convert any non-string timestamps to ISO for display
        display_record = record.copy()
        for key, value in display_record.items():
            if key.endswith('_time') and value is not None:
                display_record[key] = to_iso_string(value)
        created_records.append(display_record)
        print(f"✅ Record inserted: {record['pipeline_id']}")
    
    rg.insert_record = mock_insert
    
    context = MockContext()
    result = record_generator(config, **{'task_instance': context.task_instance})
    
    print(f"Return value: {result}")
    print(f"\n{test_name} - Existing records:")
    print_records_with_highlight(existing_records, ['window_start_time', 'window_end_time', 'pipeline_id'])
    print(f"\n{test_name} - New record:")
    print_records_with_highlight(created_records, ['window_start_time', 'window_end_time', 'granularity', 'source_id', 'stage_id', 'target_id'])

if __name__ == "__main__":
    test_case_1()
    test_case_2()
    test_boundary_cases()
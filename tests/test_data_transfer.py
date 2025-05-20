"""Tests for the data transfer module."""

import os
import pytest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock
import subprocess
import importlib.resources

from iseries_connector.data_transfer import (
    DataTransferConfig,
    DataTransferManager,
    DataTransferResult,
    ConfigurationError,
    ValidationError
)

@pytest.fixture
def mock_acs_launcher():
    """Mock the ACS Launcher path validation."""
    with patch('os.path.exists', return_value=True):
        yield

@pytest.fixture
def config(mock_acs_launcher):
    """Create a test configuration."""
    return DataTransferConfig(
        host_name="test.hostname.com",
        database="*SYSBAS",
        acs_launcher_path="C:/test/acslaunch_win-64.exe",
        batch_size=2
    )

@pytest.fixture
def dtm(config):
    """Create a test data transfer manager."""
    return DataTransferManager(config)

def test_config_validation(mock_acs_launcher):
    """Test configuration validation."""
    # Test missing host name
    with pytest.raises(ConfigurationError):
        DataTransferConfig(host_name="")
    
    # Test invalid ACS launcher path
    with patch('os.path.exists', return_value=False):
        with pytest.raises(ConfigurationError):
            DataTransferConfig(host_name="test", acs_launcher_path="invalid/path")

def test_config_default_paths(config):
    """Test default path configuration."""
    assert config.local_raw_data_directory.endswith("raw_data")
    assert config.local_data_package_directory.endswith("data_package")

def test_get_template_content(dtm, tmp_path):
    """Test template content retrieval."""
    # Test with custom template
    custom_template = """
    [DataTransferFrom]
    DataTransferVersion=1.0
    [HostInfo]
    Database=*SYSBAS
    HostFile={{source_schema}}/{{source_table}}
    HostName={{host_name}}
    [ClientInfo]
    OutputDevice=2
    FileEncoding=UTF-8
    ClientFile={{local_raw_data_directory}}/{{source_schema}}_{{source_table}}.csv
    """
    template_path = tmp_path / "test_template.txt"
    template_path.write_text(custom_template)
    
    dtm.config.template_path = str(template_path)
    content = dtm._get_template_content()
    assert "HostName={{host_name}}" in content
    assert "Database=*SYSBAS" in content
    
    # Test with built-in template
    dtm.config.template_path = None
    content = dtm._get_template_content()
    assert "HostName={{host_name}}" in content
    assert "Database=*SYSBAS" in content
    assert "DataTransferVersion=1.0" in content
    assert "FileEncoding=UTF-8" in content

def test_create_dtfx_file(dtm, tmp_path):
    """Test DTFX file creation."""
    # Create test template
    template_path = tmp_path / "test_template.txt"
    template_path.write_text("""
    [DataTransferFrom]
    DataTransferVersion=1.0
    [HostInfo]
    Database={{database}}
    HostFile={{source_schema}}/{{source_table}}
    HostName={{host_name}}
    [ClientInfo]
    OutputDevice=2
    FileEncoding=UTF-8
    ClientFile={{local_raw_data_directory}}/{{source_schema}}_{{source_table}}.csv
    [SQL]
    SQLSelect={{sql_statement}}
    """)
    
    dtm.config.template_path = str(template_path)
    
    # Create DTFX file
    output_path = tmp_path / "test.dtfx"
    dtm._create_dtfx_file(
        host_name="test.hostname.com",
        source_schema="TEST",
        source_table="TABLE",
        sql_statement="SELECT * FROM TEST.TABLE",
        output_path=str(output_path)
    )
    
    # Verify file contents
    content = output_path.read_text()
    assert "HostName=test.hostname.com" in content
    assert "Database={{database}}" in content  # Template variable should remain unchanged
    assert "HostFile=TEST/TABLE" in content
    assert "SELECT * FROM TEST.TABLE" in content
    assert "FileEncoding=UTF-8" in content
    assert "DataTransferVersion=1.0" in content

@patch('subprocess.run')
def test_transfer_data(mock_run, dtm, tmp_path):
    """Test data transfer execution."""
    # Mock successful subprocess run
    mock_result = MagicMock()
    mock_result.stdout = "Successfully transferred 100 rows"
    mock_result.returncode = 0
    mock_run.return_value = mock_result
    
    # Execute transfer
    result = dtm.transfer_data(
        host_name="test.hostname.com",
        source_schema="TEST",
        source_table="TABLE",
        sql_statement="SELECT * FROM TEST.TABLE",
        output_directory=str(tmp_path)
    )
    
    # Verify result
    assert result.is_successful
    assert result.row_count == 100
    assert result.success
    assert result.error is None
    assert result.file_path.endswith("TEST_TABLE.csv")
    
    # Verify command was called correctly
    mock_run.assert_called_once()
    call_args = mock_run.call_args[0][0]
    assert "start" in call_args
    assert "acslaunch_win-64.exe" in call_args
    assert "download" in call_args
    assert ".dtfx" in call_args

@patch('subprocess.run')
def test_transfer_data_failure(mock_run, dtm, tmp_path):
    """Test data transfer failure."""
    # Mock failed subprocess run
    mock_run.side_effect = subprocess.CalledProcessError(
        returncode=1,
        cmd="test",
        stderr="Error: Connection failed"
    )
    
    # Execute transfer
    result = dtm.transfer_data(
        host_name="test.hostname.com",
        source_schema="TEST",
        source_table="TABLE",
        sql_statement="SELECT * FROM TEST.TABLE",
        output_directory=str(tmp_path)
    )
    
    # Verify result
    assert not result.is_successful
    assert result.row_count is None
    assert not result.success
    assert "Error: Connection failed" in result.error

def test_transfer_multiple(dtm, tmp_path):
    """Test multiple data transfers."""
    transfers = [
        {
            'host_name': 'test1.hostname.com',
            'source_schema': 'TEST1',
            'source_table': 'TABLE1',
            'sql_statement': 'SELECT * FROM TEST1.TABLE1'
        },
        {
            'host_name': 'test2.hostname.com',
            'source_schema': 'TEST2',
            'source_table': 'TABLE2',
            'sql_statement': 'SELECT * FROM TEST2.TABLE2'
        },
        {
            'host_name': 'test3.hostname.com',
            'source_schema': 'TEST3',
            'source_table': 'TABLE3',
            'sql_statement': 'SELECT * FROM TEST3.TABLE3'
        }
    ]
    
    # Mock transfer_data to avoid actual execution
    with patch.object(dtm, 'transfer_data') as mock_transfer:
        mock_transfer.return_value = DataTransferResult(
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration=1.0,
            row_count=100,
            output="Success",
            success=True
        )
        
        # Execute transfers
        results = list(dtm.transfer_multiple(transfers, str(tmp_path)))
        
        # Verify results
        assert len(results) == 3
        assert all(r.is_successful for r in results)
        assert mock_transfer.call_count == 3
        
        # Verify host names were passed correctly
        calls = mock_transfer.call_args_list
        assert calls[0][1]['host_name'] == 'test1.hostname.com'
        assert calls[1][1]['host_name'] == 'test2.hostname.com'
        assert calls[2][1]['host_name'] == 'test3.hostname.com' 
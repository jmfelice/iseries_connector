"""Tests for the data transfer module."""

import os
import pytest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock
import subprocess
import importlib.resources
from typing import List
import shutil

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
def temp_dirs(tmp_path):
    """Create and cleanup temporary directories for testing."""
    # Create temporary directories
    raw_data_dir = tmp_path / "raw_data"
    data_package_dir = tmp_path / "data_package"
    raw_data_dir.mkdir()
    data_package_dir.mkdir()
    
    yield raw_data_dir, data_package_dir
    
    # Cleanup after tests
    if raw_data_dir.exists():
        shutil.rmtree(raw_data_dir)
    if data_package_dir.exists():
        shutil.rmtree(data_package_dir)

@pytest.fixture
def config(mock_acs_launcher, temp_dirs):
    """Create a test configuration."""
    raw_data_dir, data_package_dir = temp_dirs
    return DataTransferConfig(
        host_name="test.hostname.com",
        database="*SYSBAS",
        acs_launcher_path="C:/test/acslaunch_win-64.exe",
        batch_size=2,
        local_raw_data_directory=str(raw_data_dir),
        local_data_package_directory=str(data_package_dir)
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

def test_config_default_paths(tmp_path):
    """Test default path configuration."""
    with patch('pathlib.Path.cwd', return_value=tmp_path):
        config = DataTransferConfig(host_name="test")
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

@patch('subprocess.Popen')
def test_transfer_data_single(mock_popen, dtm, temp_dirs):
    """Test single data transfer execution."""
    raw_data_dir, _ = temp_dirs
    
    # Mock successful subprocess
    mock_process = MagicMock()
    mock_process.communicate.return_value = ("Successfully transferred 100 rows", "")
    mock_process.returncode = 0
    mock_popen.return_value = mock_process
    
    # Execute transfer
    results = list(dtm.transfer_data(
        source_schema="TEST",
        source_table="TABLE",
        sql_statement="SELECT * FROM TEST.TABLE",
        output_directory=str(raw_data_dir)
    ))
    
    # Verify result
    assert len(results) == 1
    result = results[0]
    assert result.is_successful
    assert result.row_count == 100
    assert result.success
    assert result.error is None
    assert result.file_path.endswith("TEST_TABLE.csv")
    
    # Verify command was called correctly
    mock_popen.assert_called_once()
    call_args = mock_popen.call_args[0][0]
    assert "start" in call_args
    assert "acslaunch_win-64.exe" in call_args
    assert "download" in call_args
    assert ".dtfx" in call_args

@patch('subprocess.Popen')
def test_transfer_data_batch(mock_popen, dtm, temp_dirs):
    """Test batch data transfer execution."""
    raw_data_dir, _ = temp_dirs
    
    # Mock successful subprocess
    mock_process = MagicMock()
    mock_process.communicate.return_value = ("Successfully transferred 100 rows", "")
    mock_process.returncode = 0
    mock_popen.return_value = mock_process
    
    # Execute batch transfer
    schemas = ["TEST1", "TEST2", "TEST3"]
    tables = ["TABLE1", "TABLE2", "TABLE3"]
    sql_statements = [
        "SELECT * FROM TEST1.TABLE1",
        "SELECT * FROM TEST2.TABLE2",
        "SELECT * FROM TEST3.TABLE3"
    ]
    
    results = list(dtm.transfer_data(
        source_schema=schemas,
        source_table=tables,
        sql_statement=sql_statements,
        output_directory=str(raw_data_dir)
    ))
    
    # Verify results
    assert len(results) == 3
    assert all(r.is_successful for r in results)
    assert all(r.row_count == 100 for r in results)
    assert mock_popen.call_count == 3

@patch('subprocess.Popen')
def test_transfer_data_failure(mock_popen, dtm, temp_dirs):
    """Test data transfer failure."""
    raw_data_dir, _ = temp_dirs
    
    # Mock failed subprocess
    mock_process = MagicMock()
    mock_process.communicate.return_value = ("", "Error: Connection failed")
    mock_process.returncode = 1
    mock_popen.return_value = mock_process
    
    # Execute transfer
    results = list(dtm.transfer_data(
        source_schema="TEST",
        source_table="TABLE",
        sql_statement="SELECT * FROM TEST.TABLE",
        output_directory=str(raw_data_dir)
    ))
    
    # Verify result
    assert len(results) == 1
    result = results[0]
    assert not result.is_successful
    assert result.row_count is None
    assert not result.success
    assert "Error: Connection failed" in result.error

def test_transfer_data_validation(dtm):
    """Test transfer data validation."""
    # Test mismatched list lengths
    with pytest.raises(ValidationError):
        list(dtm.transfer_data(
            source_schema=["TEST1", "TEST2"],
            source_table=["TABLE1"],
            sql_statement=["SELECT * FROM TEST1.TABLE1"]
        ))

@patch('subprocess.Popen')
def test_transfer_data_custom_output(mock_popen, dtm, temp_dirs):
    """Test data transfer with custom output directory."""
    raw_data_dir, _ = temp_dirs
    
    # Mock successful subprocess
    mock_process = MagicMock()
    mock_process.communicate.return_value = ("Successfully transferred 100 rows", "")
    mock_process.returncode = 0
    mock_popen.return_value = mock_process
    
    # Create custom output directory within temp directory
    custom_output = raw_data_dir / "custom_output"
    custom_output.mkdir()
    
    # Execute transfer
    results = list(dtm.transfer_data(
        source_schema="TEST",
        source_table="TABLE",
        sql_statement="SELECT * FROM TEST.TABLE",
        output_directory=str(custom_output)
    ))
    
    # Verify result
    assert len(results) == 1
    result = results[0]
    assert result.is_successful
    assert str(custom_output) in result.file_path
    assert result.file_path.endswith("TEST_TABLE.csv")

@patch('subprocess.Popen')
def test_transfer_data_batch_size(mock_popen, dtm, temp_dirs):
    """Test data transfer with batch size limits."""
    raw_data_dir, _ = temp_dirs
    
    # Mock successful subprocess
    mock_process = MagicMock()
    mock_process.communicate.return_value = ("Successfully transferred 100 rows", "")
    mock_process.returncode = 0
    mock_popen.return_value = mock_process
    
    # Create 5 transfers with batch size of 2
    dtm.config.batch_size = 2
    schemas = ["TEST1", "TEST2", "TEST3", "TEST4", "TEST5"]
    tables = ["TABLE1", "TABLE2", "TABLE3", "TABLE4", "TABLE5"]
    sql_statements = [f"SELECT * FROM {s}.{t}" for s, t in zip(schemas, tables)]
    
    results = list(dtm.transfer_data(
        source_schema=schemas,
        source_table=tables,
        sql_statement=sql_statements,
        output_directory=str(raw_data_dir)
    ))
    
    # Verify results
    assert len(results) == 5
    assert all(r.is_successful for r in results)
    assert mock_popen.call_count == 5  # Should be called 5 times total 
"""Tests for the data transfer functionality."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
from datetime import datetime

from iseries_connector.data_transfer import (
    DataTransferConfig,
    DataTransferManager,
    DataTransferResult,
    ValidationError
)

@pytest.fixture
def temp_dirs():
    """Create temporary directories for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        raw_data_dir = Path(temp_dir) / "raw_data"
        data_package_dir = Path(temp_dir) / "data_package"
        raw_data_dir.mkdir()
        data_package_dir.mkdir()
        yield raw_data_dir, data_package_dir

@pytest.fixture
def mock_acs_launcher():
    """Create a mock ACS launcher path."""
    return str(Path.cwd() / "mock_acs_launcher.exe")

@pytest.fixture
def mock_file_exists():
    """Mock file existence checks."""
    with patch('os.path.exists') as mock_exists:
        mock_exists.return_value = True
        yield mock_exists

@pytest.fixture
def config(temp_dirs, mock_acs_launcher, mock_file_exists):
    """Create a test configuration."""
    raw_data_dir, data_package_dir = temp_dirs
    return DataTransferConfig(
        host_name="test.host.com",
        acs_launcher_path=mock_acs_launcher,
        local_raw_data_directory=str(raw_data_dir),
        local_data_package_directory=str(data_package_dir)
    )

class TestDataTransferConfig:
    """Test cases for DataTransferConfig class."""

    def test_init_with_defaults(self, mock_file_exists):
        """Test initialization with default values."""
        config = DataTransferConfig(host_name="test.host.com")
        assert config.host_name == "test.host.com"
        assert config.database == "*SYSBAS"
        assert config.batch_size == 15
        assert config.template_path is None

    def test_init_with_custom_values(self, temp_dirs, mock_file_exists):
        """Test initialization with custom values."""
        raw_data_dir, data_package_dir = temp_dirs
        config = DataTransferConfig(
            host_name="test.host.com",
            database="TESTDB",
            batch_size=10,
            local_raw_data_directory=str(raw_data_dir),
            local_data_package_directory=str(data_package_dir)
        )
        assert config.host_name == "test.host.com"
        assert config.database == "TESTDB"
        assert config.batch_size == 10
        assert config.local_raw_data_directory == str(raw_data_dir)
        assert config.local_data_package_directory == str(data_package_dir)

    def test_from_env(self, monkeypatch, mock_file_exists):
        """Test creating config from environment variables."""
        monkeypatch.setenv('ISERIES_HOST_NAME', 'env.host.com')
        monkeypatch.setenv('ISERIES_DATABASE', 'ENVDB')
        monkeypatch.setenv('ISERIES_BATCH_SIZE', '20')
        
        config = DataTransferConfig.from_env()
        assert config.host_name == 'env.host.com'
        assert config.database == 'ENVDB'
        assert config.batch_size == 20

    def test_validate_missing_host(self, mock_file_exists):
        """Test validation with missing host name."""
        with pytest.raises(ValidationError, match="Host name is required"):
            DataTransferConfig(host_name="")

    def test_validate_invalid_acs_path(self, mock_file_exists):
        """Test validation with invalid ACS launcher path."""
        mock_file_exists.return_value = False
        with pytest.raises(ValidationError, match="ACS Launcher not found"):
            DataTransferConfig(
                host_name="test.host.com",
                acs_launcher_path="nonexistent.exe"
            )

    def test_validate_invalid_batch_size(self, mock_file_exists):
        """Test validation with invalid batch size."""
        with pytest.raises(ValidationError, match="Batch size must be a positive number"):
            DataTransferConfig(
                host_name="test.host.com",
                batch_size=0
            )

    def test_validate_invalid_template_path(self, temp_dirs, mock_file_exists):
        """Test validation with invalid template path."""
        mock_file_exists.return_value = False
        with pytest.raises(ValidationError, match="Template file not found"):
            DataTransferConfig(
                host_name="test.host.com",
                template_path="nonexistent.txt"
            )

class TestDataTransferManager:
    """Test cases for DataTransferManager class."""

    @pytest.fixture
    def manager(self, config, mock_file_exists):
        """Create a test manager instance."""
        return DataTransferManager(
            host_name=config.host_name,
            acs_launcher_path=config.acs_launcher_path,
            local_raw_data_directory=config.local_raw_data_directory,
            local_data_package_directory=config.local_data_package_directory
        )

    def test_init_with_config(self, config, mock_file_exists):
        """Test initialization with config object."""
        manager = DataTransferManager(
            host_name=config.host_name,
            acs_launcher_path=config.acs_launcher_path,
            local_raw_data_directory=config.local_raw_data_directory,
            local_data_package_directory=config.local_data_package_directory
        )
        assert manager.config.host_name == config.host_name
        assert manager.config.acs_launcher_path == config.acs_launcher_path

    @patch('subprocess.Popen')
    def test_transfer_data_single(self, mock_popen, manager, temp_dirs, mock_file_exists):
        """Test single data transfer."""
        raw_data_dir, _ = temp_dirs
        
        # Mock successful process
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.communicate.return_value = (
            "Successfully transferred 100 rows",
            ""
        )
        mock_popen.return_value = mock_process

        result = next(manager.transfer_data(
            source_schema="TEST",
            source_table="TABLE",
            sql_statement="SELECT * FROM TEST.TABLE"
        ))

        assert result.is_successful
        assert result.row_count == 100
        assert result.file_path == str(raw_data_dir / "TEST_TABLE.csv")

    @patch('subprocess.Popen')
    def test_transfer_data_batch(self, mock_popen, manager, temp_dirs, mock_file_exists):
        """Test batch data transfer."""
        raw_data_dir, _ = temp_dirs
        
        # Mock successful processes
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.communicate.return_value = (
            "Successfully transferred 100 rows",
            ""
        )
        mock_popen.return_value = mock_process

        schemas = ["TEST1", "TEST2"]
        tables = ["TABLE1", "TABLE2"]
        sql_statements = [
            "SELECT * FROM TEST1.TABLE1",
            "SELECT * FROM TEST2.TABLE2"
        ]

        results = list(manager.transfer_data(
            source_schema=schemas,
            source_table=tables,
            sql_statement=sql_statements
        ))

        assert len(results) == 2
        assert all(r.is_successful for r in results)
        assert results[0].file_path == str(raw_data_dir / "TEST1_TABLE1.csv")
        assert results[1].file_path == str(raw_data_dir / "TEST2_TABLE2.csv")

    @patch('subprocess.Popen')
    def test_transfer_data_failure(self, mock_popen, manager, mock_file_exists):
        """Test failed data transfer."""
        # Mock failed process
        mock_process = MagicMock()
        mock_process.returncode = 1
        mock_process.communicate.return_value = (
            "",
            "Error: Connection failed"
        )
        mock_popen.return_value = mock_process

        result = next(manager.transfer_data(
            source_schema="TEST",
            source_table="TABLE",
            sql_statement="SELECT * FROM TEST.TABLE"
        ))

        assert not result.is_successful
        assert result.error == "Error: Connection failed"

    @patch('subprocess.Popen')
    def test_transfer_data_custom_output(self, mock_popen, manager, temp_dirs, mock_file_exists):
        """Test data transfer with custom output directory."""
        raw_data_dir, _ = temp_dirs
        custom_dir = raw_data_dir / "custom"
        custom_dir.mkdir()
        
        # Mock successful process
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.communicate.return_value = (
            "Successfully transferred 100 rows",
            ""
        )
        mock_popen.return_value = mock_process

        result = next(manager.transfer_data(
            source_schema="TEST",
            source_table="TABLE",
            sql_statement="SELECT * FROM TEST.TABLE",
            output_directory=str(custom_dir)
        ))

        assert result.is_successful
        assert result.file_path == str(custom_dir / "TEST_TABLE.csv")

    def test_transfer_data_validation(self, manager, mock_file_exists):
        """Test validation of transfer data parameters."""
        with pytest.raises(ValidationError, match="must have the same length"):
            next(manager.transfer_data(
                source_schema=["TEST1", "TEST2"],
                source_table=["TABLE1"],
                sql_statement=["SELECT * FROM TEST1.TABLE1"]
            )) 
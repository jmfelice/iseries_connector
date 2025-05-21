"""Tests for the data transfer functionality."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
from datetime import datetime
import json
import pandas as pd
from typing import Generator, Tuple, Any
from iseries_connector.data_transfer import (
    DataTransferConfig,
    DataTransferManager,
    DataTransferResult,
    ValidationError
)

@pytest.fixture(scope="function")
def temp_dirs() -> Generator[Tuple[Path, Path], None, None]:
    """Create temporary directories for testing.
    
    The directories are automatically cleaned up after each test.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        raw_data_dir = Path(temp_dir) / "raw_data"
        data_package_dir = Path(temp_dir) / "data_package"
        raw_data_dir.mkdir()
        data_package_dir.mkdir()
        yield raw_data_dir, data_package_dir

@pytest.fixture(scope="function")
def mock_acs_launcher() -> str:
    """Create a mock ACS launcher path."""
    return str(Path.cwd() / "mock_acs_launcher.exe")

@pytest.fixture(scope="function", autouse=True)
def mock_file_exists() -> Generator[MagicMock, None, None]:
    """Mock file existence checks.
    
    This fixture is automatically used in all tests (autouse=True)
    and is reset between tests (scope="function").
    """
    with patch('os.path.exists') as mock_exists:
        mock_exists.return_value = True
        yield mock_exists

@pytest.fixture(scope="function")
def config(temp_dirs: Tuple[Path, Path], mock_acs_launcher: str, mock_file_exists: MagicMock) -> DataTransferConfig:
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

    def test_init_with_defaults(self, mock_file_exists: MagicMock) -> None:
        """Test initialization with default values."""
        # Mock file existence check for the default ACS launcher path
        mock_file_exists.return_value = True
        
        config = DataTransferConfig(host_name="test.host.com")
        assert config.host_name == "test.host.com"
        assert config.database == "*SYSBAS"
        assert config.batch_size == 15
        assert config.template_path is None

    def test_init_with_custom_values(self, temp_dirs: Tuple[Path, Path], mock_file_exists: MagicMock) -> None:
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

    def test_from_env(self, monkeypatch: pytest.MonkeyPatch, mock_file_exists: MagicMock) -> None:
        """Test creating config from environment variables."""
        monkeypatch.setenv('ISERIES_HOST_NAME', 'env.host.com')
        monkeypatch.setenv('ISERIES_DATABASE', 'ENVDB')
        monkeypatch.setenv('ISERIES_BATCH_SIZE', '20')
        
        config = DataTransferConfig.from_env()
        assert config.host_name == 'env.host.com'
        assert config.database == 'ENVDB'
        assert config.batch_size == 20

    def test_validate_missing_host(self, mock_file_exists: MagicMock) -> None:
        """Test validation with missing host name."""
        with pytest.raises(ValidationError, match="Host name is required"):
            DataTransferConfig(host_name="")

    def test_validate_invalid_acs_path(self, mock_file_exists: MagicMock) -> None:
        """Test validation with invalid ACS launcher path."""
        mock_file_exists.return_value = False
        with pytest.raises(ValidationError, match="ACS Launcher not found"):
            DataTransferConfig(
                host_name="test.host.com",
                acs_launcher_path="nonexistent.exe"
            )

    def test_validate_invalid_batch_size(self, mock_file_exists: MagicMock) -> None:
        """Test validation with invalid batch size."""
        with pytest.raises(ValidationError, match="Batch size must be a positive number"):
            DataTransferConfig(
                host_name="test.host.com",
                batch_size=0
            )

    def test_validate_invalid_template_path(self, temp_dirs: Tuple[Path, Path], mock_file_exists: MagicMock) -> None:
        """Test validation with invalid template path."""
        # Make ACS launcher path check pass but template path check fail
        def mock_exists(path: str) -> bool:
            return path == "C:/Program Files/IBMiAccess_v1r1/Start_Programs/Windows_x86-64/acslaunch_win-64.exe"
        
        mock_file_exists.side_effect = mock_exists
        
        with pytest.raises(ValidationError, match="Template file not found"):
            DataTransferConfig(
                host_name="test.host.com",
                template_path="nonexistent.txt"
            )

class TestDataTransferResult:
    """Test cases for DataTransferResult class."""

    def test_init_with_required_fields(self) -> None:
        """Test initialization with required fields."""
        start_time = datetime.now()
        end_time = datetime.now()
        result = DataTransferResult(
            start_time=start_time,
            end_time=end_time,
            duration=1.0,
            row_count=100,
            output="Success",
            success=True,
            source_schema="TEST",
            source_table="TABLE"
        )
        
        assert result.start_time == start_time
        assert result.end_time == end_time
        assert result.duration == 1.0
        assert result.row_count == 100
        assert result.output == "Success"
        assert result.success is True
        assert result.source_schema == "TEST"
        assert result.source_table == "TABLE"
        assert result.is_successful is True

    def test_to_dataframe(self) -> None:
        """Test conversion to DataFrame."""
        start_time = datetime.now()
        end_time = datetime.now()
        result = DataTransferResult(
            start_time=start_time,
            end_time=end_time,
            duration=1.0,
            row_count=100,
            output="Success",
            success=True,
            source_schema="TEST",
            source_table="TABLE"
        )
        
        df = result.to_dataframe()
        assert len(df) == 1
        assert df['start_time'].iloc[0] == start_time.isoformat()
        assert df['end_time'].iloc[0] == end_time.isoformat()
        assert df['duration'].iloc[0] == 1.0
        assert df['row_count'].iloc[0] == 100
        assert bool(df['success'].iloc[0]) is True
        assert df['source_schema'].iloc[0] == "TEST"
        assert df['source_table'].iloc[0] == "TABLE"

    def test_to_json(self) -> None:
        """Test conversion to JSON."""
        start_time = datetime.now()
        end_time = datetime.now()
        result = DataTransferResult(
            start_time=start_time,
            end_time=end_time,
            duration=1.0,
            row_count=100,
            output="Success",
            success=True,
            source_schema="TEST",
            source_table="TABLE"
        )
        
        json_str = result.to_json()
        data = json.loads(json_str)
        assert data['row_count'] == 100
        assert data['success'] is True
        assert data['source_schema'] == "TEST"
        assert data['source_table'] == "TABLE" 

class TestDataTransferManager:
    """Test cases for DataTransferManager class."""

    @pytest.fixture
    def manager(self, config: DataTransferConfig, mock_file_exists: MagicMock) -> DataTransferManager:
        """Create a test manager instance."""
        return DataTransferManager(
            host_name=config.host_name,
            acs_launcher_path=config.acs_launcher_path,
            local_raw_data_directory=config.local_raw_data_directory,
            local_data_package_directory=config.local_data_package_directory
        )

    def test_init_with_config(self, config: DataTransferConfig, mock_file_exists: MagicMock) -> None:
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
    def test_transfer_data_single(self, mock_popen: MagicMock, manager: DataTransferManager, temp_dirs: Tuple[Path, Path], mock_file_exists: MagicMock) -> None:
        """Test single data transfer."""
        raw_data_dir, _ = temp_dirs
        
        # Mock successful process
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.communicate.return_value = (
            "rows transferred: 100",
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
    def test_transfer_data_batch(self, mock_popen: MagicMock, manager: DataTransferManager, temp_dirs: Tuple[Path, Path], mock_file_exists: MagicMock) -> None:
        """Test batch data transfer."""
        raw_data_dir, _ = temp_dirs
        
        # Mock successful processes
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.communicate.return_value = (
            "rows transferred: 100",
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
        assert all(r.row_count == 100 for r in results)
        assert results[0].file_path == str(raw_data_dir / "TEST1_TABLE1.csv")
        assert results[1].file_path == str(raw_data_dir / "TEST2_TABLE2.csv")

    @patch('subprocess.Popen')
    def test_transfer_data_failure(self, mock_popen: MagicMock, manager: DataTransferManager, mock_file_exists: MagicMock) -> None:
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
    def test_transfer_data_custom_output(self, mock_popen: MagicMock, manager: DataTransferManager, temp_dirs: Tuple[Path, Path], mock_file_exists: MagicMock) -> None:
        """Test data transfer with custom output directory."""
        raw_data_dir, _ = temp_dirs
        custom_dir = raw_data_dir / "custom"
        custom_dir.mkdir()
        
        # Mock successful process
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.communicate.return_value = (
            "rows transferred: 100",
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
        assert result.row_count == 100
        assert result.file_path == str(custom_dir / "TEST_TABLE.csv")

    def test_transfer_data_validation(self, manager: DataTransferManager, mock_file_exists: MagicMock) -> None:
        """Test validation of transfer data parameters."""
        with pytest.raises(ValidationError, match="must have the same length"):
            next(manager.transfer_data(
                source_schema=["TEST1", "TEST2"],
                source_table=["TABLE1"],
                sql_statement=["SELECT * FROM TEST1.TABLE1"]
            )) 
        
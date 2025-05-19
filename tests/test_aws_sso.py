import os
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import sqlite3
from pathlib import Path
import tempfile
import shutil

from aws_connector.aws_sso import SSOConfig, AWSsso
from aws_connector.exceptions import CredentialError, AuthenticationError

@pytest.fixture
def temp_db_dir():
    """Create a temporary directory for test database."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def mock_aws_cli():
    """Mock AWS CLI executable path."""
    return r'C:\mock\aws.exe'

@pytest.fixture
def sso_config(temp_db_dir, mock_aws_cli):
    """Create a test SSOConfig instance."""
    return SSOConfig(
        aws_exec_file_path=mock_aws_cli,
        db_path=Path(temp_db_dir) / 'test_credentials.db',
        refresh_window_hours=1,
        max_retries=2,
        retry_delay=1
    )

class TestSSOConfig:
    def test_default_config(self):
        """Test default configuration values."""
        config = SSOConfig()
        assert config.aws_exec_file_path == r'C:\Program Files\Amazon\AWSCLIV2\aws.exe'
        assert config.db_path == Path('./data/aws_credentials.db')
        assert config.refresh_window_hours == 6
        assert config.max_retries == 3
        assert config.retry_delay == 5

    def test_custom_config(self, temp_db_dir, mock_aws_cli):
        """Test custom configuration values."""
        config = SSOConfig(
            aws_exec_file_path=mock_aws_cli,
            db_path=Path(temp_db_dir) / 'test.db',
            refresh_window_hours=12,
            max_retries=5,
            retry_delay=10
        )
        assert config.aws_exec_file_path == mock_aws_cli
        assert config.db_path == Path(temp_db_dir) / 'test.db'
        assert config.refresh_window_hours == 12
        assert config.max_retries == 5
        assert config.retry_delay == 10

    def test_from_env(self, temp_db_dir, mock_aws_cli):
        """Test configuration from environment variables."""
        env_vars = {
            'AWS_EXEC_FILE_PATH': mock_aws_cli,
            'AWS_CREDENTIALS_DB_PATH': str(Path(temp_db_dir) / 'env.db'),
            'AWS_SSO_REFRESH_WINDOW': '12',
            'AWS_SSO_MAX_RETRIES': '5',
            'AWS_SSO_RETRY_DELAY': '10'
        }
        with patch.dict(os.environ, env_vars):
            config = SSOConfig.from_env()
            assert config.aws_exec_file_path == mock_aws_cli
            assert config.db_path == Path(temp_db_dir) / 'env.db'
            assert config.refresh_window_hours == 12
            assert config.max_retries == 5
            assert config.retry_delay == 10

    def test_validation(self):
        """Test configuration validation."""
        with pytest.raises(ValueError, match="AWS executable path cannot be empty"):
            SSOConfig(aws_exec_file_path="")
        
        with pytest.raises(ValueError, match="Database path cannot be empty"):
            SSOConfig(db_path=None)
        
        with pytest.raises(ValueError, match="Refresh window must be a positive number"):
            SSOConfig(refresh_window_hours=0)
        
        with pytest.raises(ValueError, match="Max retries cannot be negative"):
            SSOConfig(max_retries=-1)
        
        with pytest.raises(ValueError, match="Retry delay cannot be negative"):
            SSOConfig(retry_delay=-1)

class TestAWSsso:
    @pytest.fixture
    def mock_subprocess(self):
        """Mock subprocess.run for AWS CLI commands."""
        with patch('subprocess.run') as mock:
            mock.return_value = MagicMock(returncode=0, stdout="Success", stderr="")
            yield mock

    def test_init(self, sso_config):
        """Test AWSsso initialization."""
        sso = AWSsso(sso_config)
        assert sso.config == sso_config
        assert sso.config.db_path.exists()

    def test_should_refresh_credentials_no_previous_refresh(self, sso_config):
        """Test should_refresh_credentials when no previous refresh exists."""
        sso = AWSsso(sso_config)
        assert sso.should_refresh_credentials() is True

    def test_should_refresh_credentials_with_recent_refresh(self, sso_config):
        """Test should_refresh_credentials with recent refresh."""
        sso = AWSsso(sso_config)
        conn = sqlite3.connect(sso.config.db_path)
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO credential_timestamps (last_refresh) VALUES (?)',
            (datetime.now().isoformat(),)
        )
        conn.commit()
        conn.close()
        
        assert sso.should_refresh_credentials() is False

    def test_should_refresh_credentials_with_old_refresh(self, sso_config):
        """Test should_refresh_credentials with old refresh."""
        sso = AWSsso(sso_config)
        conn = sqlite3.connect(sso.config.db_path)
        cursor = conn.cursor()
        old_time = (datetime.now() - timedelta(hours=2)).isoformat()
        cursor.execute(
            'INSERT INTO credential_timestamps (last_refresh) VALUES (?)',
            (old_time,)
        )
        conn.commit()
        conn.close()
        
        assert sso.should_refresh_credentials() is True

    def test_refresh_credentials_success(self, sso_config, mock_subprocess):
        """Test successful credential refresh."""
        sso = AWSsso(sso_config)
        assert sso.refresh_credentials() is True
        mock_subprocess.assert_called_once_with(
            [sso_config.aws_exec_file_path, 'sso', 'login'],
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )

    def test_refresh_credentials_failure(self, sso_config, mock_subprocess):
        """Test failed credential refresh."""
        mock_subprocess.return_value.returncode = 1
        sso = AWSsso(sso_config)
        
        with pytest.raises(AuthenticationError):
            sso.refresh_credentials()

    def test_ensure_valid_credentials(self, sso_config, mock_subprocess):
        """Test ensure_valid_credentials."""
        sso = AWSsso(sso_config)
        assert sso.ensure_valid_credentials() is True
        mock_subprocess.assert_called_once()

    def test_db_connection_error(self, sso_config):
        """Test database connection error handling."""
        sso = AWSsso(sso_config)
        # Remove the database file to force a connection error
        sso.config.db_path.unlink()
        
        with pytest.raises(CredentialError):
            sso._get_db_connection()

    def test_aws_command_error(self, sso_config):
        """Test AWS command error handling."""
        sso = AWSsso(sso_config)
        
        with patch('subprocess.run', side_effect=FileNotFoundError()):
            with pytest.raises(AuthenticationError):
                sso._run_aws_command(['sso', 'login']) 
import subprocess
from typing import Optional, List
from datetime import datetime, timedelta
import sqlite3
from pathlib import Path
from dataclasses import dataclass
import os
import uuid
import time
from .exceptions import (
    AWSConnectorError,
    CredentialError,
    AuthenticationError
)
from .utils import setup_logging

# Configure logging
logger = setup_logging(__name__)

@dataclass
class SSOConfig:
    """
    Configuration for AWS SSO authentication.
    
    This class can be initialized with direct values or from environment variables.
    Environment variables take precedence over direct values.
    
    Environment Variables:
        AWS_EXEC_FILE_PATH: Path to the AWS CLI executable
        AWS_CREDENTIALS_DB_PATH: Path to the credentials database
        AWS_SSO_REFRESH_WINDOW: Hours between credential refreshes
        AWS_SSO_MAX_RETRIES: Maximum number of authentication retries
        AWS_SSO_RETRY_DELAY: Delay between retries in seconds
    
    Examples:
        ```python
        # Default configuration
        config = SSOConfig()  # Uses all default values
        
        # Custom configuration
        config = SSOConfig(
            aws_exec_file_path="/custom/path/to/aws",
            db_path=Path("./custom/path/credentials.db"),
            refresh_window_hours=12,  # Refresh every 12 hours
            max_retries=5,  # More retries
            retry_delay=10  # Longer delay between retries
        )
        
        # From environment variables
        config = SSOConfig.from_env()
        ```
    """
    aws_exec_file_path: str = r'C:\Program Files\Amazon\AWSCLIV2\aws.exe'
    db_path: Path = Path("./data/aws_credentials.db")
    refresh_window_hours: int = 6
    max_retries: int = 3
    retry_delay: int = 5
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        self.validate()
    
    @classmethod
    def from_env(cls) -> 'SSOConfig':
        """Create a configuration from environment variables.
        
        Returns:
            SSOConfig: A new configuration instance
            
        Raises:
            ValueError: If required environment variables are missing
        """
        return cls(
            aws_exec_file_path=os.environ.get('AWS_EXEC_FILE_PATH', r'C:\Program Files\Amazon\AWSCLIV2\aws.exe'),
            db_path=Path(os.environ.get('AWS_CREDENTIALS_DB_PATH', './data/aws_credentials.db')),
            refresh_window_hours=int(os.environ.get('AWS_SSO_REFRESH_WINDOW', '6')),
            max_retries=int(os.environ.get('AWS_SSO_MAX_RETRIES', '3')),
            retry_delay=int(os.environ.get('AWS_SSO_RETRY_DELAY', '5'))
        )
    
    def validate(self) -> None:
        """Validate the configuration parameters.
        
        Raises:
            ValueError: If any required parameters are missing or invalid
        """
        if not self.aws_exec_file_path:
            raise ValueError("AWS executable path cannot be empty")
        if not self.db_path:
            raise ValueError("Database path cannot be empty")
        if self.refresh_window_hours <= 0:
            raise ValueError("Refresh window must be a positive number")
        if self.max_retries < 0:
            raise ValueError("Max retries cannot be negative")
        if self.retry_delay < 0:
            raise ValueError("Retry delay cannot be negative")

class AWSsso:
    """
    A class to handle AWS SSO authentication and credential management.
    This class provides functionality to refresh AWS SSO credentials and track their validity.
    
    Testing:
        For testing purposes, you can override the following methods:
        - _get_db_connection(): Override to return a mock database connection
        - _run_aws_command(): Override to return mock AWS CLI output
        
        Example:
            ```python
            class MockAWSsso(AWSsso):
                def _run_aws_command(self):
                    return MockCommandOutput()
            ```
    """
    
    def __init__(self, config: Optional[SSOConfig] = None):
        """
        Initialize the AWS SSO handler.
        
        Args:
            config (Optional[SSOConfig]): Configuration for SSO authentication.
                                        If None, uses default configuration.
        
        Raises:
            ValueError: If configuration parameters are invalid
            
        Examples:
            ```python
            # Default configuration
            sso = AWSsso()
            
            # Custom configuration
            config = SSOConfig(refresh_window_hours=12)
            sso = AWSsso(config)
            ```
        """
        self.config = config or SSOConfig()
        self._validate_config()
        self._init_db()
    
    def _validate_config(self) -> None:
        """
        Validate the configuration parameters.
        
        Raises:
            ValueError: If any configuration parameters are invalid
            
        Examples:
            ```python
            config = SSOConfig(refresh_window_hours=-1)  # Invalid
            try:
                sso = AWSsso(config)  # Will raise ValueError
            except ValueError as e:
                print(f"Invalid configuration: {e}")
            ```
        """
        if not self.config.aws_exec_file_path:
            raise ValueError("AWS executable path cannot be empty")
        if not self.config.db_path:
            raise ValueError("Database path cannot be empty")
        if self.config.refresh_window_hours <= 0:
            raise ValueError("Refresh window must be a positive number")
        if self.config.max_retries < 0:
            raise ValueError("Max retries cannot be negative")
        if self.config.retry_delay < 0:
            raise ValueError("Retry delay cannot be negative")
    
    def _init_db(self) -> None:
        """
        Initialize the SQLite database for storing credential timestamps.
        
        Raises:
            sqlite3.Error: If there's an error creating the database or table
            
        Examples:
            ```python
            try:
                sso = AWSsso()
                # Database initialized successfully
            except CredentialError as e:
                print(f"Failed to initialize database: {e}")
            ```
        """
        try:
            self.config.db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self.config.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS credential_timestamps (
                    id INTEGER PRIMARY KEY,
                    last_refresh TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
            conn.close()
            logger.info(f"Initialized credential database at {self.config.db_path}")
        except sqlite3.Error as e:
            error_msg = f"Error initializing credential database: {str(e)}"
            logger.error(error_msg)
            raise CredentialError(error_msg)
    
    def should_refresh_credentials(self) -> bool:
        """
        Check if AWS SSO credentials need to be refreshed based on configured window.
        
        Returns:
            bool: True if credentials need to be refreshed, False otherwise
            
        Raises:
            CredentialError: If there's an error checking credential status
            
        Examples:
            ```python
            sso = AWSsso()
            
            # Check if refresh is needed
            if sso.should_refresh_credentials():
                print("SSO credentials need refresh")
                sso.refresh_credentials()
            else:
                print("SSO credentials are still valid")
            ```
        """
        try:
            conn = sqlite3.connect(self.config.db_path)
            cursor = conn.cursor()
            cursor.execute('SELECT last_refresh FROM credential_timestamps ORDER BY id DESC LIMIT 1')
            result = cursor.fetchone()
            conn.close()
            
            if not result:
                logger.info("No previous SSO credential refresh found")
                return True
                
            last_refresh = datetime.fromisoformat(result[0])
            needs_refresh = datetime.now() - last_refresh > timedelta(hours=self.config.refresh_window_hours)
            
            if needs_refresh:
                logger.info(f"SSO credentials need refresh. Last refresh was {last_refresh}")
            else:
                logger.info(f"SSO credentials are still valid. Last refresh was {last_refresh}")
                
            return needs_refresh
            
        except Exception as e:
            error_msg = f"Error checking SSO credential timestamp: {str(e)}"
            logger.error(error_msg)
            raise CredentialError(error_msg)
    
    def update_refresh_timestamp(self) -> bool:
        """
        Update the timestamp of the last SSO credential refresh.
        
        Returns:
            bool: True if update was successful, False otherwise
            
        Raises:
            CredentialError: If there's an error updating the timestamp
            
        Examples:
            ```python
            try:
                if sso.update_refresh_timestamp():
                    print("Successfully updated SSO refresh timestamp")
                else:
                    print("Failed to update SSO refresh timestamp")
            except CredentialError as e:
                print(f"Error updating timestamp: {e}")
            ```
        """
        try:
            conn = sqlite3.connect(self.config.db_path)
            cursor = conn.cursor()
            cursor.execute(
                'INSERT INTO credential_timestamps (last_refresh) VALUES (?)',
                (datetime.now().isoformat(),)
            )
            conn.commit()
            conn.close()
            logger.info("Successfully updated SSO credential refresh timestamp")
            return True
        except Exception as e:
            error_msg = f"Error updating SSO credential timestamp: {str(e)}"
            logger.error(error_msg)
            raise CredentialError(error_msg)
    
    def refresh_credentials(self) -> bool:
        """
        Refresh AWS SSO credentials using the AWS CLI.
        
        Returns:
            bool: True if refresh was successful, False otherwise
            
        Raises:
            AuthenticationError: If there's an error during SSO authentication
            CredentialError: If there's an error updating the timestamp
        """
        for attempt in range(self.config.max_retries):
            try:
                result = subprocess.run(
                    [self.config.aws_exec_file_path, 'sso', 'login'],
                    shell=True,
                    check=True,
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0:
                    logger.info("Successfully authenticated with AWS SSO")
                    return self.update_refresh_timestamp()
                if attempt < self.config.max_retries - 1:
                    logger.warning(f"SSO authentication attempt {attempt + 1} failed, retrying...")
                    time.sleep(self.config.retry_delay)
            except subprocess.CalledProcessError as e:
                if attempt == self.config.max_retries - 1:
                    raise AuthenticationError(f"Failed to refresh SSO credentials after {self.config.max_retries} attempts: {str(e)}")
                logger.warning(f"SSO authentication attempt {attempt + 1} failed: {str(e)}")
                time.sleep(self.config.retry_delay)
            except FileNotFoundError as e:
                error_msg = f"AWS CLI executable not found at: {self.config.aws_exec_file_path}"
                logger.error(error_msg)
                raise AuthenticationError(error_msg)
        
        # If we've exhausted all retries without success, raise AuthenticationError
        raise AuthenticationError(f"Failed to refresh SSO credentials after {self.config.max_retries} attempts")
    
    def ensure_valid_credentials(self) -> bool:
        """
        Ensures that AWS SSO credentials are valid by refreshing them if necessary.
        
        Returns:
            bool: True if credentials are valid, False otherwise
            
        Raises:
            AuthenticationError: If there's an error during SSO authentication
            CredentialError: If there's an error managing credentials
            
        Examples:
            ```python
            # Basic usage
            if sso.ensure_valid_credentials():
                print("SSO credentials are valid")
            else:
                print("Failed to ensure valid SSO credentials")
            
            # With error handling
            try:
                sso.ensure_valid_credentials()
                # Proceed with AWS operations
            except (AuthenticationError, CredentialError) as e:
                print(f"Error ensuring valid SSO credentials: {e}")
            ```
        """
        if self.should_refresh_credentials():
            return self.refresh_credentials()
        return True

    def _get_db_connection(self) -> sqlite3.Connection:
        """
        Get a connection to the SQLite database.
        
        Returns:
            sqlite3.Connection: A connection to the database
            
        Raises:
            CredentialError: If there's an error connecting to the database
        """
        try:
            if not self.config.db_path.exists():
                raise CredentialError(f"Database file does not exist: {self.config.db_path}")
            return sqlite3.connect(self.config.db_path)
        except sqlite3.Error as e:
            error_msg = f"Error connecting to credential database: {str(e)}"
            logger.error(error_msg)
            raise CredentialError(error_msg)
    
    def _run_aws_command(self, command: List[str]) -> subprocess.CompletedProcess:
        """Run an AWS CLI command. Override this method for testing.
        
        Args:
            command (List[str]): The command to run
            
        Returns:
            subprocess.CompletedProcess: The command execution result
            
        Raises:
            AuthenticationError: If there's an error executing the command
        """
        try:
            return subprocess.run(
                command,
                shell=True,
                check=True,
                capture_output=True,
                text=True
            )
        except subprocess.CalledProcessError as e:
            raise AuthenticationError(f"Error executing AWS command: {str(e)}")
        except FileNotFoundError as e:
            raise AuthenticationError(f"AWS CLI executable not found: {str(e)}")
    
    @property
    def db_connection(self) -> sqlite3.Connection:
        """Get the current database connection.
        
        Returns:
            sqlite3.Connection: The current database connection
            
        Raises:
            CredentialError: If there's an error connecting to the database
        """
        return self._get_db_connection()


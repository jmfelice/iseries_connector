"""Data transfer package for IBM iSeries using DTFX files.

This module provides functionality to transfer data from IBM iSeries systems using
the IBM Access Client Solutions Data Transfer feature. It is designed to be more
efficient than ODBC for large data volumes.

Example:
    ```python
    # Create a data transfer configuration
    config = DataTransferConfig(
        host_name="your.hostname.com",
        database="*SYSBAS",
        acs_launcher_path="C:/Program Files/IBMiAccess_v1r1/Start_Programs/Windows_x86-64/acslaunch_win-64.exe"
    )

    # Initialize the data transfer manager
    dtm = DataTransferManager(config)

    # Create and execute a data transfer
    result = dtm.transfer_data(
        host_name="your.hostname.com",
        source_schema="SCHEMA",
        source_table="TABLE",
        sql_statement="SELECT * FROM SCHEMA.TABLE",
        output_directory="path/to/output"
    )
    ```
"""

from dataclasses import dataclass
from datetime import datetime
import os
import subprocess
import time
from typing import Dict, List, Optional, Any, Generator
from pathlib import Path
import importlib.resources
import tempfile

from .exceptions import (
    ISeriesConnectorError,
    ConfigurationError,
    ValidationError
)

@dataclass
class DataTransferConfig:
    """Configuration for data transfer operations.
    
    Attributes:
        host_name: The hostname of the iSeries system
        database: The database name (default: *SYSBAS)
        acs_launcher_path: Path to the ACS Launcher executable
        batch_size: Number of concurrent transfers (default: 15)
        template_path: Path to the DTFX template file (default: uses built-in template)
        local_raw_data_directory: Base directory for raw data files
        local_data_package_directory: Base directory for DTFX files
    """
    host_name: str
    database: str = "*SYSBAS"
    acs_launcher_path: str = "C:/Program Files/IBMiAccess_v1r1/Start_Programs/Windows_x86-64/acslaunch_win-64.exe"
    batch_size: int = 15
    template_path: Optional[str] = None
    local_raw_data_directory: Optional[str] = None
    local_data_package_directory: Optional[str] = None

    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.host_name:
            raise ConfigurationError("Host name is required")
        
        if not os.path.exists(self.acs_launcher_path):
            raise ConfigurationError(f"ACS Launcher not found at: {self.acs_launcher_path}")
        
        # Set default paths if not provided
        if not self.local_raw_data_directory:
            self.local_raw_data_directory = str(Path.cwd() / "raw_data")
        
        if not self.local_data_package_directory:
            self.local_data_package_directory = str(Path.cwd() / "data_package")
        
        # Create directories if they don't exist
        os.makedirs(self.local_raw_data_directory, exist_ok=True)
        os.makedirs(self.local_data_package_directory, exist_ok=True)

@dataclass
class DataTransferResult:
    """Result of a data transfer operation.
    
    Attributes:
        start_time: When the transfer started
        end_time: When the transfer completed
        duration: Total duration of the transfer
        row_count: Number of rows transferred (if available)
        output: Command output
        success: Whether the transfer was successful
        error: Error message if transfer failed
        file_path: Path to the output file
    """
    start_time: datetime
    end_time: datetime
    duration: float
    row_count: Optional[int]
    output: Optional[str]
    success: bool
    error: Optional[str] = None
    file_path: Optional[str] = None

    @property
    def is_successful(self) -> bool:
        """Check if the transfer was successful."""
        return self.success and self.row_count is not None

class DataTransferManager:
    """Manages data transfer operations using DTFX files.
    
    This class provides methods to create and execute data transfers from IBM iSeries
    systems using the IBM Access Client Solutions Data Transfer feature.
    
    Example:
        ```python
        # Create a data transfer manager
        dtm = DataTransferManager(DataTransferConfig(
            host_name="your.hostname.com"
        ))
        
        # Transfer data from a table
        result = dtm.transfer_data(
            host_name="your.hostname.com",
            source_schema="SCHEMA",
            source_table="TABLE",
            sql_statement="SELECT * FROM SCHEMA.TABLE"
        )
        
        if result.is_successful:
            print(f"Transferred {result.row_count} rows")
        ```
    """
    
    def __init__(self, config: DataTransferConfig):
        """Initialize the data transfer manager.
        
        Args:
            config: Configuration for data transfer operations
        """
        self.config = config
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate the configuration.
        
        Raises:
            ConfigurationError: If the configuration is invalid
        """
        if self.config.template_path and not os.path.exists(self.config.template_path):
            raise ConfigurationError(f"Template file not found: {self.config.template_path}")
    
    def _get_template_content(self) -> str:
        """Get the template content from either the configured path or the built-in template.
        
        Returns:
            str: The template content
            
        Raises:
            ConfigurationError: If there's an error reading the template
        """
        try:
            if self.config.template_path:
                with open(self.config.template_path, 'r') as file:
                    return file.read()
            else:
                # Use the built-in template
                with importlib.resources.open_text('iseries_connector.templates', 'dtfx_template.txt') as file:
                    return file.read()
        except Exception as e:
            raise ConfigurationError(f"Error reading template: {str(e)}")
    
    def _create_dtfx_file(
        self,
        host_name: str,
        source_schema: str,
        source_table: str,
        sql_statement: str,
        output_path: str
    ) -> None:
        """Create a DTFX file from the template.
        
        Args:
            host_name: The hostname of the iSeries system
            source_schema: Source schema name
            source_table: Source table name
            sql_statement: SQL statement to execute
            output_path: Path where the DTFX file should be created
            
        Raises:
            ConfigurationError: If there's an error creating the DTFX file
        """
        try:
            template = self._get_template_content()
            
            # Replace parameters in the template
            replacements = {
                '{{local_raw_data_directory}}': self.config.local_raw_data_directory,
                '{{local_data_package_directory}}': self.config.local_data_package_directory,
                '{{source_schema}}': source_schema,
                '{{source_table}}': source_table,
                '{{sql_statement}}': sql_statement,
                '{{host_name}}': host_name
            }
            
            for key, value in replacements.items():
                template = template.replace(key, value)
            
            # Write the DTFX file
            with open(output_path, 'w') as file:
                file.write(template)
                
        except Exception as e:
            raise ConfigurationError(f"Error creating DTFX file: {str(e)}")
    
    def transfer_data(
        self,
        host_name: str,
        source_schema: str,
        source_table: str,
        sql_statement: str,
        output_directory: Optional[str] = None
    ) -> DataTransferResult:
        """Transfer data from the iSeries system.
        
        Args:
            host_name: The hostname of the iSeries system
            source_schema: Source schema name
            source_table: Source table name
            sql_statement: SQL statement to execute
            output_directory: Optional output directory (defaults to config)
            
        Returns:
            DataTransferResult: Result of the transfer operation
            
        Raises:
            ConfigurationError: If there's an error in the configuration
            ValidationError: If the transfer fails
        """
        start_time = datetime.now()
        
        try:
            # Create output directory if specified
            if output_directory:
                os.makedirs(output_directory, exist_ok=True)
            else:
                output_directory = self.config.local_raw_data_directory
            
            # Create DTFX file
            dtfx_path = os.path.join(
                self.config.local_data_package_directory,
                f"{source_schema}_{source_table}.dtfx"
            )
            self._create_dtfx_file(
                host_name,
                source_schema,
                source_table,
                sql_statement,
                dtfx_path
            )
            
            # Execute the DTFX file
            command = f'start "" "{self.config.acs_launcher_path}" /PLUGIN=download "{dtfx_path}"'
            result = subprocess.run(
                command,
                shell=True,
                check=True,
                capture_output=True,
                text=True,
                encoding='utf-8'
            )
            
            # Process the result
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Extract row count from output
            row_count = None
            for line in result.stdout.split('\n'):
                if 'rows' in line.lower():
                    import re
                    row_match = re.search(r'(\d+)\s*(?:row|rows)', line.lower())
                    if row_match:
                        row_count = int(row_match.group(1))
                        break
            
            return DataTransferResult(
                start_time=start_time,
                end_time=end_time,
                duration=duration,
                row_count=row_count,
                output=result.stdout,
                success=True,
                file_path=os.path.join(output_directory, f"{source_schema}_{source_table}.csv")
            )
            
        except subprocess.CalledProcessError as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            return DataTransferResult(
                start_time=start_time,
                end_time=end_time,
                duration=duration,
                row_count=None,
                output=e.stdout,
                success=False,
                error=e.stderr
            )
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            return DataTransferResult(
                start_time=start_time,
                end_time=end_time,
                duration=duration,
                row_count=None,
                output=None,
                success=False,
                error=str(e)
            )
    
    def transfer_multiple(
        self,
        transfers: List[Dict[str, str]],
        output_directory: Optional[str] = None
    ) -> Generator[DataTransferResult, None, None]:
        """Transfer multiple datasets in batches.
        
        Args:
            transfers: List of transfer configurations, each containing:
                - host_name: The hostname of the iSeries system
                - source_schema: Source schema name
                - source_table: Source table name
                - sql_statement: SQL statement to execute
            output_directory: Optional output directory (defaults to config)
            
        Yields:
            DataTransferResult: Result of each transfer operation
        """
        for i in range(0, len(transfers), self.config.batch_size):
            batch = transfers[i:i + self.config.batch_size]
            batch_results = []
            
            # Execute batch
            for transfer in batch:
                result = self.transfer_data(
                    host_name=transfer['host_name'],
                    source_schema=transfer['source_schema'],
                    source_table=transfer['source_table'],
                    sql_statement=transfer['sql_statement'],
                    output_directory=output_directory
                )
                batch_results.append(result)
                yield result
            
            # Wait between batches if not the last batch
            if i + self.config.batch_size < len(transfers):
                time.sleep(30)  # Wait 30 seconds between batches 
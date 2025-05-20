"""Data transfer package for IBM iSeries using DTFX files.

This module provides functionality to transfer data from IBM iSeries systems using
the IBM Access Client Solutions Data Transfer feature. It is designed to be more
efficient than ODBC for large data volumes.

Example:

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
    
"""

from dataclasses import dataclass
from datetime import datetime
import os
import subprocess
import time
from typing import Dict, List, Optional, Any, Generator, Union
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
        source_schema: Union[str, List[str]],
        source_table: Union[str, List[str]],
        sql_statement: Union[str, List[str]],
        output_directory: Optional[str] = None
    ) -> Generator[DataTransferResult, None, None]:
        """Transfer data from the iSeries system.
        
        This method can handle both single transfers and multiple transfers in parallel.
        When lists are provided, it will process them in batches for parallel execution.
        
        Args:
            source_schema: Source schema name or list of schema names
            source_table: Source table name or list of table names
            sql_statement: SQL statement or list of SQL statements
            output_directory: Optional output directory (defaults to config)
            
        Yields:
            DataTransferResult: Result of each transfer operation
            
        Raises:
            ConfigurationError: If there's an error in the configuration
            ValidationError: If the transfer fails
            
        Examples:
            Single transfer:
            ```python
            # Transfer data from a single table
            result = next(dtm.transfer_data(
                source_schema="SCHEMA",
                source_table="TABLE",
                sql_statement="SELECT * FROM SCHEMA.TABLE"
            ))
            
            if result.is_successful:
                print(f"Transferred {result.row_count} rows to {result.file_path}")
            ```
            
            Batch transfer:
            ```python
            # Transfer data from multiple tables in parallel
            schemas = ["SCHEMA1", "SCHEMA2", "SCHEMA3"]
            tables = ["TABLE1", "TABLE2", "TABLE3"]
            sql_statements = [
                "SELECT * FROM SCHEMA1.TABLE1",
                "SELECT * FROM SCHEMA2.TABLE2",
                "SELECT * FROM SCHEMA3.TABLE3"
            ]
            
            # Process results as they complete
            for result in dtm.transfer_data(
                source_schema=schemas,
                source_table=tables,
                sql_statement=sql_statements
            ):
                if result.is_successful:
                    print(f"Successfully transferred {result.row_count} rows to {result.file_path}")
                else:
                    print(f"Transfer failed: {result.error}")
            ```
            
            Custom output directory:
            ```python
            # Transfer to a specific output directory
            result = next(dtm.transfer_data(
                source_schema="SCHEMA",
                source_table="TABLE",
                sql_statement="SELECT * FROM SCHEMA.TABLE",
                output_directory="C:/custom/output/path"
            ))
            ```
        """
        # Convert single values to lists for consistent processing
        schemas = [source_schema] if isinstance(source_schema, str) else source_schema
        tables = [source_table] if isinstance(source_table, str) else source_table
        statements = [sql_statement] if isinstance(sql_statement, str) else sql_statement
        
        # Validate lists have same length
        if not (len(schemas) == len(tables) == len(statements)):
            raise ValidationError("source_schema, source_table, and sql_statement lists must have the same length")
        
        # Create output directory if specified
        if output_directory:
            os.makedirs(output_directory, exist_ok=True)
        else:
            output_directory = self.config.local_raw_data_directory
        
        # Phase 1: Build all DTFX files
        dtfx_files = []
        for schema, table, sql in zip(schemas, tables, statements):
            dtfx_path = os.path.join(
                self.config.local_data_package_directory,
                f"{schema}_{table}.dtfx"
            )
            self._create_dtfx_file(
                host_name=self.config.host_name,
                source_schema=schema,
                source_table=table,
                sql_statement=sql,
                output_path=dtfx_path
            )
            dtfx_files.append((dtfx_path, schema, table))
        
        # Phase 2: Execute DTFX files in parallel batches
        for i in range(0, len(dtfx_files), self.config.batch_size):
            batch = dtfx_files[i:i + self.config.batch_size]
            processes = []
            
            # Start all transfers in the batch concurrently
            for dtfx_path, schema, table in batch:
                command = f'start "" "{self.config.acs_launcher_path}" /PLUGIN=download "{dtfx_path}"'
                process = subprocess.Popen(
                    command,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding='utf-8'
                )
                processes.append((dtfx_path, schema, table, process))
            
            # Wait for all processes in the batch to complete
            for dtfx_path, schema, table, process in processes:
                start_time = datetime.now()
                try:
                    stdout, stderr = process.communicate()
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    success = process.returncode == 0
                    
                    # Extract row count from output
                    row_count = None
                    for line in stdout.split('\n'):
                        if 'rows' in line.lower():
                            import re
                            row_match = re.search(r'(\d+)\s*(?:row|rows)', line.lower())
                            if row_match:
                                row_count = int(row_match.group(1))
                                break
                    
                    result = DataTransferResult(
                        start_time=start_time,
                        end_time=end_time,
                        duration=duration,
                        row_count=row_count,
                        output=stdout,
                        success=success,
                        error=stderr if not success else None,
                        file_path=os.path.join(output_directory, f"{schema}_{table}.csv")
                    )
                    yield result
                    
                except Exception as e:
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    yield DataTransferResult(
                        start_time=start_time,
                        end_time=end_time,
                        duration=duration,
                        row_count=None,
                        output=None,
                        success=False,
                        error=str(e),
                        file_path=dtfx_path
                    )
            
            # Wait between batches if not the last batch
            if i + self.config.batch_size < len(dtfx_files):
                time.sleep(30)  # Wait 30 seconds between batches 
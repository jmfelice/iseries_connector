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
import json
import pandas as pd

from .exceptions import (
    ISeriesConnectorError,
    ConfigurationError,
    ValidationError
)

@dataclass
class DataTransferConfig:
    """Configuration for data transfer operations.
    
    This class can be initialized with direct values or from environment variables.
    Environment variables take precedence over direct values.
    
    Environment Variables:
        ISERIES_HOST_NAME: The hostname of the iSeries system
        ISERIES_DATABASE: The database name (default: *SYSBAS)
        ISERIES_ACS_LAUNCHER_PATH: Path to the ACS Launcher executable
        ISERIES_BATCH_SIZE: Number of concurrent transfers
        ISERIES_TEMPLATE_PATH: Path to the DTFX template file
        ISERIES_RAW_DATA_DIR: Base directory for raw data files
        ISERIES_DATA_PACKAGE_DIR: Base directory for DTFX files
    
    Examples:
    
        # Direct initialization
        config = DataTransferConfig(
            host_name="your.hostname.com",
            acs_launcher_path="C:/Program Files/IBMiAccess_v1r1/Start_Programs/Windows_x86-64/acslaunch_win-64.exe"
        )

        # From environment variables
        config = DataTransferConfig.from_env()

        # Mixed initialization
        config = DataTransferConfig(host_name="your.hostname.com").from_env()
    """
    host_name: str
    database: str = "*SYSBAS"
    acs_launcher_path: str = "C:/Program Files/IBMiAccess_v1r1/Start_Programs/Windows_x86-64/acslaunch_win-64.exe"
    batch_size: int = 15
    template_path: Optional[str] = None
    local_raw_data_directory: Optional[str] = None
    local_data_package_directory: Optional[str] = None

    @classmethod
    def from_env(cls) -> 'DataTransferConfig':
        """Create a configuration from environment variables.
        
        Returns:
            DataTransferConfig: A new configuration instance
            
        Raises:
            ValueError: If required environment variables are missing
        """
        return cls(
            host_name=os.environ.get('ISERIES_HOST_NAME', ''),
            database=os.environ.get('ISERIES_DATABASE', '*SYSBAS'),
            acs_launcher_path=os.environ.get(
                'ISERIES_ACS_LAUNCHER_PATH',
                "C:/Program Files/IBMiAccess_v1r1/Start_Programs/Windows_x86-64/acslaunch_win-64.exe"
            ),
            batch_size=int(os.environ.get('ISERIES_BATCH_SIZE', '15')),
            template_path=os.environ.get('ISERIES_TEMPLATE_PATH'),
            local_raw_data_directory=os.environ.get('ISERIES_RAW_DATA_DIR'),
            local_data_package_directory=os.environ.get('ISERIES_DATA_PACKAGE_DIR')
        )

    def validate(self) -> None:
        """Validate the configuration parameters.
        
        Raises:
            ValidationError: If any required parameters are missing or invalid
        """
        if not self.host_name:
            raise ValidationError("Host name is required")
        
        if not os.path.exists(self.acs_launcher_path):
            raise ValidationError(f"ACS Launcher not found at: {self.acs_launcher_path}")
        
        if self.batch_size <= 0:
            raise ValidationError("Batch size must be a positive number")
        
        if self.template_path and not os.path.exists(self.template_path):
            raise ValidationError(f"Template file not found: {self.template_path}")
        
        # Set default paths if not provided
        if not self.local_raw_data_directory:
            self.local_raw_data_directory = str(Path.cwd() / "raw_data")
        
        if not self.local_data_package_directory:
            self.local_data_package_directory = str(Path.cwd() / "data_package")
        
        # Create directories if they don't exist
        os.makedirs(self.local_raw_data_directory, exist_ok=True)
        os.makedirs(self.local_data_package_directory, exist_ok=True)

    def __post_init__(self):
        """Validate configuration after initialization."""
        self.validate()

@dataclass
class DataTransferResult:
    """Result of a data transfer operation.
    
    Attributes:
        start_time: When the transfer started
        end_time: When the transfer completed
        duration: Total duration of the transfer in seconds
        row_count: Number of rows transferred (if available)
        output: Command output
        success: Whether the transfer was successful
        error: Error message if transfer failed
        file_path: Path to the output file
        source_schema: The source schema name
        source_table: The source table name
    """
    source_schema: str
    source_table: str
    start_time: datetime
    end_time: datetime
    duration: float
    row_count: Optional[int]
    output: Optional[str]
    success: bool
    error: Optional[str] = None
    file_path: Optional[str] = None
    batch_start_time: Optional[datetime] = None
    batch_end_time: Optional[datetime] = None
    batch_duration: Optional[float] = None

    def __post_init__(self) -> None:
        """Validate the result data after initialization.
        
        Raises:
            ValidationError: If the data is invalid
        """
        if self.duration < 0:
            raise ValidationError("Duration cannot be negative")
        
        if self.start_time > self.end_time:
            raise ValidationError("Start time must be before end time")
        
        if self.row_count is not None and self.row_count < 0:
            raise ValidationError("Row count cannot be negative")

    @property
    def is_successful(self) -> bool:
        """Check if the transfer was successful.
        
        A transfer is considered successful if the process completed without errors,
        regardless of whether the row count was captured.
        
        Returns:
            bool: True if the transfer was successful
        """
        return self.success

    def to_dict(self) -> Dict[str, Any]:
        """Convert the result to a dictionary.
        
        Returns:
            Dict[str, Any]: Dictionary representation of the result
        """
        return {
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'duration': self.duration,
            'row_count': self.row_count,
            'success': self.success,
            'error': self.error,
            'file_path': self.file_path,
            'source_schema': self.source_schema,
            'source_table': self.source_table,
            'batch_start_time': self.batch_start_time.isoformat() if self.batch_start_time else None,
            'batch_end_time': self.batch_end_time.isoformat() if self.batch_end_time else None,
            'batch_duration': self.batch_duration
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataTransferResult':
        """Create a result from a dictionary.
        
        Args:
            data: Dictionary containing result data
            
        Returns:
            DataTransferResult: New result instance
            
        Raises:
            ValidationError: If the data is invalid
        """
        try:
            return cls(
                start_time=datetime.fromisoformat(data['start_time']),
                end_time=datetime.fromisoformat(data['end_time']),
                duration=float(data['duration']),
                row_count=data.get('row_count'),
                output=data.get('output'),
                success=bool(data['success']),
                error=data.get('error'),
                file_path=data.get('file_path'),
                source_schema=str(data['source_schema']),
                source_table=str(data['source_table']),
                batch_start_time=datetime.fromisoformat(data['batch_start_time']) if data['batch_start_time'] else None,
                batch_end_time=datetime.fromisoformat(data['batch_end_time']) if data['batch_end_time'] else None,
                batch_duration=float(data['batch_duration']) if data['batch_duration'] else None
            )
        except (KeyError, ValueError) as e:
            raise ValidationError(f"Invalid data format: {str(e)}")

    def to_dataframe(self) -> pd.DataFrame:
        """Convert the result to a pandas DataFrame.
        
        Returns:
            pd.DataFrame: A single-row DataFrame containing the result data
        """
        return pd.DataFrame([self.to_dict()])

    def to_json(self, indent: int = 2) -> str:
        """Convert the result to a JSON string.
        
        Args:
            indent: Number of spaces for indentation (default: 2)
            
        Returns:
            str: JSON string representation of the result
        """
        return json.dumps(self.to_dict(), indent=indent)

    def __eq__(self, other: Any) -> bool:
        """Compare two results for equality.
        
        Args:
            other: Another object to compare with
            
        Returns:
            bool: True if the results are equal
        """
        if not isinstance(other, DataTransferResult):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    def __repr__(self) -> str:
        """String representation of the result for debugging.
        
        Returns:
            str: Debug representation
        """
        return (
            f"DataTransferResult("
            f"start_time={self.start_time!r}, "
            f"end_time={self.end_time!r}, "
            f"duration={self.duration}, "
            f"row_count={self.row_count}, "
            f"success={self.success}, "
            f"source_schema={self.source_schema!r}, "
            f"source_table={self.source_table!r}"
            f")"
        )

    def __str__(self) -> str:
        """String representation of the result.
        
        Returns:
            str: Formatted string representation
        """
        return self.to_json()

class DataTransferManager:
    """Manages data transfer operations using DTFX files.
    
    This class provides methods to create and execute data transfers from IBM iSeries
    systems using the IBM Access Client Solutions Data Transfer feature.
    
    Examples:
        # Create a data transfer manager with direct configuration
        dtm = DataTransferManager(
            host_name="your.hostname.com",
            acs_launcher_path="C:/Program Files/IBMiAccess_v1r1/Start_Programs/Windows_x86-64/acslaunch_win-64.exe"
        )
        
        # Create a data transfer manager using environment variables
        dtm = DataTransferManager(
            host_name=os.environ.get('ISERIES_HOST_NAME'),
            acs_launcher_path=os.environ.get('ISERIES_ACS_LAUNCHER_PATH')
        )
        
        # Transfer data from a single table
        result = next(dtm.transfer_data(
            source_schema="SCHEMA",
            source_table="TABLE",
            sql_statement="SELECT * FROM SCHEMA.TABLE"
        ))
        
        if result.is_successful:
            print(f"Transferred {result.row_count} rows to {result.file_path}")
            
        # Transfer data with custom output directory
        result = next(dtm.transfer_data(
            source_schema="SCHEMA",
            source_table="TABLE",
            sql_statement="SELECT * FROM SCHEMA.TABLE",
            output_directory="C:/custom/output/path"
        ))
    """
    
    def __init__(
        self,
        host_name: str,
        acs_launcher_path: str = "C:/Program Files/IBMiAccess_v1r1/Start_Programs/Windows_x86-64/acslaunch_win-64.exe",
        database: str = "*SYSBAS",
        batch_size: int = 15,
        template_path: Optional[str] = None,
        local_raw_data_directory: Optional[str] = None,
        local_data_package_directory: Optional[str] = None
    ):
        """Initialize the data transfer manager.
        
        Args:
            host_name: The hostname of the iSeries system
            acs_launcher_path: Path to the ACS Launcher executable
            database: The database name (default: *SYSBAS)
            batch_size: Number of concurrent transfers (default: 15)
            template_path: Path to the DTFX template file (default: uses built-in template)
            local_raw_data_directory: Base directory for raw data files
            local_data_package_directory: Base directory for DTFX files
        """
        self.config = DataTransferConfig(
            host_name=host_name,
            acs_launcher_path=acs_launcher_path,
            database=database,
            batch_size=batch_size,
            template_path=template_path,
            local_raw_data_directory=local_raw_data_directory,
            local_data_package_directory=local_data_package_directory
        )
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
                '{{database}}': self.config.database,
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
            # Transfer data from a single table
            result = next(dtm.transfer_data(
                source_schema="SCHEMA",
                source_table="TABLE",
                sql_statement="SELECT * FROM SCHEMA.TABLE"
            ))
            
            if result.is_successful:
                print(f"Transferred {result.row_count} rows to {result.file_path}")
            
            Batch transfer:
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
            
            Custom output directory:
            # Transfer to a specific output directory
            result = next(dtm.transfer_data(
                source_schema="SCHEMA",
                source_table="TABLE",
                sql_statement="SELECT * FROM SCHEMA.TABLE",
                output_directory="C:/custom/output/path"
            ))
            
            Using environment variables:
            # Create manager using environment variables
            dtm = DataTransferManager(
                host_name=os.environ.get('ISERIES_HOST_NAME'),
                acs_launcher_path=os.environ.get('ISERIES_ACS_LAUNCHER_PATH')
            )
            
            # Transfer data
            result = next(dtm.transfer_data(
                source_schema="SCHEMA",
                source_table="TABLE",
                sql_statement="SELECT * FROM SCHEMA.TABLE"
            ))
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
                command = f'"{self.config.acs_launcher_path}" /PLUGIN=download "{dtfx_path}"'
                process = subprocess.Popen(
                    command,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding='utf-8',
                    creationflags=subprocess.CREATE_NO_WINDOW  # Prevent window from showing
                )
                processes.append((dtfx_path, schema, table, process))
            
            # Wait for all processes in the batch to complete
            for dtfx_path, schema, table, process in processes:
                start_time = datetime.now()
                try:
                    # Add a small delay to ensure output is captured
                    time.sleep(1)
                    stdout, stderr = process.communicate()
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    success = process.returncode == 0
                    
                    # Extract row count from output with improved parsing
                    row_count = None
                    if stdout:
                        for line in stdout.split('\n'):
                            if 'rows' in line.lower():
                                import re
                                # Look for patterns like "X rows" or "X row" or "X records"
                                row_match = re.search(r'rows transferred:\s*(\d+)', line.lower())
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
                        file_path=os.path.join(output_directory, f"{schema}_{table}.csv"),
                        source_schema=schema,
                        source_table=table
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
                        file_path=dtfx_path,
                        source_schema=schema,
                        source_table=table
                    )
            
            # Wait between batches if not the last batch
            if i + self.config.batch_size < len(dtfx_files):
                time.sleep(15)  # Wait 15 seconds between batches 

    def execute_transfers(
        self,
        source_schema: Union[str, List[str]],
        source_table: Union[str, List[str]],
        sql_statement: Union[str, List[str]],
        output_directory: Optional[str] = None,
        return_dataframe: bool = False
    ) -> Union[List[DataTransferResult], pd.DataFrame]:
        """Execute data transfers and collect all results.
        
        This is a convenience method that executes the transfer_data generator and collects
        all results. It can return either a list of DataTransferResult objects or a pandas
        DataFrame containing all results.
        
        Args:
            source_schema: Source schema name or list of schema names
            source_table: Source table name or list of table names
            sql_statement: SQL statement or list of SQL statements
            output_directory: Optional output directory (defaults to config)
            return_dataframe: If True, returns results as a pandas DataFrame (default: False)
            
        Returns:
            Union[List[DataTransferResult], pd.DataFrame]: List of transfer results or DataFrame
            
        Raises:
            ConfigurationError: If there's an error in the configuration
            ValidationError: If the transfer fails
            
        Examples:
            # Execute transfers and get list of results
            results = dtm.execute_transfers(
                source_schema=["SCHEMA1", "SCHEMA2"],
                source_table=["TABLE1", "TABLE2"],
                sql_statement=["SELECT * FROM SCHEMA1.TABLE1", "SELECT * FROM SCHEMA2.TABLE2"]
            )
            
            # Check results
            for result in results:
                if result.is_successful:
                    print(f"Successfully transferred {result.row_count} rows to {result.file_path}")
                else:
                    print(f"Transfer failed: {result.error}")
            
            # Execute transfers and get DataFrame
            df = dtm.execute_transfers(
                source_schema=["SCHEMA1", "SCHEMA2"],
                source_table=["TABLE1", "TABLE2"],
                sql_statement=["SELECT * FROM SCHEMA1.TABLE1", "SELECT * FROM SCHEMA2.TABLE2"],
                return_dataframe=True
            )
            
            # Analyze results
            print(f"Total successful transfers: {df[df['success']].shape[0]}")
            print(f"Total rows transferred: {df['row_count'].sum()}")
        """
        batch_start_time = datetime.now()
        
        results = list(self.transfer_data(
            source_schema=source_schema,
            source_table=source_table,
            sql_statement=sql_statement,
            output_directory=output_directory
        ))
        
        batch_end_time = datetime.now()
        batch_duration = (batch_end_time - batch_start_time).total_seconds()
        
        # Add batch timing information to each result
        for result in results:
            result.batch_start_time = batch_start_time
            result.batch_end_time = batch_end_time
            result.batch_duration = batch_duration
        
        if return_dataframe:
            df = pd.concat([result.to_dataframe() for result in results], ignore_index=True)
            # Add batch timing information to DataFrame
            df['batch_start_time'] = batch_start_time
            df['batch_end_time'] = batch_end_time
            df['batch_duration'] = batch_duration
            return df
        return results 
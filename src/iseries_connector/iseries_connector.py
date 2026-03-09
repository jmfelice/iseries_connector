import pandas as pd
import pyodbc
import os
import warnings
import time
import logging
from typing import Union, List, Dict, Optional, Any, Generator
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from .exceptions import (
    ISeriesConnectorError,
    ConnectionError,
    QueryError,
    ValidationError
)

# Configure module logger
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)

@dataclass
class ISeriesConfig:
    """
    Configuration for iSeries connection.
    
    This class can be initialized with direct values or from environment variables.
    Environment variables take precedence over direct values.
    
    Environment Variables:
        ISERIES_DSN: The Data Source Name for the iSeries connection
        ISERIES_USERNAME: The username for authentication
        ISERIES_PASSWORD: The password for authentication
        ISERIES_TIMEOUT: Connection timeout in seconds
        ISERIES_MAX_RETRIES: Maximum number of connection retries
        ISERIES_RETRY_DELAY: Delay between retries in seconds
    
    Examples:
    
        # Direct initialization
        config = ISeriesConfig(
            dsn="MY_ISERIES_DSN",
            username="admin",
            password="secret"
        )

        # From environment variables
        config = ISeriesConfig.from_env()

        # Mixed initialization
        config = ISeriesConfig(dsn="MY_DSN").from_env()
        
    """
    dsn: str
    username: str
    password: str
    timeout: int = 30
    max_retries: int = 3
    retry_delay: int = 5
    
    @classmethod
    def from_env(cls) -> 'ISeriesConfig':
        """Create a configuration from environment variables.
        
        Returns:
            ISeriesConfig: A new configuration instance
            
        Raises:
            ValueError: If required environment variables are missing
        """
        return cls(
            dsn=os.environ.get('ISERIES_DSN', ''),
            username=os.environ.get('ISERIES_USERNAME', ''),
            password=os.environ.get('ISERIES_PASSWORD', ''),
            timeout=int(os.environ.get('ISERIES_TIMEOUT', '30')),
            max_retries=int(os.environ.get('ISERIES_MAX_RETRIES', '3')),
            retry_delay=int(os.environ.get('ISERIES_RETRY_DELAY', '5'))
        )
    
    def validate(self) -> None:
        """Validate the configuration parameters.
        
        Raises:
            ValidationError: If any required parameters are missing or invalid
        """
        if not self.dsn:
            raise ValidationError("DSN cannot be empty")
        if not self.username:
            raise ValidationError("Username cannot be empty")
        if not self.password:
            raise ValidationError("Password cannot be empty")
        if self.timeout <= 0:
            raise ValidationError("Timeout must be a positive number")
        if self.max_retries < 0:
            raise ValidationError("Max retries cannot be negative")
        if self.retry_delay < 0:
            raise ValidationError("Retry delay cannot be negative")

class ISeriesConn:
    """
    A class to handle iSeries database connections and operations.
    Implements context manager protocol for safe resource management.
    
    Testing:
        For testing purposes, you can override the following methods:
        - _get_connection(): Override to return a mock connection
        - _get_cursor(): Override to return a mock cursor
        
        Example:
        
            class MockISeriesConn(ISeriesConn):
                def _get_connection(self):
                    return MockConnection()
                    
    """
    
    def __init__(
        self,
        dsn: str,
        username: str,
        password: str,
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: int = 5
    ):
        """
        Initialize the ISeriesConn class with database credentials.

        Args:
            dsn (str): The Data Source Name for the iSeries connection
            username (str): The username for authentication
            password (str): The password for authentication
            timeout (int): Connection timeout in seconds (default: 30)
            max_retries (int): Maximum number of connection retries (default: 3)
            retry_delay (int): Delay between retries in seconds (default: 5)
        """
        self.config = ISeriesConfig(
            dsn=dsn,
            username=username,
            password=password,
            timeout=timeout,
            max_retries=max_retries,
            retry_delay=retry_delay
        )
        self.conn: Optional[pyodbc.Connection] = None
        self.echo: bool = False
        self._validate_config()
    
    def __str__(self) -> str:
        """Return a user-friendly string representation of the connection.
        
        Returns:
            str: A string describing the connection state and configuration
        """
        status = "Connected" if self.conn is not None else "Disconnected"
        return (
            f"ISeriesConn(dsn='{self.config.dsn}', "
            f"username='{self.config.username}', "
            f"status='{status}', "
            f"timeout={self.config.timeout}s, "
            f"max_retries={self.config.max_retries})"
        )

    def __repr__(self) -> str:
        """Return a detailed string representation of the connection.
        
        Returns:
            str: A detailed string representation including all configuration parameters
        """
        return (
            f"ISeriesConn("
            f"dsn='{self.config.dsn}', "
            f"username='{self.config.username}', "
            f"timeout={self.config.timeout}, "
            f"max_retries={self.config.max_retries}, "
            f"retry_delay={self.config.retry_delay}, "
            f"echo={self.echo}, "
            f"connected={self.conn is not None}"
            f")"
        )
    
    def _validate_config(self) -> None:
        """Validate the configuration parameters"""
        self.config.validate()

    def _get_connection(self) -> pyodbc.Connection:
        """Get a database connection. Override this method for testing.
        
        Returns:
            pyodbc.Connection: A database connection instance
            
        Raises:
            ConnectionError: If there's an error establishing the connection
        """
        return pyodbc.connect(
            f"DSN={self.config.dsn};UID={self.config.username};PWD={self.config.password}"
        )
    
    def _get_cursor(self) -> pyodbc.Cursor:
        """Get a database cursor. Override this method for testing.
        
        Returns:
            pyodbc.Cursor: A database cursor instance
            
        Raises:
            ConnectionError: If there's no active connection
        """
        if self.conn is None:
            raise ConnectionError("No active database connection")
        return self.conn.cursor()
    
    @property
    def connection(self) -> pyodbc.Connection:
        """Get the current database connection.
        
        Returns:
            pyodbc.Connection: The current database connection
            
        Raises:
            ConnectionError: If there's no active connection
        """
        if self.conn is None:
            raise ConnectionError("No active database connection")
        return self.conn
    
    @property
    def cursor(self) -> pyodbc.Cursor:
        """Get the current database cursor.
        
        Returns:
            pyodbc.Cursor: The current database cursor
            
        Raises:
            ConnectionError: If there's no active connection
        """
        return self._get_cursor()

    def connect(self) -> pyodbc.Connection:
        """
        Establishes a connection to an iSeries database using credentials.
        Implements retry logic for transient failures.

        Returns:
            pyodbc.Connection: A connection object to the iSeries database

        Raises:
            ConnectionError: If there's an error establishing the connection after retries
        """
        for attempt in range(self.config.max_retries):
            try:
                self.conn = self._get_connection()
                return self.conn
            except Exception as e:
                if attempt == self.config.max_retries - 1:
                    raise ConnectionError(f"Failed to connect after {self.config.max_retries} attempts: {str(e)}")
                logger.warning(f"Connection attempt {attempt + 1} failed: {str(e)}")
                time.sleep(self.config.retry_delay)

    def __enter__(self) -> 'ISeriesConn':
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit"""
        self.close()

    def close(self) -> None:
        """
        Closes the database connection if it exists.
        """
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception as e:
                logger.error(f"Error closing connection: {str(e)}")
            finally:
                self.conn = None

    def fetch(
        self,
        query: str,
        echo: Optional[bool] = None,
        chunksize: Optional[int] = None
    ) -> Union[pd.DataFrame, Generator[pd.DataFrame, None, None]]:
        """
        Executes a SQL query using the connection and returns the results.

        Args:
            query (str): The SQL query to be executed
            echo (Optional[bool]): Whether to print the query before execution
            chunksize (Optional[int]): Size of chunks to read data in. If None, reads all data at once.
                                     When specified, returns a generator of DataFrames.

        Returns:
            Union[pd.DataFrame, Generator[pd.DataFrame, None, None]]: 
                If chunksize is None, returns the result as a DataFrame.
                If chunksize is specified, returns a generator of DataFrames.

        Raises:
            QueryError: If there's an error executing the query
            ConnectionError: If there's an error with the database connection
        """
        if self.conn is None:
            self.connect()

        query = query.replace(";", "")
        echo = echo if echo is not None else self.echo

        if echo:
            logger.info(f"Executing query: {query}")

        try:
            if chunksize:
                return pd.read_sql(sql=query, con=self.conn, chunksize=chunksize)
            else:
                return pd.read_sql(sql=query, con=self.conn)
        
        except Exception as e:
            raise QueryError(f"Error executing query: {str(e)}")

    def execute_statements(
        self, 
        statements: Union[str, List[str]], 
        parallel: bool = False,
        echo: Optional[bool] = None
    ) -> List[Dict[str, Any]]:
        """
        Executes one or more SQL statements in iSeries and returns the results.

        Args:
            statements (Union[str, List[str]]): A single SQL statement or list of SQL statements
            parallel (bool): If True, executes statements in parallel using separate connections.
                           If False, executes statements sequentially using the same connection.
            echo (Optional[bool]): Whether to print the query before execution

        Returns:
            List[Dict[str, Any]]: List of dictionaries containing execution results.
            Each dictionary has the following keys:
                - 'success' (bool): Whether the statement executed successfully
                - 'statement' (str): The SQL statement that was executed
                - 'error' (str): Error message if execution failed (only present if success is False)
                - 'duration' (float): Time taken to execute the statement in seconds
        """
        if isinstance(statements, str):
            statements = [statements]

        start_time = time.time()
        echo = echo if echo is not None else self.echo

        if not parallel:
            if self.conn is None:
                self.connect()

            results = []
            for statement in statements:
                sanitized_statement = statement.replace(";", "")
                statement_start_time = time.time()
                with self.conn.cursor() as cursor:
                    try:
                        cursor.execute(sanitized_statement)
                        results.append({
                            "success": True,
                            "statement": sanitized_statement,
                            "duration": time.time() - statement_start_time
                        })
                        if echo:
                            logger.info(
                                f"Success: Statement = {sanitized_statement}, "
                                f"Duration = {time.time() - statement_start_time} seconds"
                            )
                    except Exception as e:
                        results.append({
                            "success": False,
                            "statement": sanitized_statement,
                            "error": str(e),
                            "duration": time.time() - statement_start_time
                        })
                        if echo:
                            logger.error(
                                f"Failed: Statement = {sanitized_statement}, "
                                f"Duration = {time.time() - statement_start_time} seconds"
                            )
        else:
            with ThreadPoolExecutor(max_workers=len(statements)) as executor:
                results = list(executor.map(self._execute_single_statement, statements))

        end_time = time.time()
        if echo:
            logger.info(f"Total time taken to execute {len(statements)} statements: {end_time - start_time} seconds")
        return results

    def _execute_single_statement(self, stmt: str) -> Dict[str, Any]:
        """
        Execute a single statement in its own connection.

        Args:
            stmt (str): The SQL statement to execute

        Returns:
            Dict[str, Any]: Dictionary containing execution results with keys:
                - 'success' (bool): Whether the statement executed successfully
                - 'statement' (str): The SQL statement that was executed
                - 'error' (str): Error message if execution failed (only present if success is False)
                - 'duration' (float): Time taken to execute the statement in seconds
        """
        statement_start_time = time.time()

        # Create a new connection for this thread
        conn = self._get_connection()
        sanitized_stmt = stmt.replace(";", "")

        try:
            cursor = conn.cursor()
            cursor.execute(sanitized_stmt)
            conn.commit()
            result = {
                "statement": sanitized_stmt,
                "success": True,
                "duration": time.time() - statement_start_time
            }
            if self.echo:
                logger.info(
                    f"Success: Statement = {sanitized_stmt}, "
                    f"Duration = {time.time() - statement_start_time} seconds"
                )
            return result

        except Exception as e:
            result = {
                "statement": sanitized_stmt,
                "success": False,
                "error": str(e),
                "duration": time.time() - statement_start_time
            }
            if self.echo:
                logger.error(
                    f"Failed: Statement = {sanitized_stmt}, "
                    f"Duration = {time.time() - statement_start_time} seconds"
                )
            return result
        
        finally:
            cursor.close()
            conn.close()

    def _parse_sql_file(self, path: str) -> List[str]:
        """
        Read a SQL file and split it into individual statements.

        Splits on semicolons and strips whitespace, ignoring empty statements.

        Args:
            path (str): Path to the SQL file.

        Returns:
            List[str]: List of SQL statements.

        Raises:
            ISeriesConnectorError: If the file cannot be read.
        """
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError as e:
            raise ISeriesConnectorError(f"Failed to read SQL file '{path}': {str(e)}") from e

        parts = content.split(";")
        statements: List[str] = []
        for part in parts:
            stmt = part.strip()
            if not stmt:
                continue
            statements.append(stmt)
        return statements

    def _execute_statements_sequential_on_connection(
        self,
        conn: pyodbc.Connection,
        statements: List[str],
        echo: bool
    ) -> List[Dict[str, Any]]:
        """
        Execute multiple SQL statements sequentially on a given connection.

        Args:
            conn (pyodbc.Connection): The database connection to use.
            statements (List[str]): List of SQL statements to execute.
            echo (bool): Whether to log execution details.

        Returns:
            List[Dict[str, Any]]: Execution results for each statement.
        """
        results: List[Dict[str, Any]] = []

        for statement in statements:
            sanitized_statement = statement.replace(";", "")
            statement_start_time = time.time()
            cursor = conn.cursor()
            try:
                cursor.execute(sanitized_statement)
                conn.commit()
                results.append(
                    {
                        "success": True,
                        "statement": sanitized_statement,
                        "duration": time.time() - statement_start_time,
                    }
                )
                if echo:
                    logger.info(
                        f"Success: Statement = {sanitized_statement}, "
                        f"Duration = {time.time() - statement_start_time} seconds"
                    )
            except Exception as e:
                results.append(
                    {
                        "success": False,
                        "statement": sanitized_statement,
                        "error": str(e),
                        "duration": time.time() - statement_start_time,
                    }
                )
                if echo:
                    logger.error(
                        f"Failed: Statement = {sanitized_statement}, "
                        f"Duration = {time.time() - statement_start_time} seconds"
                    )
            finally:
                cursor.close()

        return results

    def _execute_file_in_new_connection(self, path: str, echo: bool) -> List[Dict[str, Any]]:
        """
        Execute all statements from a SQL file using a new dedicated connection.

        Statements within the file are always executed sequentially.

        Args:
            path (str): Path to the SQL file.
            echo (bool): Whether to log execution details.

        Returns:
            List[Dict[str, Any]]: Execution results for each statement in the file.
        """
        statements = self._parse_sql_file(path)
        file_start_time = time.time()

        conn = self._get_connection()
        try:
            results = self._execute_statements_sequential_on_connection(conn, statements, echo)
        finally:
            conn.close()

        if echo:
            logger.info(
                f"Total time taken to execute {len(statements)} statements from file "
                f"{path}: {time.time() - file_start_time} seconds"
            )
        return results

    def execute_statements_from_files(
        self,
        files: Union[str, List[str]],
        parallel_files: bool = False,
        echo: Optional[bool] = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute SQL statements loaded from one or more files.

        Within each file, statements are always executed sequentially.
        Across files, execution can be sequential or parallel.

        Args:
            files (Union[str, List[str]]): A single file path or list of file paths.
            parallel_files (bool): If True, execute different files in parallel, each
                using its own dedicated connection. If False, execute files sequentially
                using the existing connection.
            echo (Optional[bool]): Whether to log execution details.

        Returns:
            List[Dict[str, Any]]: List of execution results for all statements across
            all files.
        """
        if isinstance(files, str):
            file_paths: List[str] = [files]
        else:
            file_paths = list(files)

        if not file_paths:
            return []

        start_time = time.time()
        effective_echo = echo if echo is not None else self.echo

        results: List[Dict[str, Any]] = []

        if not parallel_files:
            if self.conn is None:
                self.connect()

            for path in file_paths:
                try:
                    statements = self._parse_sql_file(path)
                except Exception as e:
                    error_result = {
                        "success": False,
                        "statement": f"-- file: {path}",
                        "error": str(e),
                        "duration": 0.0,
                    }
                    results.append(error_result)
                    if effective_echo:
                        logger.error(
                            f"Failed to process SQL file {path}: {str(e)}"
                        )
                    continue

                file_results = self.execute_statements(
                    statements, parallel=False, echo=effective_echo
                )
                results.extend(file_results)
        else:

            def _run_file(path: str) -> List[Dict[str, Any]]:
                try:
                    return self._execute_file_in_new_connection(path, effective_echo)
                except Exception as e:
                    if effective_echo:
                        logger.error(
                            f"Failed to process SQL file {path} in parallel: {str(e)}"
                        )
                    return [
                        {
                            "success": False,
                            "statement": f"-- file: {path}",
                            "error": str(e),
                            "duration": 0.0,
                        }
                    ]

            with ThreadPoolExecutor(max_workers=len(file_paths)) as executor:
                for file_results in executor.map(_run_file, file_paths):
                    results.extend(file_results)

        if effective_echo:
            logger.info(
                f"Total time taken to execute {len(results)} statements "
                f"from {len(file_paths)} file(s): {time.time() - start_time} seconds"
            )
        return results

    def fetch_from_file(
        self,
        path: str,
    ) -> Union[pd.DataFrame, Generator[pd.DataFrame, None, None]]:
        """
        Execute a single-query SQL file and return its result set.

        The file must contain exactly one non-empty SQL statement when split on
        semicolons. If multiple statements are found, a ValidationError is raised.

        Args:
            path (str): Path to the SQL file containing a single query.

        Returns:
            Union[pd.DataFrame, Generator[pd.DataFrame, None, None]]: Query result,
            mirroring the return type of ``fetch``.

        Raises:
            ValidationError: If the file does not contain exactly one statement.
            ISeriesConnectorError: If the file cannot be read.
        """
        statements = self._parse_sql_file(path)

        if not statements:
            raise ValidationError(
                f"SQL file '{path}' does not contain a valid SQL statement"
            )
        if len(statements) > 1:
            raise ValidationError(
                f"SQL file '{path}' contains multiple SQL statements; "
                "fetch_from_file expects exactly one."
            )

        query = statements[0]
        return self.fetch(query)
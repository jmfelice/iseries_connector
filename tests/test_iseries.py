"""Tests for ISeriesConn class."""

import pytest
import os
from unittest.mock import Mock, patch
import pandas as pd
from iseries_connector import ISeriesConn, ISeriesConfig, load_env
from iseries_connector.exceptions import (
    ConnectionError,
    ISeriesConnectorError,
    QueryError,
    ValidationError,
)

# Test data
TEST_CONFIG = {
    'dsn': 'TEST_ISERIES_DSN',
    'username': 'testuser',
    'password': 'testpass',
    'timeout': 30,
    'max_retries': 3,
    'retry_delay': 5
}

@pytest.fixture
def mock_connection():
    """Fixture to create a mock iSeries connection"""
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn

@pytest.fixture
def mock_cursor():
    """Fixture to create a mock iSeries cursor"""
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    return mock_cursor

class TestISeriesConfig:
    def test_init_with_direct_values(self):
        """Test initialization with direct values"""
        config = ISeriesConfig(**TEST_CONFIG)
        assert config.dsn == TEST_CONFIG['dsn']
        assert config.username == TEST_CONFIG['username']
        assert config.password == TEST_CONFIG['password']
        assert config.timeout == TEST_CONFIG['timeout']
        assert config.max_retries == TEST_CONFIG['max_retries']
        assert config.retry_delay == TEST_CONFIG['retry_delay']

    def test_init_with_defaults(self):
        """Test initialization with default values"""
        config = ISeriesConfig(
            dsn='TEST_DSN',
            username='test-user',
            password='test-pass'
        )
        assert config.timeout == 30
        assert config.max_retries == 3
        assert config.retry_delay == 5

    def test_from_env(self):
        """Test initialization from environment variables"""
        env_vars = {
            'ISERIES_DSN': 'ENV_DSN',
            'ISERIES_USERNAME': 'env-user',
            'ISERIES_PASSWORD': 'env-pass',
            'ISERIES_TIMEOUT': '20',
            'ISERIES_MAX_RETRIES': '5',
            'ISERIES_RETRY_DELAY': '10'
        }
        
        with patch.dict(os.environ, env_vars):
            config = ISeriesConfig.from_env()
            assert config.dsn == 'ENV_DSN'
            assert config.username == 'env-user'
            assert config.password == 'env-pass'
            assert config.timeout == 20
            assert config.max_retries == 5
            assert config.retry_delay == 10


class TestEnvLoading:
    def test_load_env_from_default_file(self, tmp_path, monkeypatch):
        """load_env should populate os.environ from a .env file in CWD."""
        dotenv_path = tmp_path / ".env"
        dotenv_path.write_text(
            "ISERIES_DSN=DOTENV_DSN\n"
            "ISERIES_USERNAME=dotenv-user\n"
            "ISERIES_PASSWORD=dotenv-pass\n"
        )

        monkeypatch.chdir(tmp_path)
        for key in ("ISERIES_DSN", "ISERIES_USERNAME", "ISERIES_PASSWORD"):
            monkeypatch.delenv(key, raising=False)

        load_env()

        assert os.environ["ISERIES_DSN"] == "DOTENV_DSN"
        assert os.environ["ISERIES_USERNAME"] == "dotenv-user"
        assert os.environ["ISERIES_PASSWORD"] == "dotenv-pass"

    def test_from_env_uses_dotenv_when_env_missing(self, tmp_path, monkeypatch):
        """.from_env should fall back to values from a .env file."""
        dotenv_path = tmp_path / ".env"
        dotenv_path.write_text(
            "ISERIES_DSN=DOTENV_DSN\n"
            "ISERIES_USERNAME=dotenv-user\n"
            "ISERIES_PASSWORD=dotenv-pass\n"
            "ISERIES_TIMEOUT=15\n"
            "ISERIES_MAX_RETRIES=7\n"
            "ISERIES_RETRY_DELAY=2\n"
        )

        monkeypatch.chdir(tmp_path)
        for key in (
            "ISERIES_DSN",
            "ISERIES_USERNAME",
            "ISERIES_PASSWORD",
            "ISERIES_TIMEOUT",
            "ISERIES_MAX_RETRIES",
            "ISERIES_RETRY_DELAY",
        ):
            monkeypatch.delenv(key, raising=False)

        config = ISeriesConfig.from_env()

        assert config.dsn == "DOTENV_DSN"
        assert config.username == "dotenv-user"
        assert config.password == "dotenv-pass"
        assert config.timeout == 15
        assert config.max_retries == 7
        assert config.retry_delay == 2

    @pytest.mark.parametrize(
        "invalid_field,invalid_value,expected_error",
        [
            ('dsn', '', "DSN cannot be empty"),
            ('username', '', "Username cannot be empty"),
            ('password', '', "Password cannot be empty"),
            ('timeout', 0, "Timeout must be a positive number"),
            ('max_retries', -1, "Max retries cannot be negative"),
            ('retry_delay', -1, "Retry delay cannot be negative"),
        ]
    )
    def test_validate_invalid_config(self, invalid_field, invalid_value, expected_error):
        """Test validation of invalid configurations"""
        config_dict = TEST_CONFIG.copy()
        config_dict[invalid_field] = invalid_value
        with pytest.raises(ValidationError) as exc_info:
            config = ISeriesConfig(**config_dict)
            config.validate()
        assert str(exc_info.value) == expected_error

class TestISeriesConn:
    @pytest.fixture
    def iseries_conn(self):
        """Fixture to create an ISeriesConn instance"""
        return ISeriesConn(**TEST_CONFIG)

    def test_init(self, iseries_conn):
        """Test initialization of ISeriesConn"""
        assert iseries_conn.config.dsn == TEST_CONFIG['dsn']
        assert iseries_conn.config.username == TEST_CONFIG['username']
        assert iseries_conn.config.password == TEST_CONFIG['password']
        assert iseries_conn.conn is None
        assert iseries_conn.echo is False

    @patch('iseries_connector.iseries_connector.pyodbc.connect')
    def test_connect_success(self, mock_connect, iseries_conn, mock_connection):
        """Test successful connection"""
        mock_connect.return_value = mock_connection
        conn = iseries_conn.connect()
        assert conn == mock_connection
        assert iseries_conn.conn == mock_connection
        mock_connect.assert_called_once_with(
            f"DSN={TEST_CONFIG['dsn']};UID={TEST_CONFIG['username']};PWD={TEST_CONFIG['password']}"
        )

    @patch('iseries_connector.iseries_connector.pyodbc.connect')
    def test_connect_retry(self, mock_connect, iseries_conn, mock_connection):
        """Test connection with retry"""
        mock_connect.side_effect = [Exception("Connection failed"), mock_connection]
        conn = iseries_conn.connect()
        assert conn == mock_connection
        assert mock_connect.call_count == 2

    @patch('iseries_connector.iseries_connector.pyodbc.connect')
    def test_connect_failure(self, mock_connect, iseries_conn):
        """Test connection failure after all retries"""
        mock_connect.side_effect = Exception("Connection failed")
        with pytest.raises(ConnectionError) as exc_info:
            iseries_conn.connect()
        assert "Failed to connect after" in str(exc_info.value)

    def test_context_manager(self, iseries_conn, mock_connection):
        """Test context manager functionality"""
        with patch('iseries_connector.iseries_connector.pyodbc.connect', return_value=mock_connection):
            with iseries_conn as conn:
                assert conn == iseries_conn
                assert iseries_conn.conn == mock_connection
            assert iseries_conn.conn is None

    def test_close(self, iseries_conn, mock_connection):
        """Test connection close"""
        iseries_conn.conn = mock_connection
        iseries_conn.close()
        assert iseries_conn.conn is None
        mock_connection.close.assert_called_once()

    @patch('pandas.read_sql')
    @patch('iseries_connector.iseries_connector.pyodbc.connect')
    def test_fetch_success(self, mock_connect, mock_read_sql, iseries_conn, mock_connection):
        """Test successful query execution"""
        mock_connect.return_value = mock_connection
        expected_df = pd.DataFrame({'col1': [1, 2, 3]})
        mock_read_sql.return_value = expected_df
        
        result = iseries_conn.fetch("SELECT * FROM test_table")
        assert result.equals(expected_df)
        mock_read_sql.assert_called_once()
        mock_connect.assert_called_once()

    @patch('pandas.read_sql')
    @patch('iseries_connector.iseries_connector.pyodbc.connect')
    def test_fetch_with_chunksize(self, mock_connect, mock_read_sql, iseries_conn, mock_connection):
        """Test query execution with chunksize"""
        mock_connect.return_value = mock_connection
        expected_dfs = [
            pd.DataFrame({'col1': [1, 2]}),
            pd.DataFrame({'col1': [3, 4]})
        ]
        mock_read_sql.return_value = expected_dfs
        
        result = iseries_conn.fetch("SELECT * FROM test_table", chunksize=2)
        assert list(result) == expected_dfs
        mock_read_sql.assert_called_once()
        mock_connect.assert_called_once()

    @patch('iseries_connector.iseries_connector.pyodbc.connect')
    def test_fetch_no_connection(self, mock_connect, iseries_conn):
        """Test query execution without connection"""
        mock_connect.side_effect = Exception("Connection failed")
        with pytest.raises(ConnectionError) as exc_info:
            iseries_conn.fetch("SELECT * FROM test_table")
        assert "Failed to connect after" in str(exc_info.value)

    @patch('pandas.read_sql')
    def test_fetch_error(self, mock_read_sql, iseries_conn, mock_connection):
        """Test query execution with error"""
        mock_read_sql.side_effect = Exception("Query failed")
        iseries_conn.conn = mock_connection
        
        with pytest.raises(QueryError) as exc_info:
            iseries_conn.fetch("SELECT * FROM test_table")
        assert "Error executing query" in str(exc_info.value)

    @patch('iseries_connector.iseries_connector.pyodbc.connect')
    def test_execute_statements_sequential(self, mock_connect, iseries_conn, mock_connection, mock_cursor):
        """Test sequential statement execution"""
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        statements = [
            "UPDATE table1 SET col1 = 'value1';",
            "UPDATE table2 SET col2 = 'value2';"
        ]
        
        results = iseries_conn.execute_statements(statements, parallel=False)
        assert len(results) == 2
        assert all(r['success'] for r in results)
        assert mock_cursor.execute.call_count == 2
        # Ensure semicolons are stripped before execution
        executed_statements = [call.args[0] for call in mock_cursor.execute.call_args_list]
        assert all(";" not in s for s in executed_statements)
        mock_cursor.__enter__.assert_called()
        mock_cursor.__exit__.assert_called()
        mock_connect.assert_called_once()

    def test_execute_statements_parallel(self, iseries_conn):
        """Test parallel statement execution"""
        with patch.object(iseries_conn, '_execute_single_statement') as mock_execute:
            mock_execute.return_value = {
                'success': True,
                'statement': 'test',
                'duration': 0.1
            }
            
            statements = [
                "UPDATE table1 SET col1 = 'value1'",
                "UPDATE table2 SET col2 = 'value2'"
            ]
            
            results = iseries_conn.execute_statements(statements, parallel=True)
            assert len(results) == 2
            assert all(r['success'] for r in results)
            assert mock_execute.call_count == 2

    def test_execute_statements_error(self, iseries_conn, mock_connection, mock_cursor):
        """Test statement execution with error"""
        iseries_conn.conn = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Execution failed")
        
        results = iseries_conn.execute_statements("UPDATE test SET col = 'value'")
        assert len(results) == 1
        assert not results[0]['success']
        assert "Execution failed" in results[0]['error']
        mock_cursor.__enter__.assert_called()
        mock_cursor.__exit__.assert_called()

    def test_execute_single_statement_strips_semicolon(self, iseries_conn, mock_connection, mock_cursor):
        """Test that _execute_single_statement strips semicolons before execution"""
        with patch.object(iseries_conn, '_get_connection', return_value=mock_connection):
            mock_connection.cursor.return_value = mock_cursor

            stmt = "UPDATE table1 SET col1 = 'value1';"
            result = iseries_conn._execute_single_statement(stmt)

            # Verify execution used sanitized statement
            mock_cursor.execute.assert_called_once()
            executed_stmt = mock_cursor.execute.call_args[0][0]
            assert ";" not in executed_stmt

            # Result should also contain sanitized statement
            assert result["success"] is True
            assert ";" not in result["statement"]

    def test_echo_property(self, iseries_conn):
        """Test echo property functionality"""
        iseries_conn.echo = True
        assert iseries_conn.echo is True
        
        mock_conn = Mock()
        with patch('iseries_connector.iseries_connector.pyodbc.connect', return_value=mock_conn), \
             patch('pandas.read_sql', return_value=pd.DataFrame()), \
             patch('iseries_connector.iseries_connector.logger.info') as mock_logger:
            iseries_conn.fetch("SELECT * FROM test_table", echo=True)
            mock_logger.assert_called()

    def test_connection_property(self, iseries_conn, mock_connection):
        """Test connection property"""
        iseries_conn.conn = mock_connection
        assert iseries_conn.connection == mock_connection
        
        iseries_conn.conn = None
        with pytest.raises(ConnectionError):
            _ = iseries_conn.connection

    def test_cursor_property(self, iseries_conn, mock_connection, mock_cursor):
        """Test cursor property"""
        iseries_conn.conn = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        assert iseries_conn.cursor == mock_cursor
        
        iseries_conn.conn = None
        with pytest.raises(ConnectionError):
            _ = iseries_conn.cursor 

    def test_parse_sql_file_splits_statements(self, iseries_conn, tmp_path):
        """Test that _parse_sql_file splits content on semicolons and strips whitespace"""
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(
            "SELECT 1;\n\n  SELECT 2 ;\n-- comment line\nSELECT 3;  \n"
        )

        statements = iseries_conn._parse_sql_file(str(sql_file))
        assert statements == ["SELECT 1", "SELECT 2", "-- comment line\nSELECT 3"]

    def test_parse_sql_file_missing_raises(self, iseries_conn):
        """Test that _parse_sql_file raises on missing file"""
        with pytest.raises(ISeriesConnectorError):
            iseries_conn._parse_sql_file("nonexistent.sql")

    def test_execute_statements_from_files_sequential(self, iseries_conn):
        """Test sequential execution across multiple files"""
        files = ["file1.sql", "file2.sql"]

        with patch.object(
            iseries_conn, "_parse_sql_file"
        ) as mock_parse, patch.object(
            iseries_conn, "execute_statements"
        ) as mock_execute:
            mock_parse.side_effect = [
                ["stmt1_file1", "stmt2_file1"],
                ["stmt1_file2"],
            ]

            def _execute_side_effect(statements, parallel, echo=None):
                return [
                    {"success": True, "statement": s, "duration": 0.1}
                    for s in statements
                ]

            mock_execute.side_effect = _execute_side_effect

            results = iseries_conn.execute_statements_from_files(
                files, parallel_files=False
            )

            assert [r["statement"] for r in results] == [
                "stmt1_file1",
                "stmt2_file1",
                "stmt1_file2",
            ]
            assert mock_execute.call_count == 2
            mock_parse.assert_called()

    def test_execute_statements_from_files_parallel(self, iseries_conn):
        """Test parallel execution across multiple files"""
        files = ["file1.sql", "file2.sql", "file3.sql"]

        with patch.object(
            iseries_conn, "_execute_file_in_new_connection"
        ) as mock_exec:
            mock_exec.side_effect = (
                lambda path, echo: [
                    {
                        "success": True,
                        "statement": f"stmt_for_{path}",
                        "duration": 0.1,
                    }
                ]
            )

            results = iseries_conn.execute_statements_from_files(
                files, parallel_files=True
            )

            assert len(results) == 3
            assert {
                r["statement"] for r in results
            } == {
                "stmt_for_file1.sql",
                "stmt_for_file2.sql",
                "stmt_for_file3.sql",
            }
            assert mock_exec.call_count == 3

    def test_execute_statements_from_files_parse_error(self, iseries_conn):
        """Test error handling when parsing a SQL file fails"""
        with patch.object(
            iseries_conn, "_parse_sql_file", side_effect=ISeriesConnectorError("boom")
        ):
            results = iseries_conn.execute_statements_from_files(
                "file1.sql", parallel_files=False
            )

        assert len(results) == 1
        assert results[0]["success"] is False
        assert results[0]["statement"].startswith("-- file: file1.sql")
        assert "boom" in results[0]["error"]

    def test_fetch_from_file_success(self, iseries_conn, tmp_path):
        """Test successful fetch_from_file execution"""
        sql_file = tmp_path / "query.sql"
        sql_file.write_text("SELECT * FROM test_table;")

        expected_df = pd.DataFrame({"col1": [1, 2, 3]})

        with patch.object(iseries_conn, "fetch", return_value=expected_df) as mock_fetch:
            result = iseries_conn.fetch_from_file(str(sql_file))

        assert result.equals(expected_df)
        mock_fetch.assert_called_once_with("SELECT * FROM test_table")

    def test_fetch_from_file_multiple_statements_error(self, iseries_conn, tmp_path):
        """Test that fetch_from_file raises when file contains multiple statements"""
        sql_file = tmp_path / "multi.sql"
        sql_file.write_text("SELECT 1; SELECT 2;")

        with pytest.raises(ValidationError):
            iseries_conn.fetch_from_file(str(sql_file))

    def test_fetch_from_file_empty_file_error(self, iseries_conn, tmp_path):
        """Test that fetch_from_file raises when file has no valid statements"""
        sql_file = tmp_path / "empty.sql"
        sql_file.write_text("   \n   ")

        with pytest.raises(ValidationError):
            iseries_conn.fetch_from_file(str(sql_file))
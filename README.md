# iSeries Connector

A Python library for connecting to and interacting with IBM iSeries databases. This library provides a simple and efficient way to execute SQL queries and statements against iSeries databases using pyodbc.

## Features

- Easy connection management with automatic retry logic
- Support for both single and parallel statement execution
- Execute SQL scripts from files (sequential within a file, optional parallelism across files)
- Fetch query results directly into pandas DataFrames
- Configurable through environment variables or direct initialization
- Comprehensive error handling and logging
- Typed APIs and documentation
- Data transfer tooling for high-volume exports using IBM ACS

## Requirements

- Python 3.9 or higher
- pyodbc 4.0.0 or higher
- pandas 2.0.0 or higher
 
## Installation

### From PyPI (recommended)

```bash
pip install iseries-connector
```

### From source

```bash
git clone https://github.com/jmfelice/iseries-connector.git
cd iseries-connector
pip install -e .[dev]
```

## Quick Start Examples

### 1. Basic Connection
```python
from iseries_connector import ISeriesConn, ISeriesConfig

# Create a configuration
config = ISeriesConfig(
    dsn="MY_DSN",
    username="user",
    password="pass",
)

# Connect to the database
with ISeriesConn(config=config) as conn:
    print("Connected successfully!")
```

### 2. Fetching Data
```python
from iseries_connector import ISeriesConn, ISeriesConfig

# Load configuration from environment variables (and optional .env file)
config = ISeriesConfig.from_env()

with ISeriesConn(config=config) as conn:
    # Fetch data into a pandas DataFrame
    df = conn.fetch("SELECT * FROM SCHEMA.TABLE WHERE COLUMN = 'value'")
    print(f"Retrieved {len(df)} rows")
```

### 3. Executing SQL Statements
```python
from iseries_connector import ISeriesConn, ISeriesConfig

config = ISeriesConfig.from_env()

with ISeriesConn(config=config) as conn:
    # Single statement
    conn.execute_statements("UPDATE table1 SET col1 = 'value1'")

    # Multiple statements sequentially on one connection
    statements = [
        "UPDATE table1 SET col1 = 'value1'",
        "UPDATE table2 SET col2 = 'value2'",
    ]
    conn.execute_statements(statements, parallel=False)

    # Multiple statements in parallel, each on its own connection
    conn.execute_statements(statements, parallel=True)
```

### 4. Executing SQL from Files
```python
from iseries_connector import ISeriesConn, ISeriesConfig

config = ISeriesConfig.from_env()

with ISeriesConn(config=config) as conn:
    # Single file, statements executed sequentially
    conn.execute_statements_from_files("sql/setup.sql")

    # Multiple files executed sequentially (per-file statements still sequential)
    conn.execute_statements_from_files(
        ["sql/schema.sql", "sql/data.sql"],
        parallel_files=False,
    )

    # Multiple files executed in parallel (per-file statements still sequential)
    conn.execute_statements_from_files(
        ["sql/indexes.sql", "sql/cleanup.sql"],
        parallel_files=True,
    )
```

### 5. Fetching Data from a SQL File
```python
from iseries_connector import ISeriesConn, ISeriesConfig

config = ISeriesConfig.from_env()

with ISeriesConn(config=config) as conn:
    # The file must contain exactly one SQL query
    df = conn.fetch_from_file("sql/top_customers.sql")
    print(f"Retrieved {len(df)} rows")
```

### 6. Data Transfer with IBM ACS
```python
from iseries_connector import DataTransferManager

manager = DataTransferManager(
    host_name="your.hostname.com",
    # acs_launcher_path can be overridden if needed
)

# Single-table transfer
result = next(
    manager.transfer_data(
        source_schema="SRC_SCHEMA",
        source_table="MY_TABLE",
        sql_statement="SELECT * FROM SRC_SCHEMA.MY_TABLE",
    )
)
if result.is_successful:
    print(f"Transferred {result.row_count} rows to {result.file_path}")

# Multiple tables in a batch
schemas = ["SRC_SCHEMA"] * 3
tables = ["TABLE1", "TABLE2", "TABLE3"]
statements = [
    "SELECT * FROM SRC_SCHEMA.TABLE1",
    "SELECT * FROM SRC_SCHEMA.TABLE2",
    "SELECT * FROM SRC_SCHEMA.TABLE3",
]
batch_results = list(
    manager.transfer_data(
        source_schema=schemas,
        source_table=tables,
        sql_statement=statements,
    )
)
for r in batch_results:
    print(f"{r.source_schema}.{r.source_table}: success={r.is_successful}")
```

### 7. Using a `.env` file and `load_env`

You can keep credentials and connection details in a simple `.env` file in your working directory:

```bash
ISERIES_DSN=MY_DSN
ISERIES_USERNAME=user
ISERIES_PASSWORD=pass

# Optional data transfer settings
ISERIES_HOST_NAME=my.host.name
ISERIES_ACS_LAUNCHER_PATH=/path/to/acslaunch_win-64.exe
ISERIES_RAW_DATA_DIR=/path/to/raw_data
ISERIES_DATA_PACKAGE_DIR=/path/to/data_package
```

Then load configuration in Python:

```python
from iseries_connector import (
    ISeriesConn,
    ISeriesConfig,
    DataTransferConfig,
    DataTransferManager,
)

# ISeriesConn configuration from .env / environment
conn_config = ISeriesConfig.from_env()
with ISeriesConn(config=conn_config) as conn:
    df = conn.fetch("SELECT * FROM SCHEMA.TABLE")

# Data transfer configuration from .env / environment
dt_config = DataTransferConfig.from_env()
manager = DataTransferManager(config=dt_config)
result = next(
    manager.transfer_data(
        source_schema="SCHEMA",
        source_table="TABLE",
        sql_statement="SELECT * FROM SCHEMA.TABLE",
    )
)
print(result)
```

## Configuration

The library can be configured using environment variables:

```bash
# Environment Variables
ISERIES_DSN=MY_DSN
ISERIES_USERNAME=user
ISERIES_PASSWORD=pass
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| dsn | str | - | Data Source Name for the iSeries connection |
| username | str | - | Database username |
| password | str | - | Database password |
| timeout | int | 30 | Connection timeout in seconds |
| max_retries | int | 3 | Maximum number of connection retry attempts |
| retry_delay | int | 5 | Delay between retry attempts in seconds |

## Development

1. Clone the repository:
   ```bash
   git clone https://github.com/jmfelice/iseries-connector.git
   cd iseries-connector
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   .\venv\Scripts\activate   # Windows
   ```

3. Install for development:
   ```bash
   python setup.py develop
   ```

4. Run tests:
   ```bash
   make test
   ```

5. Run linting:
   ```bash
   make lint
   ```

6. Build documentation:
   ```bash
   make docs
   ```

## Testing

The project uses pytest for testing. Run the test suite with:

```bash
pytest
```

For coverage reports:

```bash
pytest --cov=src/iseries_connector
```

## Documentation

Full documentation is available at [Read the Docs](https://iseries-connector.readthedocs.io/).

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

Please ensure your code follows our coding standards and includes appropriate tests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 
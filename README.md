# iSeries Connector

A Python library for connecting to and interacting with IBM iSeries databases. This library provides a simple and efficient way to execute SQL queries and statements against iSeries databases using pyodbc.

## Features

- Easy connection management with automatic retry logic
- Support for both single and parallel statement execution
- Pandas DataFrame integration for query results
- Configurable through environment variables or direct initialization
- Comprehensive error handling and logging
- Type hints and documentation
- Connection pooling and resource management
- Asynchronous query execution support
- Comprehensive test coverage

## Requirements

- Python 3.9 or higher
- pyodbc 4.0.0 or higher
- pandas 2.0.0 or higher

## Installation

Clone the repository and install:

```bash
git clone https://github.com/jmfelice/iseries-connector.git
cd iseries-connector
python setup.py install
```

## Quick Start Examples

### 1. Basic Connection
```python
from iseries_connector import ISeriesConn, ISeriesConfig

# Create a configuration
config = ISeriesConfig(
    dsn="MY_DSN",
    username="user",
    password="pass"
)

# Connect to the database
with ISeriesConn(config) as conn:
    print("Connected successfully!")
```

### 2. Fetching Data
```python
from iseries_connector import ISeriesConn, ISeriesConfig

config = ISeriesConfig.from_env()  # Using environment variables

with ISeriesConn(config) as conn:
    # Fetch data into a pandas DataFrame
    df = conn.fetch("SELECT * FROM SCHEMA.TABLE WHERE COLUMN = 'value'")
    print(f"Retrieved {len(df)} rows")
```

### 3. Single Table Data Transfer
```python
from iseries_connector import ISeriesConn, ISeriesConfig, DataTransferTask

config = ISeriesConfig.from_env()

with ISeriesConn(config) as conn:
    # Create a data transfer task for a single table
    task = DataTransferTask(
        source_schema="SRC_SCHEMA",
        source_table="MY_TABLE",
        target_schema="TGT_SCHEMA"
    )
    
    # Execute the transfer
    result = conn.execute_transfer(task)
    print(f"Transferred {result.rows_transferred} rows")
```

### 4. Multiple Tables Data Transfer
```python
from iseries_connector import ISeriesConn, ISeriesConfig, DataTransferTask

config = ISeriesConfig.from_env()

# List of tables to transfer
tables = ["TABLE1", "TABLE2", "TABLE3"]

with ISeriesConn(config) as conn:
    for table in tables:
        task = DataTransferTask(
            source_schema="SRC_SCHEMA",
            source_table=table,
            target_schema="TGT_SCHEMA"
        )
        
        result = conn.execute_transfer(task)
        print(f"Table {table}: Transferred {result.rows_transferred} rows")
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

Full documentation is available at [Read the Docs](https://jmfelice.github.io/iseries_connector/).

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

Please ensure your code follows our coding standards and includes appropriate tests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 
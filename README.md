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
- IBM iSeries Access ODBC Driver
- pyodbc 4.0.0 or higher
- pandas 2.0.0 or higher

## Installation

```bash
pip install iseries-connector
```

## Quick Start

```python
from iseries_connector import ISeriesConn, ISeriesConfig

# Create a configuration
config = ISeriesConfig(
    dsn="MY_ISERIES_DSN",
    username="admin",
    password="secret",
    timeout=30,
    max_retries=3,
    retry_delay=5
)

# Or use environment variables
config = ISeriesConfig.from_env()

# Connect and execute queries
with ISeriesConn(**config.__dict__) as conn:
    # Execute a query and get results as DataFrame
    df = conn.fetch("SELECT * FROM MYTABLE")
    
    # Execute multiple statements in parallel
    results = conn.execute_statements([
        "UPDATE TABLE1 SET COL1 = 'value1'",
        "UPDATE TABLE2 SET COL2 = 'value2'"
    ], parallel=True)
```

## Configuration

The library can be configured using environment variables or direct initialization:

```python
# Environment Variables
ISERIES_DSN=MY_DSN
ISERIES_USERNAME=admin
ISERIES_PASSWORD=secret
ISERIES_TIMEOUT=30
ISERIES_MAX_RETRIES=3
ISERIES_RETRY_DELAY=5
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
   git clone https://github.com/enterprise-dw/iseries-connector.git
   cd iseries-connector
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   .\venv\Scripts\activate   # Windows
   ```

3. Install development dependencies:
   ```bash
   pip install -e ".[dev]"
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
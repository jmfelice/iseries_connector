# iSeries Connector

A Python library for connecting to and interacting with IBM iSeries databases. This library provides a simple and efficient way to execute SQL queries and statements against iSeries databases using pyodbc.

## Features

- Easy connection management with automatic retry logic
- Support for both single and parallel statement execution
- Pandas DataFrame integration for query results
- Configurable through environment variables or direct initialization
- Comprehensive error handling and logging
- Type hints and documentation

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
    password="secret"
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

## Documentation

Full documentation is available at [Read the Docs](https://iseries-connector.readthedocs.io/).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 
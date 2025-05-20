# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2024-03-19

### Added
- Initial release of iSeries Connector
- Basic connection management with retry logic
- Support for both single and parallel statement execution
- Pandas DataFrame integration for query results
- Environment variable configuration
- Comprehensive error handling and logging
- Type hints and documentation
- Test suite with pytest
- Sphinx documentation

### Features
- `ISeriesConfig` class for configuration management
- `ISeriesConn` class for database operations
- Support for context managers
- Automatic connection retry logic
- Parallel statement execution
- Chunked data processing
- Request ID tracking in logs 
"""Common exceptions for iSeries connector modules."""

class ISeriesConnectorError(Exception):
    """Base exception for all iSeries connector related errors."""
    pass

class ConnectionError(ISeriesConnectorError):
    """Exception raised for connection related errors."""
    pass

class QueryError(ISeriesConnectorError):
    """Exception raised for query execution errors."""
    pass

class ConfigurationError(ISeriesConnectorError):
    """Exception raised for configuration related errors."""
    pass

class ValidationError(ISeriesConnectorError):
    """Exception raised for validation related errors."""
    pass 
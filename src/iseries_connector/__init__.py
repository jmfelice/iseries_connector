"""iSeries Connector - A Python library for connecting to and interacting with IBM iSeries databases."""

from .iseries_connector import ISeriesConn, ISeriesConfig
from .exceptions import ISeriesConnectorError, ConnectionError, QueryError

__version__ = '0.1.0'
__author__ = 'Enterprise DW'
__email__ = 'enterprise.dw@example.com'

__all__ = ['ISeriesConn', 'ISeriesConfig', 'ISeriesConnectorError', 'ConnectionError', 'QueryError']

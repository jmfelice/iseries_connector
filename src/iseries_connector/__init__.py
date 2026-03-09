"""iSeries Connector - A Python library for connecting to and interacting with IBM iSeries databases."""

from .iseries_connector import ISeriesConn, ISeriesConfig, load_env
from .data_transfer import DataTransferConfig, DataTransferManager, DataTransferResult
from .exceptions import ISeriesConnectorError, ConnectionError, QueryError, ValidationError

__version__ = '0.2.0'
__author__ = 'Jared Felice'
__email__ = 'jmfelice@icloud.com'

__all__ = [
    'ISeriesConn',
    'ISeriesConfig',
    'load_env',
    'ISeriesConnectorError',
    'ConnectionError',
    'QueryError',
    'DataTransferConfig',
    'DataTransferResult',
    'DataTransferManager',
    'ValidationError',
]

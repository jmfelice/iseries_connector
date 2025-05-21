"""iSeries Connector - A Python library for connecting to and interacting with IBM iSeries databases."""

from .iseries_connector import ISeriesConn, ISeriesConfig
from .data_transfer import DataTransferConfig, DataTransferResult, DataTransferManager
from .exceptions import ISeriesConnectorError, ConnectionError, QueryError

__version__ = '0.1.0'
__author__ = 'Jared Felice'
__email__ = 'jmfelice@icloud.com'

__all__ = [
    'ISeriesConn', 
    'ISeriesConfig', 
    'ISeriesConnectorError', 
    'ConnectionError', 
    'QueryError', 
    'DataTransferConfig', 
    'DataTransferResult', 
    'DataTransferManager'
    ]

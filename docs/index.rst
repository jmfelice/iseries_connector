.. AWS Connector documentation master file, created by
   sphinx-quickstart on Mon May 19 15:18:14 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to iSeries Connector's documentation!
=========================================

iSeries Connector is a Python library for connecting to and interacting with IBM iSeries databases. It provides a simple and efficient way to execute SQL queries and statements against iSeries databases using pyodbc.

Features
--------

* Easy connection management with automatic retry logic
* Support for both single and parallel statement execution
* Pandas DataFrame integration for query results
* Configurable through environment variables or direct initialization
* Comprehensive error handling and logging
* Type hints and documentation
* Efficient data transfer using IBM Access Client Solutions

Installation
-----------

You can install iSeries Connector using pip:

.. code-block:: bash

   pip install iseries-connector

Quick Start
----------

Here's a quick example of how to use iSeries Connector:

.. code-block:: python

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

Configuration
------------

The library can be configured using environment variables or direct initialization:

.. code-block:: python

   # Environment Variables
   ISERIES_DSN=MY_DSN
   ISERIES_USERNAME=admin
   ISERIES_PASSWORD=secret
   ISERIES_TIMEOUT=30
   ISERIES_MAX_RETRIES=3
   ISERIES_RETRY_DELAY=5

Contents
--------

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   usage
   api
   configuration
   modules/iseries_connector
   modules/data_transfer
   contributing
   changelog

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


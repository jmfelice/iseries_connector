.. AWS Connector documentation master file, created by
   sphinx-quickstart on Mon May 19 15:18:14 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to iSeries Connector's documentation!
=========================================

iSeries Connector is a robust Python library for connecting to and interacting with IBM iSeries databases. It provides a simple, efficient, and type-safe way to execute SQL queries and statements against iSeries databases using pyodbc.

Features
--------

* Easy connection management with automatic retry logic
* Support for both single and parallel statement execution
* Pandas DataFrame integration for query results
* Configurable through environment variables or direct initialization
* Comprehensive error handling and logging
* Full type hints and documentation
* Connection pooling and resource management
* Asynchronous query execution support
* Comprehensive test coverage

Installation
-----------

Clone the repository and install:

.. code-block:: bash

   git clone https://github.com/jmfelice/iseries-connector.git
   cd iseries-connector
   python setup.py install

For development installation:

.. code-block:: bash

   git clone https://github.com/jmfelice/iseries-connector.git
   cd iseries_connector
   python setup.py develop

Quick Start Examples
------------------

1. Basic Connection
~~~~~~~~~~~~~~~~~

.. code-block:: python

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

2. Fetching Data
~~~~~~~~~~~~~~

.. code-block:: python

   from iseries_connector import ISeriesConn, ISeriesConfig

   config = ISeriesConfig.from_env()  # Using environment variables

   with ISeriesConn(config) as conn:
       # Fetch data into a pandas DataFrame
       df = conn.fetch("SELECT * FROM SCHEMA.TABLE WHERE COLUMN = 'value'")
       print(f"Retrieved {len(df)} rows")

3. Single Table Data Transfer
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

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

4. Multiple Tables Data Transfer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

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
   ISERIES_POOL_SIZE=5
   ISERIES_POOL_TIMEOUT=30

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
   examples
   contributing
   changelog

API Reference
------------

.. toctree::
   :maxdepth: 2
   :caption: API Reference:

   modules/iseries_connector
   modules/data_transfer
   modules/exceptions
   modules/utils

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


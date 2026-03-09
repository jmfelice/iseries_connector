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
* Support for both single and parallel SQL statement execution
* Execute SQL scripts from files (sequential within a file, optional parallelism across files)
* Pandas DataFrame integration for query results
* Configurable through environment variables or direct initialization
* Comprehensive error handling and logging
* Full type hints and documentation
* Data transfer tooling for high-volume exports using IBM ACS

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
   with ISeriesConn(
       dsn=config.dsn,
       username=config.username,
       password=config.password,
       timeout=config.timeout,
       max_retries=config.max_retries,
       retry_delay=config.retry_delay,
   ) as conn:
       print("Connected successfully!")

2. Fetching Data
~~~~~~~~~~~~~~

.. code-block:: python

   from iseries_connector import ISeriesConn, ISeriesConfig

   config = ISeriesConfig.from_env()  # Using environment variables

   with ISeriesConn(
       dsn=config.dsn,
       username=config.username,
       password=config.password,
       timeout=config.timeout,
       max_retries=config.max_retries,
       retry_delay=config.retry_delay,
   ) as conn:
       # Fetch data into a pandas DataFrame
       df = conn.fetch("SELECT * FROM SCHEMA.TABLE WHERE COLUMN = 'value'")
       print(f"Retrieved {len(df)} rows")

3. Executing SQL Statements
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from iseries_connector import ISeriesConn, ISeriesConfig

   config = ISeriesConfig.from_env()

   with ISeriesConn(
       dsn=config.dsn,
       username=config.username,
       password=config.password,
       timeout=config.timeout,
       max_retries=config.max_retries,
       retry_delay=config.retry_delay,
   ) as conn:
       # Single statement
       conn.execute_statements("UPDATE table1 SET col1 = 'value1'")

       # Multiple statements sequentially on one connection
       statements = [
           "UPDATE table1 SET col1 = 'value1'",
           "UPDATE table2 SET col2 = 'value2'",
       ]
       conn.execute_statements(statements, parallel=False)

       # Multiple statements in parallel, each on its own connection
       conn.execute_statements(statements, parallel=True)

4. Executing SQL from Files
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from iseries_connector import ISeriesConn, ISeriesConfig

   config = ISeriesConfig.from_env()

   with ISeriesConn(
       dsn=config.dsn,
       username=config.username,
       password=config.password,
       timeout=config.timeout,
       max_retries=config.max_retries,
       retry_delay=config.retry_delay,
   ) as conn:
       # Single file, statements executed sequentially
       conn.execute_statements_from_files("sql/setup.sql")

       # Multiple files executed sequentially (per-file statements still sequential)
       conn.execute_statements_from_files(
           ["sql/schema.sql", "sql/data.sql"],
           parallel_files=False,
       )

       # Multiple files executed in parallel (per-file statements still sequential)
       conn.execute_statements_from_files(
           ["sql/indexes.sql", "sql/cleanup.sql"],
           parallel_files=True,
       )

5. Fetching Data from a SQL File
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from iseries_connector import ISeriesConn, ISeriesConfig

   config = ISeriesConfig.from_env()

   with ISeriesConn(
       dsn=config.dsn,
       username=config.username,
       password=config.password,
       timeout=config.timeout,
       max_retries=config.max_retries,
       retry_delay=config.retry_delay,
   ) as conn:
       # The file must contain exactly one SQL query
       df = conn.fetch_from_file("sql/top_customers.sql")
       print(f"Retrieved {len(df)} rows")

6. Data Transfer with IBM ACS
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from iseries_connector import DataTransferManager

   manager = DataTransferManager(
       host_name="your.hostname.com",
       # acs_launcher_path can be overridden if needed
   )

   # Single-table transfer
   result = next(
       manager.transfer_data(
           source_schema="SRC_SCHEMA",
           source_table="MY_TABLE",
           sql_statement="SELECT * FROM SRC_SCHEMA.MY_TABLE",
       )
   )
   if result.is_successful:
       print(f"Transferred {result.row_count} rows to {result.file_path}")

   # Multiple tables in a batch
   schemas = ["SRC_SCHEMA"] * 3
   tables = ["TABLE1", "TABLE2", "TABLE3"]
   statements = [
       "SELECT * FROM SRC_SCHEMA.TABLE1",
       "SELECT * FROM SRC_SCHEMA.TABLE2",
       "SELECT * FROM SRC_SCHEMA.TABLE3",
   ]
   batch_results = list(
       manager.transfer_data(
           source_schema=schemas,
           source_table=tables,
           sql_statement=statements,
       )
   )
   for r in batch_results:
       print(f"{r.source_schema}.{r.source_table}: success={r.is_successful}")

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


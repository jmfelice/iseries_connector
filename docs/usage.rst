Usage Guide
==========

This guide provides detailed examples of how to use the iSeries Connector library.

Basic Usage
----------

Here's a basic example of how to connect to an iSeries database and execute a query:

.. code-block:: python

   from iseries_connector import ISeriesConn, ISeriesConfig

   # Create a configuration
   config = ISeriesConfig(
       dsn="MY_ISERIES_DSN",
       username="admin",
       password="secret"
   )

   # Connect and execute a query
   with ISeriesConn(**config.__dict__) as conn:
       df = conn.fetch("SELECT * FROM MYTABLE")
       print(df.head())

Using Environment Variables
-------------------------

You can also configure the connection using environment variables:

.. code-block:: python

   import os
   from iseries_connector import ISeriesConfig

   # Set environment variables
   os.environ['ISERIES_DSN'] = 'MY_DSN'
   os.environ['ISERIES_USERNAME'] = 'admin'
   os.environ['ISERIES_PASSWORD'] = 'secret'
   os.environ['ISERIES_TIMEOUT'] = '30'
   os.environ['ISERIES_MAX_RETRIES'] = '3'
   os.environ['ISERIES_RETRY_DELAY'] = '5'

   # Create configuration from environment
   config = ISeriesConfig.from_env()

Executing Multiple Statements
---------------------------

You can execute multiple SQL statements either sequentially or in parallel:

.. code-block:: python

   from iseries_connector import ISeriesConn, ISeriesConfig

   config = ISeriesConfig(
       dsn="MY_DSN",
       username="admin",
       password="secret"
   )

   with ISeriesConn(**config.__dict__) as conn:
       # Sequential execution
       results = conn.execute_statements([
           "UPDATE TABLE1 SET COL1 = 'value1'",
           "UPDATE TABLE2 SET COL2 = 'value2'"
       ])

       # Parallel execution
       results = conn.execute_statements([
           "UPDATE TABLE1 SET COL1 = 'value1'",
           "UPDATE TABLE2 SET COL2 = 'value2'"
       ], parallel=True)

       # Check results
       for result in results:
           if result['success']:
               print(f"Statement executed successfully: {result['statement']}")
           else:
               print(f"Statement failed: {result['statement']}")
               print(f"Error: {result['error']}")

Working with Large Datasets
-------------------------

For large datasets, you can use chunking to process the data in smaller batches:

.. code-block:: python

   from iseries_connector import ISeriesConn, ISeriesConfig

   config = ISeriesConfig(
       dsn="MY_DSN",
       username="admin",
       password="secret"
   )

   with ISeriesConn(**config.__dict__) as conn:
       # Process data in chunks of 1000 rows
       for chunk in conn.fetch("SELECT * FROM LARGE_TABLE", chunksize=1000):
           # Process each chunk
           process_chunk(chunk)

Error Handling
-------------

The library provides comprehensive error handling:

.. code-block:: python

   from iseries_connector import ISeriesConn, ISeriesConfig
   from iseries_connector.exceptions import ConnectionError, QueryError

   config = ISeriesConfig(
       dsn="MY_DSN",
       username="admin",
       password="secret"
   )

   try:
       with ISeriesConn(**config.__dict__) as conn:
           df = conn.fetch("SELECT * FROM MYTABLE")
   except ConnectionError as e:
       print(f"Connection error: {e}")
   except QueryError as e:
       print(f"Query error: {e}")

Logging
-------

The library includes built-in logging support:

.. code-block:: python

   import logging
   from iseries_connector import ISeriesConn, ISeriesConfig

   # Configure logging
   logging.basicConfig(level=logging.INFO)

   config = ISeriesConfig(
       dsn="MY_DSN",
       username="admin",
       password="secret"
   )

   # Enable query echoing
   with ISeriesConn(**config.__dict__) as conn:
       conn.echo = True
       df = conn.fetch("SELECT * FROM MYTABLE") 
Redshift Module
==============

The Redshift module provides a robust interface for connecting to and interacting with Amazon Redshift databases. It includes features for connection management, query execution, and error handling.

Configuration
------------

The module uses the ``RedshiftConfig`` class for managing connection parameters. You can configure it in several ways:

1. Direct initialization:

.. code-block:: python

    from aws_connector import RedshiftConfig

    config = RedshiftConfig(
        host="my-cluster.xxxxx.region.redshift.amazonaws.com",
        username="admin",
        password="secret",
        database="mydb",
        port=5439,
        timeout=30,
        ssl=True,
        max_retries=3,
        retry_delay=5
    )

2. From environment variables:

.. code-block:: python

    config = RedshiftConfig.from_env()

The following environment variables are supported:

- ``REDSHIFT_HOST``: The hostname or endpoint of the Redshift cluster
- ``REDSHIFT_USERNAME``: The username for authentication
- ``REDSHIFT_PASSWORD``: The password for authentication
- ``REDSHIFT_DATABASE``: The name of the database to connect to
- ``REDSHIFT_PORT``: The port number for the Redshift cluster
- ``REDSHIFT_TIMEOUT``: Connection timeout in seconds
- ``REDSHIFT_SSL``: Whether to use SSL for the connection
- ``REDSHIFT_MAX_RETRIES``: Maximum number of connection retries
- ``REDSHIFT_RETRY_DELAY``: Delay between retries in seconds

Basic Usage
----------

Here's a basic example of using the Redshift connector:

.. code-block:: python

    from aws_connector import RedConn

    # Create a connection
    redshift = RedConn(
        host="my-cluster.xxxxx.region.redshift.amazonaws.com",
        username="admin",
        password="secret",
        database="mydb"
    )

    # Use context manager for automatic connection management
    with redshift as conn:
        # Execute a query and get results as DataFrame
        df = conn.fetch("SELECT * FROM my_table LIMIT 10")
        
        # Execute multiple statements
        results = conn.execute_statements([
            "CREATE TABLE IF NOT EXISTS new_table (id INT, name VARCHAR(100))",
            "INSERT INTO new_table VALUES (1, 'test')"
        ])

Advanced Usage
-------------

1. Using chunked data fetching:

.. code-block:: python

    with redshift as conn:
        # Fetch data in chunks of 1000 rows
        for chunk in conn.fetch("SELECT * FROM large_table", chunksize=1000):
            process_chunk(chunk)

2. Parallel statement execution:

.. code-block:: python

    with redshift as conn:
        # Execute multiple statements in parallel
        results = conn.execute_statements([
            "CREATE TABLE table1 (id INT)",
            "CREATE TABLE table2 (id INT)",
            "CREATE TABLE table3 (id INT)"
        ], parallel=True)

3. Error handling:

.. code-block:: python

    from aws_connector.exceptions import ConnectionError, QueryError

    try:
        with redshift as conn:
            df = conn.fetch("SELECT * FROM non_existent_table")
    except ConnectionError as e:
        print(f"Connection error: {e}")
    except QueryError as e:
        print(f"Query error: {e}")

API Reference
------------

RedshiftConfig
~~~~~~~~~~~~~

.. autoclass:: aws_connector.redshift.RedshiftConfig
   :members:
   :undoc-members:
   :show-inheritance:

RedConn
~~~~~~~

.. autoclass:: aws_connector.redshift.RedConn
   :members:
   :undoc-members:
   :show-inheritance:

Testing
-------

The module is designed to be easily testable. You can override the following methods for testing:

.. code-block:: python

    class MockRedConn(RedConn):
        def _get_connection(self):
            return MockConnection()
            
        def _get_cursor(self):
            return MockCursor()

Best Practices
-------------

1. Always use the context manager (``with`` statement) to ensure proper connection cleanup:

.. code-block:: python

    with redshift as conn:
        # Your code here
        # Connection will be automatically closed

2. Use parameterized queries to prevent SQL injection:

.. code-block:: python

    with redshift as conn:
        # Instead of string formatting
        df = conn.fetch("SELECT * FROM users WHERE id = %s", params=[user_id])

3. Handle large datasets using chunked fetching:

.. code-block:: python

    with redshift as conn:
        for chunk in conn.fetch("SELECT * FROM large_table", chunksize=1000):
            process_chunk(chunk)

4. Use parallel execution for independent statements:

.. code-block:: python

    with redshift as conn:
        results = conn.execute_statements(statements, parallel=True) 
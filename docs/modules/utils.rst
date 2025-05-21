Utilities Module
===============

.. automodule:: iseries_connector.utils
   :members:
   :undoc-members:
   :show-inheritance:

Utility Functions
---------------

.. autofunction:: iseries_connector.utils.validate_dsn
   :noindex:

.. autofunction:: iseries_connector.utils.format_query
   :noindex:

.. autofunction:: iseries_connector.utils.sanitize_sql
   :noindex:

.. autofunction:: iseries_connector.utils.get_connection_info
   :noindex:

.. autofunction:: iseries_connector.utils.format_error_message
   :noindex:

.. autofunction:: iseries_connector.utils.retry_on_error
   :noindex:

Usage Examples
-------------

.. code-block:: python

    from iseries_connector.utils import (
        validate_dsn,
        format_query,
        sanitize_sql,
        get_connection_info,
        format_error_message,
        retry_on_error
    )

    # Validate DSN
    if validate_dsn("MY_DSN"):
        print("DSN is valid")

    # Format query with parameters
    query = format_query(
        "SELECT * FROM {table} WHERE {column} = ?",
        table="MYTABLE",
        column="ID"
    )

    # Sanitize SQL input
    safe_input = sanitize_sql(user_input)

    # Get connection information
    conn_info = get_connection_info(conn)

    # Format error message
    error_msg = format_error_message(
        "Failed to execute query",
        query="SELECT * FROM TABLE",
        error=exception
    )

    # Retry function on error
    @retry_on_error(max_retries=3, delay=5)
    def execute_query(query: str) -> None:
        # Query execution code
        pass 
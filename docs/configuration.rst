Configuration
============

This guide explains how to configure the iSeries Connector library.

Configuration Options
-------------------

The library can be configured using either direct initialization or environment variables.

Direct Initialization
-------------------

You can create a configuration object directly:

.. code-block:: python

   from iseries_connector import ISeriesConfig

   config = ISeriesConfig(
       dsn="MY_DSN",           # Data Source Name
       username="admin",       # Database username
       password="secret",      # Database password
       timeout=30,            # Connection timeout in seconds
       max_retries=3,         # Maximum number of connection retries
       retry_delay=5          # Delay between retries in seconds
   )

Environment Variables
------------------

You can also configure the library using environment variables:

.. code-block:: bash

   # Required
   ISERIES_DSN=MY_DSN
   ISERIES_USERNAME=admin
   ISERIES_PASSWORD=secret

   # Optional
   ISERIES_TIMEOUT=30
   ISERIES_MAX_RETRIES=3
   ISERIES_RETRY_DELAY=5

To create a configuration from environment variables:

.. code-block:: python

   from iseries_connector import ISeriesConfig

   config = ISeriesConfig.from_env()

Configuration Validation
---------------------

The configuration is automatically validated when created. The following validations are performed:

* DSN cannot be empty
* Username cannot be empty
* Password cannot be empty
* Timeout must be a positive number
* Max retries cannot be negative
* Retry delay cannot be negative

Example:

.. code-block:: python

   from iseries_connector import ISeriesConfig
   from iseries_connector.exceptions import ValidationError

   try:
       config = ISeriesConfig(
           dsn="",  # Empty DSN will raise ValidationError
           username="admin",
           password="secret"
       )
   except ValidationError as e:
       print(f"Configuration error: {e}")

Using the Configuration
--------------------

Once you have a configuration object, you can use it to create a connection:

.. code-block:: python

   from iseries_connector import ISeriesConn, ISeriesConfig

   # Create configuration
   config = ISeriesConfig(
       dsn="MY_DSN",
       username="admin",
       password="secret"
   )

   # Create connection
   with ISeriesConn(**config.__dict__) as conn:
       # Use the connection
       df = conn.fetch("SELECT * FROM MYTABLE")

Best Practices
------------

1. Never hardcode credentials in your code
2. Use environment variables for sensitive information
3. Set appropriate timeout and retry values based on your network conditions
4. Validate configuration before using it
5. Use context managers (with statement) to ensure proper resource cleanup 
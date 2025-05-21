Exceptions Module
================

.. automodule:: iseries_connector.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

Exception Classes
---------------

.. autoclass:: iseries_connector.exceptions.ISeriesError
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: iseries_connector.exceptions.ConnectionError
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: iseries_connector.exceptions.QueryError
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: iseries_connector.exceptions.ConfigurationError
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: iseries_connector.exceptions.ValidationError
   :members:
   :undoc-members:
   :show-inheritance:

Usage Examples
-------------

.. code-block:: python

    from iseries_connector.exceptions import (
        ISeriesError,
        ConnectionError,
        QueryError,
        ConfigurationError,
        ValidationError
    )

    try:
        # Attempt to connect to iSeries
        conn = ISeriesConn(**config.__dict__)
    except ConnectionError as e:
        print(f"Failed to connect: {e}")
    except ConfigurationError as e:
        print(f"Invalid configuration: {e}")
    except ISeriesError as e:
        print(f"General iSeries error: {e}") 
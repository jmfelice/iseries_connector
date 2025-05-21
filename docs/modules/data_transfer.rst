Data Transfer Module
===================

.. automodule:: iseries_connector.data_transfer
   :members:
   :undoc-members:
   :show-inheritance:

Data Transfer Classes
-------------------

.. autoclass:: iseries_connector.data_transfer.DataTransfer
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: iseries_connector.data_transfer.BatchTransfer
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: iseries_connector.data_transfer.AsyncTransfer
   :members:
   :undoc-members:
   :show-inheritance:

Data Transfer Utilities
---------------------

.. automodule:: iseries_connector.data_transfer.utils
   :members:
   :undoc-members:
   :show-inheritance:

Examples
--------

.. code-block:: python

    from iseries_connector.data_transfer import DataTransfer, BatchTransfer
    from typing import List, Dict, Any
    import pandas as pd

    # Single table transfer
    transfer = DataTransfer(
        source_conn=source_conn,
        target_conn=target_conn,
        source_table="SOURCE_TABLE",
        target_table="TARGET_TABLE"
    )
    
    # Execute transfer
    transfer.execute()

    # Batch transfer multiple tables
    batch = BatchTransfer(
        source_conn=source_conn,
        target_conn=target_conn,
        tables=[
            ("SOURCE_TABLE1", "TARGET_TABLE1"),
            ("SOURCE_TABLE2", "TARGET_TABLE2")
        ]
    )
    
    # Execute batch transfer
    results: List[Dict[str, Any]] = batch.execute(parallel=True)

Configuration
------------

.. autoclass:: iseries_connector.data_transfer.DataTransferConfig
   :members:
   :undoc-members:
   :show-inheritance:

Results
-------

.. autoclass:: iseries_connector.data_transfer.DataTransferResult
   :members:
   :undoc-members:
   :show-inheritance:

Manager
-------

.. autoclass:: iseries_connector.data_transfer.DataTransferManager
   :members:
   :undoc-members:
   :show-inheritance: 
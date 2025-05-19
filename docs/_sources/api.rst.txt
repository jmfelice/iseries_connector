API Reference
============

This section provides detailed information about the AWS Connector API.

Core Components
--------------

.. automodule:: aws_connector
   :members:
   :undoc-members:
   :show-inheritance:

AWS Service Connectors
---------------------

S3 Connector
~~~~~~~~~~~

.. automodule:: aws_connector.s3
   :members:
   :undoc-members:
   :show-inheritance:

Example usage:

.. code-block:: python

    from aws_connector.s3 import S3Connector

    # Initialize the connector
    s3 = S3Connector(
        aws_access_key_id='your_access_key',
        aws_secret_access_key='your_secret_key',
        region_name='us-west-2'
    )

    # List buckets
    buckets = s3.list_buckets()
    print(f"Available buckets: {buckets}")

    # Upload a file
    s3.upload_file(
        bucket_name='my-bucket',
        file_path='local_file.txt',
        object_key='remote_file.txt'
    )

Redshift Connector
~~~~~~~~~~~~~~~~

.. automodule:: aws_connector.redshift
   :members:
   :undoc-members:
   :show-inheritance:

Example usage:

.. code-block:: python

    from aws_connector.redshift import RedshiftConnector

    # Initialize the connector
    redshift = RedshiftConnector(
        aws_access_key_id='your_access_key',
        aws_secret_access_key='your_secret_key',
        region_name='us-west-2'
    )

    # Create a cluster
    redshift.create_cluster(
        cluster_identifier='my-cluster',
        node_type='dc2.large',
        master_username='admin',
        master_user_password='password123',
        db_name='mydb'
    )

    # Execute a query
    results = redshift.execute_query(
        cluster_identifier='my-cluster',
        database='mydb',
        query='SELECT * FROM users LIMIT 10'
    )
    print(f"Query results: {results}") 
=============
AWS Connector
=============

A Python library that provides a simplified interface for interacting with AWS services, focusing on S3 and Redshift operations.

.. image:: https://img.shields.io/pypi/v/aws_connector.svg
        :target: https://pypi.python.org/pypi/aws_connector

.. image:: https://img.shields.io/travis/jmfelice/aws_connector.svg
        :target: https://travis-ci.com/jmfelice/aws_connector

.. image:: https://readthedocs.org/projects/aws-connector/badge/?version=latest
        :target: https://aws-connector.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status

* Free software: MIT license
* Documentation: https://aws-connector.readthedocs.io

Features
--------

* S3 Operations:
    * List buckets and objects
    * Upload and download files
    * Manage bucket policies and permissions
    * Stream large files efficiently

* Redshift Operations:
    * Create and manage clusters
    * Execute SQL queries
    * Load data from S3 to Redshift
    * Monitor cluster status

* Common Features:
    * Simplified AWS credential management
    * Error handling and retries
    * Logging and monitoring
    * Type hints and documentation

Installation
-----------

You can install AWS Connector using pip:

.. code-block:: bash

    pip install aws-connector

Quick Start
----------

Here's a quick example of using the S3 connector:

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

And here's an example with Redshift:

.. code-block:: python

    from aws_connector.redshift import RedshiftConnector

    # Initialize the connector
    redshift = RedshiftConnector(
        aws_access_key_id='your_access_key',
        aws_secret_access_key='your_secret_key',
        region_name='us-west-2'
    )

    # Execute a query
    results = redshift.execute_query(
        cluster_identifier='my-cluster',
        database='mydb',
        query='SELECT * FROM users LIMIT 10'
    )
    print(f"Query results: {results}")

Documentation
------------

For more detailed documentation, including API reference and examples, visit:
https://aws-connector.readthedocs.io

Contributing
-----------

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

License
-------

This project is licensed under the MIT License - see the LICENSE file for details.

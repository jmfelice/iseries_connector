Configuration Guide
=================

This guide explains how to configure the AWS Connector package for different environments and use cases.

Environment Variables
------------------

The package can be configured using environment variables. Here's a complete list of supported variables:

Redshift Configuration
~~~~~~~~~~~~~~~~~~~~

- ``REDSHIFT_HOST``: The hostname or endpoint of the Redshift cluster
- ``REDSHIFT_USERNAME``: The username for authentication
- ``REDSHIFT_PASSWORD``: The password for authentication
- ``REDSHIFT_DATABASE``: The name of the database to connect to
- ``REDSHIFT_PORT``: The port number for the Redshift cluster (default: 5439)
- ``REDSHIFT_TIMEOUT``: Connection timeout in seconds (default: 30)
- ``REDSHIFT_SSL``: Whether to use SSL for the connection (default: true)
- ``REDSHIFT_MAX_RETRIES``: Maximum number of connection retries (default: 3)
- ``REDSHIFT_RETRY_DELAY``: Delay between retries in seconds (default: 5)

S3 Configuration
~~~~~~~~~~~~~~

- ``AWS_S3_BUCKET``: The S3 bucket name
- ``AWS_S3_DIRECTORY``: The directory path within the bucket
- ``AWS_IAM_ROLE``: The IAM role ARN
- ``AWS_REGION``: The AWS region
- ``AWS_KMS_KEY_ID``: The KMS key ID for encryption
- ``AWS_MAX_RETRIES``: Maximum number of retries for AWS operations (default: 3)
- ``AWS_TIMEOUT``: Timeout in seconds for AWS operations (default: 30)

AWS SSO Configuration
~~~~~~~~~~~~~~~~~~~

- ``AWS_EXEC_FILE_PATH``: Path to the AWS CLI executable
- ``AWS_CREDENTIALS_DB_PATH``: Path to the credentials database
- ``AWS_SSO_REFRESH_WINDOW``: Hours between credential refreshes (default: 6)
- ``AWS_SSO_MAX_RETRIES``: Maximum number of authentication retries (default: 3)
- ``AWS_SSO_RETRY_DELAY``: Delay between retries in seconds (default: 5)

Setting Up Environment Variables
-----------------------------

1. Using a .env file:

.. code-block:: text

    # .env
    REDSHIFT_HOST=my-cluster.xxxxx.region.redshift.amazonaws.com
    REDSHIFT_USERNAME=admin
    REDSHIFT_PASSWORD=secret
    REDSHIFT_DATABASE=mydb
    
    AWS_S3_BUCKET=my-bucket
    AWS_S3_DIRECTORY=data/
    AWS_IAM_ROLE=123456789012
    AWS_REGION=us-east-1
    
    AWS_EXEC_FILE_PATH=C:\Program Files\Amazon\AWSCLIV2\aws.exe
    AWS_CREDENTIALS_DB_PATH=./data/aws_credentials.db

2. Using environment variables in your shell:

.. code-block:: bash

    # Windows PowerShell
    $env:REDSHIFT_HOST = "my-cluster.xxxxx.region.redshift.amazonaws.com"
    $env:REDSHIFT_USERNAME = "admin"
    $env:REDSHIFT_PASSWORD = "secret"
    $env:REDSHIFT_DATABASE = "mydb"

    # Linux/macOS
    export REDSHIFT_HOST="my-cluster.xxxxx.region.redshift.amazonaws.com"
    export REDSHIFT_USERNAME="admin"
    export REDSHIFT_PASSWORD="secret"
    export REDSHIFT_DATABASE="mydb"

3. Using Python to set environment variables:

.. code-block:: python

    import os

    os.environ["REDSHIFT_HOST"] = "my-cluster.xxxxx.region.redshift.amazonaws.com"
    os.environ["REDSHIFT_USERNAME"] = "admin"
    os.environ["REDSHIFT_PASSWORD"] = "secret"
    os.environ["REDSHIFT_DATABASE"] = "mydb"

Configuration Examples
-------------------

1. Basic Redshift Configuration:

.. code-block:: python

    from aws_connector import RedConn

    redshift = RedConn(
        host="my-cluster.xxxxx.region.redshift.amazonaws.com",
        username="admin",
        password="secret",
        database="mydb"
    )

2. S3 Configuration with KMS Encryption:

.. code-block:: python

    from aws_connector import S3Connector

    s3 = S3Connector(
        bucket="my-bucket",
        directory="data/",
        iam="123456789012",
        region="us-east-1",
        kms_key_id="arn:aws:kms:region:account:key/key-id"
    )

3. AWS SSO Configuration with Custom Settings:

.. code-block:: python

    from aws_connector import AWSsso, SSOConfig

    config = SSOConfig(
        refresh_window_hours=12,
        max_retries=5,
        retry_delay=10
    )
    sso = AWSsso(config)

Best Practices
------------

1. Security:

   - Never commit credentials to version control
   - Use environment variables or secure credential storage
   - Rotate credentials regularly
   - Use KMS encryption for sensitive data

2. Performance:

   - Set appropriate timeout values based on your use case
   - Configure retry settings based on network reliability
   - Use chunked data processing for large datasets

3. Error Handling:

   - Implement proper error handling for all operations
   - Log errors appropriately
   - Use retry mechanisms for transient failures

4. Testing:

   - Use different configurations for development and production
   - Test with mock services when possible
   - Validate configurations before deployment

Common Issues and Solutions
-------------------------

1. Connection Timeouts:

   - Increase the timeout value
   - Check network connectivity
   - Verify firewall settings

2. Authentication Failures:

   - Verify credentials
   - Check IAM permissions
   - Ensure AWS SSO is properly configured

3. S3 Upload Failures:

   - Verify bucket permissions
   - Check file size limits
   - Ensure proper KMS key configuration

4. Redshift Query Failures:

   - Verify table permissions
   - Check query syntax
   - Monitor query performance 
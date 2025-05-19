AWS SSO Module
=============

The AWS SSO module provides functionality for managing AWS Single Sign-On (SSO) authentication and credential management. It handles credential refresh, validation, and tracking of credential validity periods.

Configuration
------------

The module uses the ``SSOConfig`` class for managing SSO parameters. You can configure it in several ways:

1. Default initialization:

.. code-block:: python

    from aws_connector import SSOConfig

    config = SSOConfig()  # Uses all default values

2. Custom configuration:

.. code-block:: python

    config = SSOConfig(
        aws_exec_file_path="/custom/path/to/aws",
        db_path=Path("./custom/path/credentials.db"),
        refresh_window_hours=12,  # Refresh every 12 hours
        max_retries=5,  # More retries
        retry_delay=10  # Longer delay between retries
    )

3. From environment variables:

.. code-block:: python

    config = SSOConfig.from_env()

The following environment variables are supported:

- ``AWS_EXEC_FILE_PATH``: Path to the AWS CLI executable
- ``AWS_CREDENTIALS_DB_PATH``: Path to the credentials database
- ``AWS_SSO_REFRESH_WINDOW``: Hours between credential refreshes
- ``AWS_SSO_MAX_RETRIES``: Maximum number of authentication retries
- ``AWS_SSO_RETRY_DELAY``: Delay between retries in seconds

Basic Usage
----------

Here's a basic example of using the AWS SSO handler:

.. code-block:: python

    from aws_connector import AWSsso

    # Initialize with default configuration
    sso = AWSsso()

    # Ensure credentials are valid
    if sso.ensure_valid_credentials():
        print("SSO credentials are valid")
    else:
        print("Failed to ensure valid SSO credentials")

Advanced Usage
-------------

1. Custom configuration with error handling:

.. code-block:: python

    from aws_connector import AWSsso, SSOConfig
    from aws_connector.exceptions import AuthenticationError, CredentialError

    try:
        config = SSOConfig(
            refresh_window_hours=12,
            max_retries=5,
            retry_delay=10
        )
        sso = AWSsso(config)
        sso.ensure_valid_credentials()
    except AuthenticationError as e:
        print(f"Authentication error: {e}")
    except CredentialError as e:
        print(f"Credential error: {e}")

2. Manual credential refresh:

.. code-block:: python

    if sso.should_refresh_credentials():
        print("Credentials need refresh")
        if sso.refresh_credentials():
            print("Successfully refreshed credentials")
        else:
            print("Failed to refresh credentials")
    else:
        print("Credentials are still valid")

3. Checking credential status:

.. code-block:: python

    try:
        if sso.should_refresh_credentials():
            print("SSO credentials need refresh")
            sso.refresh_credentials()
        else:
            print("SSO credentials are still valid")
    except CredentialError as e:
        print(f"Error checking credential status: {e}")

API Reference
------------

SSOConfig
~~~~~~~~~

.. autoclass:: aws_connector.aws_sso.SSOConfig
   :members:
   :undoc-members:
   :show-inheritance:

AWSsso
~~~~~~

.. autoclass:: aws_connector.aws_sso.AWSsso
   :members:
   :undoc-members:
   :show-inheritance:

Testing
-------

The module is designed to be easily testable. You can override the following methods for testing:

.. code-block:: python

    class MockAWSsso(AWSsso):
        def _get_db_connection(self):
            return MockConnection()
            
        def _run_aws_command(self):
            return MockCommandOutput()

Best Practices
-------------

1. Initialize SSO handler early in your application:

.. code-block:: python

    # At application startup
    sso = AWSsso()
    sso.ensure_valid_credentials()

2. Handle credential refresh in long-running applications:

.. code-block:: python

    def run_long_process():
        sso = AWSsso()
        
        while True:
            try:
                sso.ensure_valid_credentials()
                # Your long-running process here
                time.sleep(3600)  # Check every hour
            except (AuthenticationError, CredentialError) as e:
                logger.error(f"SSO error: {e}")
                break

3. Use appropriate refresh windows:

.. code-block:: python

    # For applications that run for hours
    config = SSOConfig(refresh_window_hours=12)
    sso = AWSsso(config)

    # For short-running scripts
    config = SSOConfig(refresh_window_hours=1)
    sso = AWSsso(config)

4. Implement proper error handling:

.. code-block:: python

    try:
        sso = AWSsso()
        if not sso.ensure_valid_credentials():
            raise AuthenticationError("Failed to ensure valid credentials")
        # Proceed with AWS operations
    except (AuthenticationError, CredentialError) as e:
        logger.error(f"SSO error: {e}")
        # Handle error appropriately 
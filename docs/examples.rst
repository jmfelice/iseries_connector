Examples
========

This page provides examples of common use cases for the AWS Connector package.

Basic Data Pipeline
-----------------

This example shows how to create a basic data pipeline that:
1. Connects to Redshift
2. Fetches data
3. Processes it
4. Uploads to S3
5. Loads into another Redshift table

.. code-block:: python

    from aws_connector import RedConn, S3Connector, AWSsso
    import pandas as pd

    def run_data_pipeline():
        # Initialize AWS SSO
        sso = AWSsso()
        sso.ensure_valid_credentials()

        # Connect to source Redshift
        source_redshift = RedConn(
            host="source-cluster.xxxxx.region.redshift.amazonaws.com",
            username="admin",
            password="secret",
            database="sourcedb"
        )

        # Connect to target Redshift
        target_redshift = RedConn(
            host="target-cluster.xxxxx.region.redshift.amazonaws.com",
            username="admin",
            password="secret",
            database="targetdb"
        )

        # Initialize S3 connector
        s3 = S3Connector(
            bucket="my-bucket",
            directory="data/",
            iam="123456789012",
            region="us-east-1"
        )

        try:
            # Fetch data from source
            with source_redshift as conn:
                df = conn.fetch("""
                    SELECT 
                        user_id,
                        transaction_date,
                        amount
                    FROM transactions
                    WHERE transaction_date >= CURRENT_DATE - 7
                """)

            # Process data
            df['processed_date'] = pd.Timestamp.now()
            df['amount_usd'] = df['amount'] * 1.2  # Example conversion

            # Upload to S3
            result = s3.upload_to_s3(
                df,
                table_name="processed_transactions",
                temp_file_name="transactions_20240101.csv"
            )

            if not result['success']:
                raise Exception(f"Upload failed: {result['error']}")

            # Create target table
            create_table = """
            CREATE TABLE IF NOT EXISTS processed_transactions (
                user_id INT,
                transaction_date DATE,
                amount DECIMAL(10,2),
                processed_date TIMESTAMP,
                amount_usd DECIMAL(10,2)
            )
            """

            # Load to target Redshift
            with target_redshift as conn:
                conn.execute_statements([
                    create_table,
                    f"""
                    COPY processed_transactions
                    FROM 's3://{s3.config.bucket}/{s3.config.directory}transactions_20240101.csv'
                    IAM_ROLE 'arn:aws:iam::{s3.config.iam}:role/{s3.config.region}-{s3.config.iam}-fisher-production'
                    FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1
                    """
                ])

        except Exception as e:
            print(f"Pipeline failed: {e}")
            raise

Large Dataset Processing
----------------------

This example demonstrates how to handle large datasets using chunked processing:

.. code-block:: python

    from aws_connector import RedConn, S3Connector
    import pandas as pd

    def process_large_dataset():
        redshift = RedConn(
            host="my-cluster.xxxxx.region.redshift.amazonaws.com",
            username="admin",
            password="secret",
            database="mydb"
        )

        s3 = S3Connector(
            bucket="my-bucket",
            directory="data/"
        )

        try:
            with redshift as conn:
                # Process data in chunks of 10000 rows
                for i, chunk in enumerate(conn.fetch(
                    "SELECT * FROM large_table",
                    chunksize=10000
                )):
                    # Process chunk
                    processed_chunk = process_chunk(chunk)
                    
                    # Upload chunk
                    result = s3.upload_to_s3(
                        processed_chunk,
                        table_name="processed_data",
                        temp_file_name=f"chunk_{i}.csv"
                    )
                    
                    if not result['success']:
                        print(f"Failed to upload chunk {i}: {result['error']}")

        except Exception as e:
            print(f"Processing failed: {e}")
            raise

    def process_chunk(chunk):
        # Example processing
        chunk['processed_date'] = pd.Timestamp.now()
        return chunk

AWS SSO Integration
-----------------

This example shows how to integrate AWS SSO with your application:

.. code-block:: python

    from aws_connector import AWSsso, RedConn, S3Connector
    import time

    class AWSService:
        def __init__(self):
            self.sso = AWSsso()
            self.redshift = None
            self.s3 = None

        def initialize(self):
            # Ensure SSO credentials are valid
            self.sso.ensure_valid_credentials()

            # Initialize services
            self.redshift = RedConn(
                host="my-cluster.xxxxx.region.redshift.amazonaws.com",
                username="admin",
                password="secret",
                database="mydb"
            )

            self.s3 = S3Connector(
                bucket="my-bucket",
                directory="data/"
            )

        def run_service(self):
            while True:
                try:
                    # Check credentials before each operation
                    self.sso.ensure_valid_credentials()

                    # Your service logic here
                    with self.redshift as conn:
                        data = conn.fetch("SELECT * FROM my_table")

                    # Upload to S3
                    self.s3.upload_to_s3(data, table_name="my_table")

                    # Sleep for an hour
                    time.sleep(3600)

                except Exception as e:
                    print(f"Service error: {e}")
                    # Wait before retrying
                    time.sleep(60)

    # Run the service
    service = AWSService()
    service.initialize()
    service.run_service()

Error Handling
-------------

This example demonstrates proper error handling:

.. code-block:: python

    from aws_connector import RedConn, S3Connector
    from aws_connector.exceptions import (
        ConnectionError,
        QueryError,
        UploadError,
        RedshiftError
    )

    def safe_operation():
        redshift = RedConn(
            host="my-cluster.xxxxx.region.redshift.amazonaws.com",
            username="admin",
            password="secret",
            database="mydb"
        )

        s3 = S3Connector(
            bucket="my-bucket",
            directory="data/"
        )

        try:
            with redshift as conn:
                # Execute query
                df = conn.fetch("SELECT * FROM my_table")

                # Upload to S3
                result = s3.upload_to_s3(df, table_name="my_table")

                if not result['success']:
                    raise UploadError(result['error'])

        except ConnectionError as e:
            print(f"Connection error: {e}")
            # Handle connection issues
        except QueryError as e:
            print(f"Query error: {e}")
            # Handle query issues
        except UploadError as e:
            print(f"Upload error: {e}")
            # Handle upload issues
        except RedshiftError as e:
            print(f"Redshift error: {e}")
            # Handle Redshift issues
        except Exception as e:
            print(f"Unexpected error: {e}")
            # Handle unexpected issues
        finally:
            # Cleanup
            pass

Testing
-------

This example shows how to test your code using mock objects:

.. code-block:: python

    from aws_connector import RedConn, S3Connector
    import unittest
    from unittest.mock import Mock, patch

    class MockRedConn(RedConn):
        def _get_connection(self):
            return MockConnection()
            
        def _get_cursor(self):
            return MockCursor()

    class MockS3Connector(S3Connector):
        def _get_s3_client(self):
            return MockS3Client()
            
        def _get_redshift_client(self):
            return MockRedshiftClient()

    class TestAWSService(unittest.TestCase):
        def setUp(self):
            self.redshift = MockRedConn(
                host="test-host",
                username="test-user",
                password="test-pass",
                database="test-db"
            )
            
            self.s3 = MockS3Connector(
                bucket="test-bucket",
                directory="test/"
            )

        def test_data_pipeline(self):
            # Test your pipeline
            with self.redshift as conn:
                df = conn.fetch("SELECT * FROM test_table")
                
            result = self.s3.upload_to_s3(df, table_name="test_table")
            
            self.assertTrue(result['success'])
            self.assertIsNone(result['error'])

    if __name__ == '__main__':
        unittest.main() 
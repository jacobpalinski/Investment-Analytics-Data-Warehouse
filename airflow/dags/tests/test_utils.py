# Import modules
import base64
import pytest
from dags.utils.utils import create_snowflake_connection, s3_get_object, s3_put_object

class TestSnowflakeConnection:
    def test_create_snowflake_connection(self, mocker):
        # Create variables for function
        user = "test_user"
        account = "test_account"
        warehouse = "test_wh"
        database = "test_db"
        schema = "test_schema"
        private_key = b"fake_private_key"
        private_key_encoded = base64.b64encode(private_key)

        # Create a mock snowflake connection
        mock_connection = mocker.MagicMock()
        mock_connect = mocker.patch("snowflake.connector.connect", return_value=mock_connection)

        # Invoke create_snowflake_connection
        conn = create_snowflake_connection(
        user=user,
        account=account,
        private_key_encoded=private_key_encoded,
        warehouse=warehouse,
        database=database,
        schema=schema,
        )

        # Assert
        assert conn == mock_connection
        mock_connect.assert_called_once_with(
        user=user,
        private_key=private_key,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        )
    
    def test_create_snowflake_connection_failure(self, mocker):
        # Create variables for function
        user = "test_user"
        account = "test_account"
        warehouse = "test_wh"
        database = "test_db"
        schema = "test_schema"
        private_key = b"fake_private_key"
        private_key_encoded = base64.b64encode(private_key)

        # Patch connect to raise an exception
        mocker.patch(
            "snowflake.connector.connect",
            side_effect=Exception("Connection failed")
        )

        # Assert that exception is raised
        with pytest.raises(Exception, match="Connection failed"):
            create_snowflake_connection(
                user=user,
                account=account,
                private_key_encoded=private_key_encoded,
                warehouse=warehouse,
                database=database,
                schema=schema,
            )
class TestS3Utils:
    def test_s3_get_object_success(self, mocker):
        # Create variables for function
        bucket = "test-bucket"
        key = "test/key.json"
        expected_object = {"body": b"data"}

        # Create a mock s3 client with get_object
        mock_s3_client = mocker.Mock()
        mock_s3_client.get_object.return_value = expected_object

        # Patch boto3.client to return mock s3_client
        mocker.patch("boto3.client", return_value=mock_s3_client)

        # Execute function
        result = s3_get_object(bucket, key)

        # Assert
        assert result == expected_object
        mock_s3_client.get_object.assert_called_once_with(Bucket=bucket, Key=key)
    
    def test_s3_get_object_failure(self, mocker):
        # Create variables for function
        bucket = "test-bucket"
        key = "test/key.json"

        # Create a mock s3 client with get_object
        mock_s3_client = mocker.Mock()
        mock_s3_client.get_object.side_effect = Exception("NoSuchKey")

        # Patch boto3.client to return mock s3_client
        mocker.patch("boto3.client", return_value=mock_s3_client)

        # Assert
        with pytest.raises(Exception, match="NoSuchKey"):
            s3_get_object(bucket, key)

        mock_s3_client.get_object.assert_called_once_with(Bucket=bucket, Key=key)
    
    def test_s3_put_object_success(self, mocker):
        # Create variables for function
        bucket = "test-bucket"
        key = "test/key.json"
        data = b'{"body": "data"}'

        # Create a mock s3 client
        mock_s3_client = mocker.Mock()
        mocker.patch("boto3.client", return_value=mock_s3_client)

        # Execute function
        s3_put_object(bucket, key, data)

        # Assert
        mock_s3_client.put_object.assert_called_once_with(Bucket=bucket, Key=key, Body=data)

    def test_s3_put_object_failure(self, mocker):
        # Create variables for function
        bucket = "test-bucket"
        key = "test/key.json"
        data = b'{"body": "data"}'

        # Create a mock s3 client
        mock_s3_client = mocker.Mock()
        mock_s3_client.put_object.side_effect = Exception("Upload failed")
        mocker.patch("boto3.client", return_value=mock_s3_client)

        # Assert
        with pytest.raises(Exception, match="Upload failed"):
            s3_put_object(bucket, key, data)

        mock_s3_client.put_object.assert_called_once_with(Bucket=bucket, Key=key, Body=data)
    


    




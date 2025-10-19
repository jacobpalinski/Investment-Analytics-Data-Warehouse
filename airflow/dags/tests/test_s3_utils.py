# Import modules
from datetime import datetime
import json
import pytest
from dags.utils.s3_utils import S3

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
        mocker.patch("dags.utils.s3_utils.boto3.client", return_value=mock_s3_client)

        # Instantiate the S3 class
        s3_instance = S3("aws_access_key_id", "aws_secret_access_key")

        # Execute function
        result = s3_instance.get_object(bucket, key)

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

        # Instantiate the S3 class
        s3_instance = S3("aws_access_key_id", "aws_secret_access_key")

        # Assert
        with pytest.raises(Exception, match="NoSuchKey"):
            s3_instance.get_object(bucket, key)

        mock_s3_client.get_object.assert_called_once_with(Bucket=bucket, Key=key)
    
    def test_s3_put_object_success(self, mocker):
        # Create variables for function
        bucket = "test-bucket"
        key = "test/key.json"
        data = b'{"body": "data"}'

        # Create a mock s3 client
        mock_s3_client = mocker.Mock()

        # Patch boto3.client to return mock s3_client
        mocker.patch("dags.utils.s3_utils.boto3.client", return_value=mock_s3_client)

        # Instantiate the S3 class
        s3_instance = S3("aws_access_key_id", "aws_secret_access_key")

        # Execute function
        s3_instance.put_object(bucket, key, data)

        # Assert
        mock_s3_client.put_object.assert_called_once_with(Bucket=bucket, Key=key, Body=data)

    def test_s3_put_object_failure(self, mocker):
        # Create variables for function
        bucket = "test-bucket"
        key = "test/key.json"
        data = b'{"body": "data"}'

        # Create a mock s3 client with put_object that raises an exception
        mock_s3_client = mocker.Mock()
        mock_s3_client.put_object.side_effect = Exception("Upload failed")
        
        # Patch boto3.client to return mock s3_client
        mocker.patch("boto3.client", return_value=mock_s3_client)

        # Instantiate the S3 class
        s3_instance = S3("aws_access_key_id", "aws_secret_access_key")

        # Assert
        with pytest.raises(Exception, match="Upload failed"):
            s3_instance.put_object(bucket, key, data)

        mock_s3_client.put_object.assert_called_once_with(Bucket=bucket, Key=key, Body=data)
    
    def test_update_metadata_success(self, mocker):
        # Create variables for function
        bucket = "test-bucket"
        metadata_key = "metadata.json"
        today = datetime.now().strftime("%Y-%m-%d")

        # Mock existing metadata file contents
        existing_metadata = {"company_dimension": "2025-01-01"}

        # Mock get_object to return a S3 response
        mock_body = mocker.Mock()
        mock_body.read.return_value = json.dumps(existing_metadata).encode("utf-8")

        mock_s3_client = mocker.Mock()
        mock_s3_client.get_object.return_value = {"Body": mock_body}

        # Patch boto3.client to return mock s3_client
        mocker.patch("boto3.client", return_value=mock_s3_client)

        # Instantiate the S3 class
        s3_instance = S3("aws_access_key_id", "aws_secret_access_key")

        # Execute function
        s3_instance.update_metadata(bucket, metadata_key)

        # Assert — ensure put_object was called with updated metadata
        args, kwargs = mock_s3_client.put_object.call_args
        updated_data = json.loads(kwargs["data"].decode("utf-8"))

        assert kwargs["bucket"] == bucket
        assert kwargs["key"] == metadata_key
        assert updated_data["company_dimension"] == today

        mock_s3_client.get_object.assert_called_once_with(bucket=bucket, key=metadata_key)
        mock_s3_client.put_object.assert_called_once()

    def test_update_metadata_failure(self, mocker):
        # Create variables for function
        bucket = "test-bucket"
        metadata_key = "metadata.json"

        # Create a mock s3 client with get_object that raises an exception
        mock_s3_client = mocker.Mock()
        mock_s3_client.get_object.side_effect = Exception("Metadata not found")

        # Patch boto3.client to return mock s3_client
        mocker.patch("dags.utils.s3_utils.boto3.client", return_value=mock_s3_client)

        # Instantiate the S3 class
        s3_instance = S3("aws_access_key_id", "aws_secret_access_key")

        # Assert
        with pytest.raises(Exception, match="Metadata not found"):
            s3_instance.update_metadata(bucket, metadata_key)

        mock_s3_client.get_object.assert_called_once_with(bucket=bucket, key=metadata_key)
# Import modules
from datetime import datetime
import json
import pytest
from dags.utils.s3_utils import S3, SNSNotifier

class TestS3Utils:
    """ Test Suite for S3 class """
    def test_s3_get_object_success(self, mocker):
        """ Test get_object method of S3 class successfully retrieves mock object """
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

        # Assert object has been called
        assert result == expected_object
        mock_s3_client.get_object.assert_called_once_with(Bucket=bucket, Key=key)
    
    def test_s3_get_object_failure(self, mocker):
        """ Test get_object method of S3 class unsuccessfully retrieves mock object """
        # Create variables for function
        bucket = "test-bucket"
        key = "test/key.json"

        # Create a mock s3 client with get_object
        mock_s3_client = mocker.Mock()
        mock_s3_client.get_object.side_effect = Exception("Client Error")

        # Patch boto3.client to return mock s3_client
        mocker.patch("boto3.client", return_value=mock_s3_client)

        # Instantiate the S3 class
        s3_instance = S3("aws_access_key_id", "aws_secret_access_key")

        # Assert object has not been called
        with pytest.raises(Exception, match="Client Error"):
            s3_instance.get_object(bucket, key)

        mock_s3_client.get_object.assert_called_once_with(Bucket=bucket, Key=key)
    
    def test_s3_put_object_success(self, mocker):
        """ Test put_object method of S3 class successfully puts object into mock S3 bucket """
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

        # Assert object has been put into bucket
        mock_s3_client.put_object.assert_called_once_with(Bucket=bucket, Key=key, Body=data)

    def test_s3_put_object_failure(self, mocker):
        """ Test put_object method of S3 class fails to put object into mock S3 bucket """
        # Create variables for function
        bucket = "test-bucket"
        key = "test/key.json"
        data = b'{"body": "data"}'

        # Create a mock s3 client with put_object that raises an exception
        mock_s3_client = mocker.Mock()
        mock_s3_client.put_object.side_effect = Exception("Client Error")
        
        # Patch boto3.client to return mock s3_client
        mocker.patch("boto3.client", return_value=mock_s3_client)

        # Instantiate the S3 class
        s3_instance = S3("aws_access_key_id", "aws_secret_access_key")

        # Assert object has not been put into object
        with pytest.raises(Exception, match="Client Error"):
            s3_instance.put_object(bucket, key, data)

        mock_s3_client.put_object.assert_called_once_with(Bucket=bucket, Key=key, Body=data)
    
    def test_update_metadata_success(self, mocker):
        """ Test update_metadata method of S3 class is successful in putting mock object into bucket """
        # Create variables for function
        bucket = "test-bucket"
        metadata_object = "metadata.json"
        metadata_key = "company_dimension"
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
        s3_instance.update_metadata(bucket, metadata_object, metadata_key)

        # Assert put_object was called with updated metadata
        args, kwargs = mock_s3_client.put_object.call_args
        updated_data = json.loads(kwargs["Body"].decode("utf-8"))

        assert kwargs["Bucket"] == bucket
        assert kwargs["Key"] == metadata_object
        assert updated_data[metadata_key] == today

        mock_s3_client.get_object.assert_called_once_with(Bucket=bucket, Key=metadata_object)
        mock_s3_client.put_object.assert_called_once()

    def test_update_metadata_failure(self, mocker):
        """ Test update_metadata method of S3 class is unsuccessful in putting mock object into bucket """
        # Create variables for function
        bucket = "test-bucket"
        metadata_object = "metadata.json"
        metadata_key = "company_dimension"

        # Create a mock s3 client with get_object that raises an exception
        mock_s3_client = mocker.Mock()
        mock_s3_client.get_object.side_effect = Exception("Metadata not found")

        # Patch boto3.client to return mock s3_client
        mocker.patch("dags.utils.s3_utils.boto3.client", return_value=mock_s3_client)

        # Instantiate the S3 class
        s3_instance = S3("aws_access_key_id", "aws_secret_access_key")

        # Assert metadata has not been successfully updated
        with pytest.raises(Exception, match="Metadata not found"):
            s3_instance.update_metadata(bucket, metadata_object, metadata_key)

        mock_s3_client.get_object.assert_called_once_with(Bucket=bucket, Key=metadata_object)

class TestSNSNotifier:
    """ Test Suite for SNSNotifier class """
    def test_get_client_success(self, mocker):
        """ Tests get_client function of SNSNotifier class """
        # Create mock SNS client
        mock_sns_client = mocker.Mock()

        # Patch boto3.client to return mock SNS client
        mock_boto3_client = mocker.patch(
            "dags.utils.s3_utils.boto3.client",
            return_value=mock_sns_client
        )

        # Instantiate SNSNotifier
        sns_notifier = SNSNotifier(
            access_key_id="aws_access_key_id",
            secret_access_key="aws_secret_access_key",
            region="ap-southeast-2",
            topic_arn="arn:aws:sns:ap-southeast-2:123456789:test-topic"
        )

        # Execute get_client
        client = sns_notifier.get_client()

        # Assertions
        assert client == mock_sns_client

        mock_boto3_client.assert_called_once_with(
            "sns",
            aws_access_key_id="aws_access_key_id",
            aws_secret_access_key="aws_secret_access_key",
            region_name="ap-southeast-2"
        )
    
    def test_build_message_success(self, mocker):
        """ Tests build_message function of SNSNotifier class where context is non-empty """
        # Create mock SNS client
        mock_sns_client = mocker.Mock()

        # Patch boto3.client to return mock SNS client
        mock_boto3_client = mocker.patch(
            "dags.utils.s3_utils.boto3.client",
            return_value=mock_sns_client
        )

        # Instantiate SNSNotifier
        sns_notifier = SNSNotifier(
            access_key_id="aws_access_key_id",
            secret_access_key="aws_secret_access_key",
            region="ap-southeast-2",
            topic_arn="arn:aws:sns:ap-southeast-2:123456789:test-topic"
        )

        # Create mock task instance
        mock_task_instance = type(
            "TaskInstance",
            (),
            {
                "task_id": "test_task",
                "log_url": "http://localhost/log"
            }
        )()

        # Create mock dag
        mock_dag = type(
            "DAG",
            (),
            {
                "dag_id": "test_dag"
            }
        )()

        # Create airflow context
        context = {
            "task_instance": mock_task_instance,
            "dag": mock_dag,
            "execution_date": "2025-01-01",
            "exception": Exception("Task failed")
        }

        # Execute build_message
        message = sns_notifier.build_message(context)

        # Convert JSON string back to dictionary
        payload = json.loads(message)

        # Assertions
        assert payload["dag_id"] == "test_dag"
        assert payload["task_id"] == "test_task"
        assert payload["execution_date"] == "2025-01-01"
        assert payload["log_url"] == "http://localhost/log"
        assert payload["exception"] == "Task failed"
    
    def test_build_message_missing_context(self, mocker):
        """ Tests build_message function of SNSNotifier class where context is empty """
        # Create mock SNS client
        mock_sns_client = mocker.Mock()

        # Patch boto3.client to return mock SNS client
        mock_boto3_client = mocker.patch(
            "dags.utils.s3_utils.boto3.client",
            return_value=mock_sns_client
        )

        # Instantiate SNSNotifier
        sns_notifier = SNSNotifier(
            access_key_id="aws_access_key_id",
            secret_access_key="aws_secret_access_key",
            region="ap-southeast-2",
            topic_arn="arn:aws:sns:ap-southeast-2:123456789:test-topic"
        )

        # Create empty context
        context = {}

        # Execute build_message
        result = sns_notifier.build_message(context)

        # Convert JSON string back to dictionary
        payload = json.loads(result)

        # Assertions
        assert payload["dag_id"] is None
        assert payload["task_id"] is None
        assert payload["log_url"] is None
        assert payload["exception"] is None
    
    def test_send_success(self, mocker):
        """ Tests send function of the SNSNotifier class for a successful send """
        # Create mock SNS client
        mock_sns_client = mocker.Mock()

        # Patch boto3.client to return mock SNS client
        mock_boto3_client = mocker.patch(
            "dags.utils.s3_utils.boto3.client",
            return_value=mock_sns_client
        )

        # Instantiate SNSNotifier
        sns_notifier = SNSNotifier(
            access_key_id="aws_access_key_id",
            secret_access_key="aws_secret_access_key",
            region="ap-southeast-2",
            topic_arn="arn:aws:sns:ap-southeast-2:123456789:test-topic"
        )

        # Create mock task instance
        mock_task_instance = type(
            "TaskInstance",
            (),
            {
                "task_id": "test_task",
                "log_url": "http://localhost/log"
            }
        )()

        # Create mock dag
        mock_dag = type(
            "DAG",
            (),
            {
                "dag_id": "test_dag"
            }
        )()

        # Create airflow context
        context = {
            "task_instance": mock_task_instance,
            "dag": mock_dag,
            "execution_date": "2025-01-01",
            "exception": Exception("Task failed")
        }

        # Execute send function
        sns_notifier.send(context)

        # Expected message
        expected_message = sns_notifier.build_message(context)

        # Assert send function has been called successfully
        mock_sns_client.publish.assert_called_once_with(
            TopicArn="arn:aws:sns:ap-southeast-2:123456789:test-topic",
            Subject="Airflow Alert: test_dag.test_task Failed",
            Message=expected_message,
        )
    
    def test_send_failure(self, mocker):
        """ Tests send function of the SNSNotifier class for a failed send """
        # Create mock SNS client
        mock_sns_client = mocker.Mock()

        # Create exception on publish
        mock_sns_client.publish.side_effect = Exception("SNS publish failed")

        # Patch boto3.client to return mock SNS client
        mock_boto3_client = mocker.patch(
            "dags.utils.s3_utils.boto3.client",
            return_value=mock_sns_client
        )

        # Instantiate SNSNotifier
        sns_notifier = SNSNotifier(
            access_key_id="aws_access_key_id",
            secret_access_key="aws_secret_access_key",
            region="ap-southeast-2",
            topic_arn="arn:aws:sns:ap-southeast-2:123456789:test-topic"
        )

        # Create mock task instance
        mock_task_instance = type(
            "TaskInstance",
            (),
            {
                "task_id": "test_task",
                "log_url": "http://localhost/log"
            }
        )()

        # Create mock dag
        mock_dag = type(
            "DAG",
            (),
            {
                "dag_id": "test_dag"
            }
        )()

        # Create airflow context
        context = {
            "task_instance": mock_task_instance,
            "dag": mock_dag,
            "execution_date": "2025-01-01",
            "exception": Exception("Task failed")
        }

        # Assert exception has been raised and send function has been called once
        with pytest.raises(Exception, match="SNS publish failed"):
            sns_notifier.send(context)

        mock_sns_client.publish.assert_called_once()
    
    def test_call_method(self, mocker):
        """ Tests __call__ method of SNSNotifier class """
        # Create mock SNS client
        mock_sns_client = mocker.Mock()

        # Patch boto3.client to return mock SNS client
        mock_boto3_client = mocker.patch(
            "dags.utils.s3_utils.boto3.client",
            return_value=mock_sns_client
        )

        # Instantiate SNSNotifier
        sns_notifier = SNSNotifier(
            access_key_id="aws_access_key_id",
            secret_access_key="aws_secret_access_key",
            region="ap-southeast-2",
            topic_arn="arn:aws:sns:ap-southeast-2:123456789:test-topic"
        )

        # Mock send functionality
        mock_send = mocker.patch.object(sns_notifier, "send")

        # Create mock task instance
        mock_task_instance = type(
            "TaskInstance",
            (),
            {
                "task_id": "test_task",
                "log_url": "http://localhost/log"
            }
        )()

        # Create mock dag
        mock_dag = type(
            "DAG",
            (),
            {
                "dag_id": "test_dag"
            }
        )()

        # Create airflow context
        context = {
            "task_instance": mock_task_instance,
            "dag": mock_dag,
            "execution_date": "2025-01-01",
            "exception": Exception("Task failed")
        }

        # Execute __call__
        sns_notifier.send(context)

        # Assert send has been called at least once
        mock_send.assert_called_once_with(context)


    









        

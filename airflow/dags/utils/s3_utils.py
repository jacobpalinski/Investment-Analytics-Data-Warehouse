# Import modules
import os
import json
from datetime import datetime
from botocore.client import BaseClient
from botocore.exceptions import ClientError
import boto3
import logging

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class S3:
    """ 
    Class for managing interactions with S3

    Attributes:
        s3 (boto3.client): S3 client for AWS connection

    Methods:
        get_object(bucket, key):
            Retrieves an object from an S3 bucket
        put_object(bucket, key, data):
            Puts an object into an S3 bucket
        update_metadata(bucket, metadata_key):
            Updates the metadata of an S3 bucket by adding or updating a specific key with the current timestamp
    """

    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str):
        """ Initializes the S3 client with provided AWS credentials. """
        self.s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    
    def get_object(self, bucket: str, key: str) -> dict:
        """
        Retrieves an object from an S3 bucket

        Args:
            bucket (str): The name of the S3 bucket.
            key (str): The key of the object to retrieve.

        Returns:
            dict: The S3 object.
        """
        try:
            logger.info(f"Retrieving object from {key} from the {bucket} bucket")
            response = self.s3.get_object(Bucket=bucket, Key=key)
            logger.info(f"Successfully retrieved data from S3 Key: {key}")
            return response
        
        except ClientError as e:
            logger.exception(f"Error retrieving {key}: {e}")
            raise e

    
    def put_object(self, bucket: str, key: str, data: dict) -> None:
        """
        Puts an object into an S3 bucket

        Args:
            bucket (str): The name of the S3 bucket.
            key (str): The key for the object to put.
            data (bytes): The data to write to the S3 object.
        """
        try:
            logger.info(f"Putting data to the following S3 Bucket: {bucket} and S3 Key: {key}")
            self.s3.put_object(Bucket=bucket, Key=key, Body=data)
        
        except ClientError as e:
            logger.exception(f"Error putting data to {key}: {e}")
            raise e
    
    def update_metadata(self, bucket: str, metadata_object: str, metadata_key: str) -> None:
        """
        Updates the metadata of an S3 bucket by adding or updating a specific key with the current timestamp

        Args:
            bucket (str): The name of the S3 bucket.
            metadata_object (str): The metadata object key in the S3 bucket.
            metadata_key (str): The metadata key to update with the current timestamp.
        """
        # Retrieve todays date and convert to string format
        today = datetime.now().strftime('%Y-%m-%d')

        # Update metadata with current run date
        response = self.get_object(bucket=bucket, key=metadata_object)
        metadata = json.loads(response['Body'].read().decode('utf-8'))
        metadata[f'{metadata_key}'] = today
        self.put_object(bucket=bucket, key=metadata_object, data=json.dumps(metadata).encode('utf-8'))

class SNSNotifier:
    """
    Class for sending notifications using AWS SNS service
    
    Instance Variables
    ------------------
    access_key_id (str): AWS Access Key
    secret_access_key (str): AWS Secret Access Key
    region (str): AWS region
    topic_arn (str): ARN of the SNS topic to which notifications will be sent

    Methods
    -------
    get_client() -> BaseClient:
        Builds a connection for SNS
    build_message(context) -> str:
        Builds a message payload for SNS notification based on the Airflow context.
    send(context) -> None:
        Sends an SNS notification with relevant information about the Airflow task failure.
    __call__(context) -> None:
        Calls send function

    """
    def __init__(self, access_key_id, secret_access_key, region, topic_arn):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.region = region
        self.topic_arn = topic_arn
    
    def get_client(self) -> BaseClient:
        """
        Builds a connection for SNS 
        """
        return boto3.client(
            'sns',
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region
        )
    
    def build_message(self, context: dict) -> str:
        """
        Builds a message payload for SNS notification based on the Airflow context.
        
        Args:
            context (dict): Airflow context dictionary containing information about the DAG run, task instance, and any exceptions.
        
        Returns:
            str: A JSON-formatted string containing relevant information for the notification.
        """
        # Retrieves relevant information from Airflow context to include in notification message
        task_instance = context.get("task_instance")
        dag = context.get("dag")
        exception = context.get("exception")

        # Create payload information to be sent in SNS notification
        payload = {
            "dag_id": dag.dag_id if dag else None,
            "task_id": task_instance.task_id if task_instance else None,
            "execution_date": str(context.get("execution_date")),
            "log_url": task_instance.log_url if task_instance else None,
            "exception": str(exception) if exception else None,
        }

        # Json payload to be sent in SNS notification
        return json.dumps(payload, indent=2)

    def send(self, context: dict) -> None:
        """
        Sends an SNS notification with relevant information about the Airflow task failure.
        
        Args:
            context (dict): Airflow context dictionary containing information about the DAG run, task instance, and any exceptions.
        """
        # Retrieves relevant information from Airflow context to include in notification message
        dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
        task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown"

        # Build message payload for SNS notification
        message = self.build_message(context)

        # Create SNS connection
        sns_client = self.get_client()

        # Send SNS notification
        sns_client.publish(
            TopicArn=self.topic_arn,
            Subject=f"Airflow Alert: {dag_id}.{task_id} Failed",
            Message=message,
        )
    
    def __call__(self, context):
        """
        Calls send function
        
        Args:
            context (dict): Airflow context dictionary containing information about the DAG run, task instance, and any exceptions.
        """
        self.send(context)


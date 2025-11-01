# Import modules
import os
import json
from datetime import datetime
import boto3

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
        return self.s3.get_object(Bucket=bucket, Key=key)
    
    def put_object(self, bucket: str, key: str, data: dict) -> None:
        """
        Puts an object into an S3 bucket

        Args:
            bucket (str): The name of the S3 bucket.
            key (str): The key for the object to put.
            data (bytes): The data to write to the S3 object.
        """
        self.s3.put_object(Bucket=bucket, Key=key, Body=data)
    
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
        metadata = self.get_object(bucket=bucket, key=metadata_object)
        metadata = json.loads(metadata['Body'].read().decode('utf-8'))
        metadata[f'{metadata_key}'] = today
        self.put_object(bucket=bucket, key=metadata_object, data=json.dumps(metadata).encode('utf-8'))


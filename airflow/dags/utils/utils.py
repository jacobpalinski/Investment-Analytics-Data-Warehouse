# Import necessary libraries
import datetime
import os
import base64
import json
import boto3
import snowflake.connector
from dotenv import load_dotenv

def create_snowflake_connection(user: str, account: str, private_key_encoded: bytes, warehouse: str, database: str, schema: str) -> snowflake.connector.SnowflakeConnection:
    """
    Creates a Snowflake connection using the provided credentials.
    
    Args:
        user (str): Snowflake username.
        password (str): Snowflake password.
        account (str): Snowflake account identifier.
        warehouse (str): Snowflake warehouse name.
        database (str): Snowflake database name.
        schema (str): Snowflake schema name.

    Returns:
        snowflake.connector.SnowflakeConnection: A connection object to interact with Snowflake.
    """
    private_key_decoded = base64.b64decode(private_key_encoded)

    return snowflake.connector.connect(
        user=user,
        private_key=private_key_decoded,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

def s3_get_object(bucket: str, key: str) -> dict:
    """
    Retrieves an object from an S3 bucket.

    Args:
        bucket (str): The name of the S3 bucket.
        key (str): The key of the object to retrieve.

    Returns:
        dict: The S3 object.
    """
    s3 = boto3.client('s3', aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET"))
    return s3.get_object(Bucket=bucket, Key=key)

def s3_put_object(bucket: str, key: str, data: dict) -> None:
    """
    Puts an object into an S3 bucket.

    Args:
        bucket (str): The name of the S3 bucket.
        key (str): The key for the object to put.
        data (bytes): The data to write to the S3 object.
    """
    s3 = boto3.client('s3', aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET"))
    s3.put_object(Bucket=bucket, Key=key, Body=data)

def read_sql_file(file_path: str) -> str:
    """
    Reads a SQL file and returns its content.

    Args:
        file_path (str): The path to the SQL file.

    Returns:
        str: The content of the SQL file.
    """
    with open(file_path, 'r') as file:
        return file.read()


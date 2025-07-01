# Import necessary libraries
import os
import boto3
import snowflake.connector
from dotenv import load_dotenv

def create_snowflake_connection(user: str, password: str, account: str, warehouse: str, database: str, schema: str) -> snowflake.connector.SnowflakeConnection:
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
    return snowflake.connector.connect(
        user=user,
        password=password,
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



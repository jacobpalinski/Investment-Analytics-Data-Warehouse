# Import necessary libraries
import datetime
import os
import json
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

def company_financials_extraction_helper(cik: str, fiscal_year: int, fiscal_quarter: int, financial_statement: str, key: str, json_path: json, financials_data: list) -> dict:
    """
    Helper function to extract financial data from a JSON response.

    Args:
        financial_statement (str): The type of financial statement (e.g., 'income_statement').
        key (str): The key to extract from the JSON.
        json_path (json): The JSON object containing the financial data.

    Returns:
        dict: A dictionary containing the extracted financial data.
    """
    value = json_path.get('value')
    currency = json_path.get('unit', 'USD')
    if value is not None:
        financials_data.append((cik, fiscal_year, fiscal_quarter, financial_statement, key, currency, value))


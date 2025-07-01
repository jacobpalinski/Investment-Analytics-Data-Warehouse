# Import necessary libraries
import os
import io
import requests
import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import finnhub
import time
from dotenv import load_dotenv
from airflow.dags.utils.utils import create_snowflake_connection, s3_get_object

def extract_company_info():
    """
    Extracts company information from Finnhub API and loads it into Snowflake.
    """

    # Create snowflake connection
    snowflake_conn = create_snowflake_connection(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_RAW_SCHEMA")
    )
    
    # Load environment variables
    load_dotenv()
    finnhub_api_key = os.getenv("FINNHUB_API_KEY")

    # Load SQL from file
    with open("create_raw_company_information.sql", "r") as file:
        create_table_sql = file.read()

    # Retrieve Nasdaq listed tickers csv file from S3
    s3_object = s3_get_object(bucket='s3_bucket', key='s3_key') # Adjust bucket and key later
    nasdaq_listed_tickers_df = pd.read_csv(s3_object['Body'])
    tickers = [f"{ticker}" for ticker in nasdaq_listed_tickers_df['Symbol'].dropna().tolist()]

    # Retrieve company information from Finnhub API
    finnhub_client = finnhub.Client(api_key=finnhub_api_key)
    company_info = []

    for ticker in tickers:
        try:
            response = finnhub_client.company_profile2(symbol=ticker)
    
        # Check if the response is valid
            if not response:    
                print(f"No data found for {ticker}")
                continue 

            company_info.append({
                "cik": response.get("cik", ""),
                "company_name": response.get("name", ""),
                "ticker_symbol": ticker,
                "industry": response.get("finnhubIndustry", "")
            })
    
        except Exception as e:
            print(f"Error for {ticker}: {e}")
            continue

        time.sleep(1)  # Sleep to handle 60 calls per minute rate limit

    # Run the SQL
    with snowflake_conn.cursor() as cur:
        cur.execute(create_table_sql)

    # Ingest data into Snowflake
    company_info_df = pd.DataFrame(company_info)
    success, nchunks, nrows, _ = write_pandas(snowflake_conn, company_info_df, table_name="raw_company_information", database=os.getenv("SNOWFLAKE_DATABASE"), schema=os.getenv("SNOWFLAKE_RAW_SCHEMA"))
    print(f"Loaded {nrows} rows. Success: {success}")



    



    

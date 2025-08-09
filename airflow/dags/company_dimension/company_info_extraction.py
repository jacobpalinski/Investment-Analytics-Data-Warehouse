# Import necessary libraries
import os
import json
import io
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import requests
import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from polygon import RESTClient
import finnhub
import time
from dotenv import load_dotenv
from utils.utils import create_snowflake_connection, s3_get_object, s3_put_object

def extract_company_info():
    """
    Extracts company information from Finnhub and Polygon API and loads it into Snowflake.
    """

    # Load environment variables
    load_dotenv()

    # Create snowflake connection
    snowflake_conn = create_snowflake_connection(
        user=os.getenv("SNOWFLAKE_USER"),
        private_key_encoded=os.getenv("SNOWFLAKE_PRIVATE_KEY_B64"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse='INVESTMENT_ANALYTICS_DWH',
        database='INVESTMENT_ANALYTICS',
        schema='RAW'
    )
    
    # Initialize API keys
    finnhub_api_key = os.getenv("FINNHUB_API_KEY")
    polygon_api_key = os.getenv("POLYGON_API_KEY")

    # Retrieve Nasdaq listed tickers csv file from S3
    s3_object = s3_get_object(bucket=os.getenv('AWS_S3_BUCKET'), key='nasdaq_listed_symbols_20250528.csv') # Adjust bucket and key later
    nasdaq_listed_tickers_df = pd.read_csv(s3_object['Body'])
    tickers = [f"{ticker}" for ticker in nasdaq_listed_tickers_df['Symbol'].dropna().tolist()]

    # Retrieve company name, ticker symbol and industry information from Finnhub API along with CIK from Polygon.io
    finnhub_client = finnhub.Client(api_key=finnhub_api_key)
    polygon_client = RESTClient(polygon_api_key)
    company_info = []

    for ticker in tickers[:500]:  # Limit to first 500 tickers for testing
        try:
            finnhub_response = finnhub_client.company_profile2(symbol=ticker)
            polygon_response = polygon_client.get_ticker_details(ticker)
    
            # Check if the response is valid
            if not (finnhub_response and polygon_response):    
                print(f"No data found for {ticker}")
                continue

            company_info.append({
                "cik": getattr(polygon_response, "cik", None),
                "company_name": finnhub_response.get("name", ""),
                "ticker_symbol": ticker,
                "industry": finnhub_response.get("finnhubIndustry", "")
            })
    
        except Exception as e:
            print(f"Error for {ticker}: {e}")
            continue

        print('Extracted company information for: ', ticker)

        time.sleep(1)  # Sleep to handle 60 calls per minute rate limit

    # Ingest data into Snowflake
    company_info_df = pd.DataFrame(company_info)
    company_info_df.columns = map(str.upper, company_info_df.columns)  # Convert column names to uppercase
    success, nchunks, nrows, _ = write_pandas(snowflake_conn, company_info_df, table_name="RAW_COMPANY_INFORMATION", database=os.getenv("SNOWFLAKE_DATABASE"), schema=os.getenv("SNOWFLAKE_RAW_SCHEMA"))
    print(f"Loaded {nrows} rows. Success: {success}")

    '''# Retrieve todays date and convert to string format
    today = datetime.now().strftime('%Y-%m-%d')

    # Update metadata with current run date
    metadata = s3_get_object(bucket=os.getenv('AWS_S3_BUCKET'), key='metadata.json')
    metadata = json.loads(metadata['Body'].read().decode('utf-8'))
    metadata['company_dimension'] = today
    s3_put_object(bucket=os.getenv('AWS_S3_BUCKET'), key='metadata.json', data=json.dumps(metadata).encode('utf-8'))'''



    



    

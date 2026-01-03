# Import necessary libraries
import logging
import os
import json
import io
import time
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import logging
from dags.api_extraction.finnhub_api import FinnhubApi
from dags.api_extraction.polygon_api import PolygonApi
from dags.utils.s3_utils import S3
from dags.utils.snowflake_utils import Snowflake

def extract_company_info():
    """
    Extracts company information from Finnhub and Polygon API and loads it into Snowflake.
    """

    # Create setup for logging
    logger = logging.getLogger(__name__)

    # Load environment variables
    load_dotenv()

    # Instantiate Snowflake Client
    snowflake_client = Snowflake(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        private_key_encoded=os.getenv("SNOWFLAKE_PRIVATE_KEY_B64")
    )

    # Create snowflake connection
    snowflake_conn = snowflake_client.create_connection(
    warehouse='INVESTMENT_ANALYTICS_DWH',
    database='INVESTMENT_ANALYTICS',
    schema='RAW')

    # Instantiate finnhub and polygon clients
    finnhub_api_client = FinnhubApi(finnhub_api_key=os.getenv("FINNHUB_API_KEY"))
    polygon_api_client = PolygonApi(polygon_api_key=os.getenv("POLYGON_API_KEY"))

    # Instantiate S3 class
    s3_client = S3(aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))

    # Retrieve latest date of Nasdaq listed tickers extraction
    metadata = s3_client.get_object(bucket=os.getenv('AWS_S3_BUCKET'), key='metadata.json')
    metadata = json.loads(metadata['Body'].read().decode('utf-8'))
    latest_run_date = metadata.get('nasdaq_listed_tickers')[0]
    latest_run_date_no_hyphen = datetime.strptime(latest_run_date, "%Y-%m-%d").strftime("%Y%m%d")

    # Retrieve Nasdaq listed tickers csv file from S3
    s3_object = s3_client.get_object(bucket=os.getenv('AWS_S3_BUCKET'), key=f'nasdaq_listed_symbols_{latest_run_date_no_hyphen}.csv') # Adjust bucket and key later
    nasdaq_listed_tickers_df = pd.read_csv(s3_object['Body'])
    tickers = [f"{ticker}" for ticker in nasdaq_listed_tickers_df['Symbol'].dropna().tolist()]

    # Create company info list
    company_info = []

    for ticker in tickers:
        try:
            # Get response from APIs
            finnhub_response = finnhub_api_client.extract_company_profile(ticker=ticker)
            polygon_response = polygon_api_client.extract_company_cik(ticker=ticker)
    
            # Check if the response is valid
            if not (finnhub_response and polygon_response):    
                continue
            
            # Append results to company info list
            company_info.append({
                "cik": getattr(polygon_response, "cik", None),
                "company_name": finnhub_response.get("name", ""),
                "ticker_symbol": ticker,
                "industry": finnhub_response.get("finnhubIndustry", "")
            })
    
        except Exception as e:
            logger.error(f"Error for {ticker}: {e}", exc_info=True)
        
        # Sleep to handle 60 calls per minute rate limit
        time.sleep(1.25)
    
    # Load data into Snowflake test table
    snowflake_client.load_to_snowflake(connection=snowflake_conn, data=company_info, target_table='RAW_COMPANY_INFORMATION')



    



    

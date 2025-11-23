# Import necessary libraries
import os
import json
import requests
from datetime import datetime, date
import pandas as pd
import logging
from dotenv import load_dotenv
from dags.utils.snowflake_utils import Snowflake
from dags.utils.s3_utils import S3
from dags.api_extraction.fred_api import FredApi

def extract_economic_indicators():
    """
    Extracts economic indicators from FRED API and loads it into Snowflake.
    """
    # Create setup for logging
    logger = logging.getLogger(__name__)

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

    # Instantiate FRED API client
    fred_api_key = os.getenv("FRED_API_KEY")
    fred_api_client = FredApi(fred_api_key=fred_api_key)

    # Define the economic indicators and their corresponding series IDs
    series = {
        'interest_rate': 'DFF',
        'unemployment_rate': 'UNRATE',
        'gdp_growth_rate': 'A191RL1Q225SBEA',
        'consumer_confidence': 'UMCSENT',
        'consumer_price_index': 'CPIAUCSL',
    }

    # Current date information
    today = datetime.now()
    date = today.date()
    year = today.year
    month_int = today.month
    month_name = today.strftime('%B')
    quarter = f"Q{(month_int - 1) // 3 + 1}"
    day = today.day

    # Economic indicator data storage
    economic_indicators = []

    # Retrieve economic indicators from FRED API
    for indicator, series_id in series.items():
        indicator_series = fred_api_client.extract_fred_data(series_id=series_id)

        if not indicator_series.empty:
            value = indicator_series.iloc[0]
        else:
            logger.warning(f"No data found for {indicator}")
            value = None

        economic_indicators.append({'date': date, 'year': year, 'quarter': quarter, 'month': month_name, 'day': day, 'indicator': indicator, 'value': value})
    
    # Load data into Snowflake test table
    snowflake_client.load_to_snowflake(connection=snowflake_conn, data=economic_indicators, target_table='RAW_ECONOMIC_INDICATORS')
    


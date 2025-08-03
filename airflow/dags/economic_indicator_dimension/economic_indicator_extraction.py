# Import necessary libraries
import os
import json
import requests
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd
from fredapi import Fred
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from utils.utils import create_snowflake_connection, s3_get_object, s3_put_object

def extract_economic_indicators():
    """
    Extracts economic indicators from FRED API and loads it into Snowflake.
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

    # Define the economic indicators and their corresponding series IDs and frequencies
    series = {
        'interest_rate': 'DFF',
        'unemployment_rate': 'UNRATE',
        'gdp_growth_rate': 'A191RL1Q225SBEA',
        'consumer_confidence': 'UMCSENT',
        'consumer_price_index': 'CPIAUCSL',
    }

    # Intiialise FRED API key and FRED API client
    fred_api_key = os.getenv("FRED_API_KEY")
    fred_client = Fred(api_key=fred_api_key)
    #url = "https://api.stlouisfed.org/fred/observations"

    # Current date information
    today = datetime.now()
    year = today.year
    month_int = today.month
    month_name = today.strftime('%B')
    quarter = f"Q{(month_int - 1) // 3 + 1}"

    # Economic indicator data storage
    economic_indicators = []

    # Retrieve economic indicators from an external API (e.g., FRED)
    '''for indicator, values in series.items():
        params = {
        'series_id': values[0],
        'realtime_start': '2025-01-01',
        'api_key': fred_api_key,
        'file_type': 'json',
        'units': 'lin',
        'sort_order': 'desc'
        #'limit': 1
        } '''
    
    for indicator, series_id in series.items():
        indicator_series = fred_client.get_series(series_id, sort_order='desc', limit=1)
        print(indicator_series)

        if not indicator_series.empty:
            value = indicator_series.iloc[0]
        else:
            print(f"No data found for {indicator}")
            continue

        economic_indicators.append({'year': year, 'quarter': quarter, 'month': month_name, 'indicator': indicator, 'value': value})
        
    # Process and prepare data for Snowflake
    economic_indicators_df = pd.DataFrame(economic_indicators)
    economic_indicators_df.columns = map(str.upper, economic_indicators_df.columns)  # Convert column names to uppercase
    
    # Load data into Snowflake
    success, nchunks, nrows, _ = write_pandas(snowflake_conn, economic_indicators_df, table_name="RAW_ECONOMIC_INDICATORS", database=os.getenv("SNOWFLAKE_DATABASE"), schema=os.getenv("SNOWFLAKE_RAW_SCHEMA"))
    print(f"Loaded {nrows} rows. Success: {success}")

    # Update metadata in S3
    today_str = today.strftime('%Y-%m-%d')
    


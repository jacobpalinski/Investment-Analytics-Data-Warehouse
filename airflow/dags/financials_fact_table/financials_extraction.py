# Import necessary libraries
import os
import pandas as pd
import logging
from dotenv import load_dotenv
from dags.api_extraction.sec_api import SecApi
from dags.utils.s3_utils import S3
from dags.utils.snowflake_utils import Snowflake

def extract_financials():
    '''Extracts company financials data from SEC API and loads it into Snowflake. '''

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

    # Instantiate SEC API client
    sec_api_client = SecApi(user_agent=os.getenv("SEC_API_USER_AGENT"))

    # Retrieve current CIKs from dim_company dimension table in Snowflake
    ciks = snowflake_client.query_current_ciks(connection=snowflake_conn, schema='CORE', table_name='DIM_COMPANY')

    # Create financials data list
    financials_data = []

    for cik in ciks:
        try:
            # Fetch financials for the CIK
            financials = sec_api_client.sec_data_request(cik=cik)

            # Append financials items to the list            
            parsed_financials = sec_api_client.extract_financial_data(cik=cik, response=financials)
            financials_data.append(parsed_financials)
        except Exception as e:
            logger.error(f"Error fetching financials for CIK {cik}: {e}", exc_info=True)
    
    # Load data into Snowflake test table
    snowflake_client.load_to_snowflake(connection=snowflake_conn, data=financials_data, target_table='RAW_FINANCIALS')





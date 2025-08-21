# Import necessary libraries
import os
import json
import requests
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from polygon import RESTClient
from dotenv import load_dotenv
from utils.utils import create_snowflake_connection, s3_get_object, s3_put_object
from financials_fact_table.financials_functions import polygon_parse_response, parse_response_sec_api

def extract_financials():
    '''Extracts company financials data from Polygon API and loads it into Snowflake.'''
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

    # Retrieve current CIKs from dim_company dimension table in Snowflake
    with snowflake_conn.cursor() as cursor:
        cursor.execute("""
                       select 
                       distinct 
                       cik 
                       from investment_analytics.core.dim_company
                       where is_current = TRUE """)
        ciks = [row[0] for row in cursor.fetchall()]
    
    # Retrieve todays date and convert to string format
    today = datetime.now().strftime('%Y-%m-%d')

    # Retrieve date 92 days prior to today in string format
    three_months_prior = date.today() - relativedelta(months=3, days=2)
    three_months_prior_str = three_months_prior.strftime('%Y-%m-%d')

    # Retrieve the date of last run of financials_dimension DAG
    ''' metadata = s3_get_object(bucket=os.getenv('AWS_S3_BUCKET'), key='metadata.json')
    metadata = json.loads(metadata['Body'].read().decode('utf-8'))
    latest_run_date = metadata.get('financials_dimension', three_months_prior_str) '''
    latest_run_date = three_months_prior_str

    # Initialise Polygon API client
    polygon_api_key = os.getenv("POLYGON_API_KEY")
    polygon_client = RESTClient(polygon_api_key)

    # Create headers for SEC API requests
    sec_api_headers = {'User-Agent': os.getenv("SEC_API_USER_AGENT")}

    # Create a list to hold financials_data and pass to parsing functions
    financials_data = []

    for cik in ciks[:500]:
        try:
            # Retrieve financials data for the current CIK
            quarterly_financials_response = list(polygon_client.vx.list_stock_financials(cik=cik, 
            filing_date_gt=latest_run_date, filing_date_lte=today, timeframe='quarterly',
            include_sources=False, order="desc", limit=1, sort="period_of_report_date"))
            annual_financials_response = list(polygon_client.vx.list_stock_financials(cik=cik, 
            filing_date_gt=latest_run_date, filing_date_lte=today, timeframe='annual', include_sources=False, order="desc", limit=1, sort="period_of_report_date"))
            
            # Check if responses don't exist
            if not quarterly_financials_response and not annual_financials_response:
                # If not retrieve data from SEC API
                url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
                response = requests.get(url, headers=sec_api_headers, timeout=10)
                
                if response.status_code != 200:
                    print(f"Failed to retrieve data for CIK: {cik}, Status Code: {response.status_code}")
                    continue
                
                facts = response.json().get('facts', {})
                data = facts.get('us-gaap') or facts.get('ifrs-full', {})
                parse_response_sec_api(response=data, cik=cik, financials_data=financials_data)

            elif quarterly_financials_response and not annual_financials_response:
                polygon_parse_response(response=quarterly_financials_response, cik=cik, financials_data=financials_data)
                print('Parsed')
            
            elif not quarterly_financials_response and annual_financials_response:
                polygon_parse_response(response=annual_financials_response, cik=cik, financials_data=financials_data)
            
            else:
                polygon_parse_response(response=quarterly_financials_response, cik=cik, financials_data=financials_data)
                polygon_parse_response(response=annual_financials_response, cik=cik, financials_data=financials_data)     
        
        except Exception as e:
            print(f"Error extracting financials for {cik}: {e}")
            continue
        
        print(f'Extracted financials for CIK: {cik}')
    
    # Load financials data into Snowflake
    financials_df = pd.DataFrame(financials_data)
    financials_df.columns = map(str.upper, financials_df.columns)  # Convert column names to uppercase
    success, nchunks, nrows, _ = write_pandas(snowflake_conn, financials_df, table_name="RAW_FINANCIALS")
    print(f"Loaded {nrows} rows. Success: {success}")

    # Update metadata with current run date
    ''' metadata['financials_dimension'] = today
    s3_put_object(bucket=os.getenv('AWS_S3_BUCKET'), key='metadata.json', data=json.dumps(metadata).encode('utf-8')) '''





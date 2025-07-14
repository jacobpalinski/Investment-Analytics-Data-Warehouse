# Import necessary libraries
import os
import io
import requests
import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from polygon import RESTClient
from datetime import datetime
import finnhub
import time
from dotenv import load_dotenv
from utils.utils import create_snowflake_connection, company_financials_extraction_helper

def extract_financials():
    '''Extracts company financials data from Polygon API and loads it into Snowflake.'''
    # Load environment variables
    load_dotenv()

    # Create snowflake connection
    snowflake_conn = create_snowflake_connection(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
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
                       from dim_company
                       where is_current = TRUE """)
        ciks = [row[0] for row in cursor.fetchall()]
    
    # Retrieve current quarter end date and convert to string format
    today = datetime.now().strftime('%Y-%m-%d')

    # Initialise Polygon API client
    polygon_api_key = os.getenv("POLYGON_API_KEY")
    polygon_client = RESTClient(polygon_api_key)

    financials_data = []

    for cik in ciks:
        try:
            # Retrieve financials data for the current CIK
            quarterly_financials_response = polygon_client.vx.list_stock_financials(cik=cik, 
            period_of_report_date_lte=today, timeframe='quarter',
            include_sources=False, order="desc", limit=1, sort="period_of_report_date")
            annual_financials_response = polygon_client.vx.list_stock_financials(cik=cik, 
            period_of_report_date_lte=today, timeframe='annual', include_sources=False, order="desc", limit=1, sort="period_of_report_date")
            
            # Check if both responses exist
            if not quarterly_financials_response and not annual_financials_response:
                print(f"No financial data found for CIK: {cik}")
                continue
            
            if datetime(quarterly_financials_response['results'][0]['start_date'], "%Y-%m-%d") > datetime(annual_financials_response['results'][0]['end_date'], "%Y-%m-%d"):
                financials_response = quarterly_financials_response
            else:
                financials_response = annual_financials_response

            # Extract fiscal year and quarter from the response
            fiscal_year = financials_response['results'][0]['fiscal_year']
            fiscal_quarter = financials_response['results'][0].get('fiscal_period', None) 
            
            # Process the financials data
            # Income Statement
            income_statement = financials_response.get('income_statement', {})
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='income_statement',
                key='income_statement',
                json_path=income_statement.get('revenues', {}),
                financials_data=financials_data
            )
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='income_statement',
                key='income_statement',
                json_path=income_statement.get('gross_profit', {}),
                financials_data=financials_data
            )
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='income_statement',
                key='income_statement',
                json_path=income_statement.get('operating_income_loss', {}),
                financials_data=financials_data
            )
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='income_statement',
                key='income_statement',
                json_path=income_statement.get('net_income_loss', {}),
                financials_data=financials_data
            )
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='income_statement',
                key='income_statement',
                json_path=income_statement.get('basic_earnings_per_share', {}),
                financials_data=financials_data
            )
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='income_statement',
                key='income_statement',
                json_path=income_statement.get('diluted_earnings_per_share', {}),
                financials_data=financials_data
            )
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='income_statement',
                key='income_statement',
                json_path=income_statement.get('operating_expenses', {}),
                financials_data=financials_data
            )
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='income_statement',
                key='income_statement',
                json_path=income_statement.get('income_tax_benefit', {}),
                financials_data=financials_data
            )
            # Balance Sheet
            balance_sheet = financials_response.get('balance_sheet', {})
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='balance_sheet',
                key='balance_sheet',
                json_path=balance_sheet.get('assets', {}),
                financials_data=financials_data
            )
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='balance_sheet',
                key='balance_sheet',
                json_path=balance_sheet.get('liabilities', {}),
                financials_data=financials_data
            )
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='balance_sheet',
                key='balance_sheet',
                json_path=balance_sheet.get('equity', {}),
                financials_data=financials_data
            )
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='balance_sheet',
                key='balance_sheet',
                json_path=balance_sheet.get('current_assets', {}),
                financials_data=financials_data
            )
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='balance_sheet',
                key='balance_sheet',
                json_path=balance_sheet.get('current_liabilities', {}),
                financials_data=financials_data
            )
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='balance_sheet',
                key='balance_sheet',
                json_path=balance_sheet.get('noncurrent_liabilities', {}),
                financials_data=financials_data
            )
            # Cash Flow Statement
            cash_flow_statement = financials_response.get('cash_flow_statement', {})
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='cash_flow_statement',
                key='cash_flow_statement',
                json_path=cash_flow_statement.get('net_cash_flow_from_operating_activities', {}),
                financials_data=financials_data
            )
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='cash_flow_statement',
                key='cash_flow_statement',
                json_path=cash_flow_statement.get('net_cash_flow_from_investing_activities', {}),
                financials_data=financials_data
            )
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='cash_flow_statement',
                key='cash_flow_statement',
                json_path=cash_flow_statement.get('net_cash_flow_from_financing_activities', {}),
                financials_data=financials_data
            )
            company_financials_extraction_helper(
                cik=cik,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                financial_statement='cash_flow_statement',
                key='cash_flow_statement',
                json_path=cash_flow_statement.get('net_cash_flow', {}),
                financials_data=financials_data
            )
        
        except Exception as e:
            print(f"Error extracting financials for {cik}: {e}")
            continue
        
        print(f'Extracted financials for CIK: {cik}')
    
    # Load financials data into Snowflake
    financials_df = pd.DataFrame(financials_data)
    financials_df.columns = map(str.upper, financials_df.columns)  # Convert column names to uppercase
    success, nchunks, nrows, _ = write_pandas(snowflake_conn, financials_df, table_name="RAW_FINANCIALS", database=os.getenv("SNOWFLAKE_DATABASE"), schema=os.getenv("SNOWFLAKE_RAW_SCHEMA"))
    print(f"Loaded {nrows} rows. Success: {success}")





# Import modules
import os
from datetime import datetime, timedelta
import time
import logging
import pytest
import requests
import pandas as pd
from dotenv import load_dotenv
from utils.snowflake_utils import Snowflake
from dags.api_extraction.finnhub_api import FinnhubApi
from dags.api_extraction.polygon_api import PolygonApi
from dags.api_extraction.news_api import NewsApi
from utils.s3_utils import S3

# Create setup for logging
logger = logging.getLogger(__name__)

class IntegrationTests:
    """ Class for integration tests """
    def test_extract_company_info(self):
        """
        Tests extract_company_info from Finnhub and Polygon API and loads it into Snowflake (test table)
        """
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
        schema='TST')

        # Instantiate finnhub and polygon clients
        finnhub_api_client = FinnhubApi(finnhub_api_key=os.getenv("FINNHUB_API_KEY"))
        polygon_api_client = PolygonApi(polygon_api_key=os.getenv("POLYGON_API_KEY"))

        # Instantiate S3 class
        s3_client = S3(aws_access_key_id=os.getenv("AWS_ACCESS"), aws_secret_access_key=os.getenv("AWS_SECRET"))

        # Retrieve current date
        today = datetime.now().strftime('%Y%m%d')

        # Retrieve Nasdaq listed tickers csv file from S3
        s3_object = s3_client.get_object(bucket=os.getenv('AWS_S3_BUCKET'), key=f'nasdaq_listed_symbols_{today}.csv') # Adjust bucket and key later
        nasdaq_listed_tickers_df = pd.read_csv(s3_object['Body'])
        tickers = [f"{ticker}" for ticker in nasdaq_listed_tickers_df['Symbol'].dropna().tolist()]

        # Create company info list
        company_info = []

        for ticker in tickers[:25]:
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
        snowflake_client.load_to_snowflake(connection=snowflake_conn, data=company_info, table_name='COMPANY_INFORMATION_TST')
    
    def test_extract_company_news(self):
        """
        Tests extract_company_news from Polygon API and loads it into Snowflake (test table)
        """
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
        schema='TST')

        # Instantiate polygon client
        polygon_api_client = PolygonApi(polygon_api_key=os.getenv("POLYGON_API_KEY"))
        
        # Retrieve current ticker symbols from dim_company dimension table in Snowflake
        tickers = snowflake_client.query_current_ciks(snowflake_conn)

        # Filter ticker symbols for testing purposes
        ticker_symbols_filter = ['AAPL', 'ABNB', 'AAL']
        filtered_tickers = [t for t in tickers if t in ticker_symbols_filter]

        # Retrieve yesterday's date in string format
        yesterday = datetime.today() - timedelta(days=1)
        formatted_date = yesterday.strftime('%Y-%m-%d')

        # Create company news data list
        company_news_data = []

        # Fetch news for each ticker
        for ticker in filtered_tickers:
            try:
                # Fetch news for the ticker
                news = polygon_api_client.extract_company_news(ticker=ticker)

                # Append news items to the list            
                for item in news:
                    company_news_data.append({
                        "date": formatted_date,
                        "ticker_symbol": ticker,
                        "title": item.title,
                        "description": item.description,
                        "source": item.publisher.name,
                    })
            except Exception as e:
                logger.error(f"Error fetching news for {ticker}: {e}", exc_info=True)
        
        # Load data into Snowflake test table
        snowflake_client.load_to_snowflake(connection=snowflake_conn, data=company_news_data, table_name='COMPANY_NEWS_TST')
    
    def test_extract_non_company_news(self):
        """
        Tests extract_non_company_news from News API and loads it into Snowflake (test table)
        """
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
        schema='TST')

        # Instantiate News API client
        news_api_client = NewsApi(news_api_key=os.getenv("NEWS_API_KEY"))

        # Parameters for API calls
        parameters= [
        {"category": "business", "country": "us", "qInMeta": "economy AND interest rate"},
        {"category": "business", "country": "us", "qInMeta": "economy AND inflation"},
        {"category": "business", "country": "us", "qInMeta": "economy AND Federal Reserve"},
        {"category": "business", "country": "us", "qInMeta": "economy AND consumer confidence"},
        {"category": "business", "country": "us", "qInMeta": "economy and unemployment"},
        {"category": "business", "country": "us", "qInMeta": "economy AND GDP"},
        {"category": "business", "country": "us", "qInMeta": "economy AND tariffs"},
        {"category": "business", "country": "us", "qInMeta": "economy AND treasury yields"},
        {"category": "business", "country": "us", "qInMeta": "economy AND trade balance"},
        {"category": "business", "country": "us", "qInMeta": "economy AND retail sales"},
        {"category": "business", "country": "us", "qInMeta": "economy AND CPI"},
        {"category": "business", "country": "us", "qInMeta": "economy AND bond spreads"},
        {"category": "politics", "country": "us", "qInMeta": "Trump AND Canada"},
        {"category": "politics", "country": "us", "qInMeta": "Trump AND China"},
        {"category": "politics", "country": "us", "qInMeta": "Trump AND Mexico"},
        {"category": "politics", "country": "us", "qInMeta": "Trump AND EU"},
        {"category": "politics", "country": "us", "qInMeta": "Democrats"},
        {"category": "politics", "country": "us", "qInMeta": "Republicans"}
        ]

        # Create non-company news data list
        non_company_news_data = []

        # Execute API call for parameters
        for param in parameters:
            news_response = news_api_client.fetch_with_retry(params=parameters, timeout=10)
            # Append news items to the list
            for item in news_response["results"]:
                non_company_news_data.append({
                    "date": item.get("pubDate"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "source": item.get("source_name")
                })
        
        # Load data into Snowflake test table
        snowflake_client.load_to_snowflake(connection=snowflake_conn, data=non_company_news_data, table_name='NON_COMPANY_NEWS_TST')


    








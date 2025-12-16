# Import modules
import os
import time
import json
from datetime import datetime, timedelta, timezone
import time
import logging
import pytest
import requests
import pandas as pd
from dotenv import load_dotenv
from typing import List
from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import snowflake.connector
from dags.utils.snowflake_utils import Snowflake
from dags.api_extraction.finnhub_api import FinnhubApi
from dags.api_extraction.polygon_api import PolygonApi
from dags.api_extraction.news_api import NewsApi
from dags.api_extraction.reddit_api import RedditApi
from dags.api_extraction.sec_api import SecApi
from dags.api_extraction.fred_api import FredApi
from dags.utils.s3_utils import S3

# Create setup for logging
logger = logging.getLogger(__name__)

class TestIntegrationTesting:
    """ Class for integration tests """
    def test_extract_company_info(self):
        """
        Tests extract_company_info from Finnhub and Polygon API and loads it into Snowflake (test table)
        """
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
        s3_client = S3(aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET"))

        # Retrieve latest date of Nasdaq listed tickers extraction
        metadata = s3_client.get_object(bucket=os.getenv('AWS_S3_TST_BUCKET'), key='metadata.json')
        metadata = json.loads(metadata['Body'].read().decode('utf-8'))
        latest_run_date = metadata.get('nasdaq_listed_tickers')
        latest_run_date_no_hyphen = datetime.strptime(latest_run_date, "%Y-%m-%d").strftime("%Y%m%d")

        # Retrieve Nasdaq listed tickers csv file from S3
        s3_object = s3_client.get_object(bucket=os.getenv('AWS_S3_TST_BUCKET'), key=f'nasdaq_listed_symbols_{latest_run_date_no_hyphen}.csv') # Adjust bucket and key later
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
        snowflake_client.load_to_snowflake(connection=snowflake_conn, data=company_info, target_table='COMPANY_INFORMATION_TST')

        with snowflake_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM INVESTMENT_ANALYTICS.TST.COMPANY_INFORMATION_TST")
            result = cursor.fetchone()
            assert result[0] == len(company_info)
            cursor.execute("TRUNCATE TABLE INVESTMENT_ANALYTICS.TST.COMPANY_INFORMATION_TST")
    
    def test_extract_company_news(self):
        """
        Tests extract_company_news from Polygon API and loads it into Snowflake (test table)
        """
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
        with snowflake_conn.cursor() as cursor:
            cursor.execute("""
                        select 
                        distinct 
                        ticker_symbol
                        from investment_analytics.tst.dim_company
                        where is_current = TRUE """)
            tickers = [row[0] for row in cursor.fetchall()]

        # Filter ticker symbols for testing purposes
        ticker_symbols_filter = ['AAPL', 'ABNB', 'AAL']
        filtered_tickers = [t for t in tickers if t in ticker_symbols_filter]

        # Retrieve yesterday's date in string format
        yesterday = datetime.today() - timedelta(days=1)
        formatted_date = yesterday.strftime('%Y-%m-%d')

        # Create company news data list
        company_news = []

        # Fetch news for each ticker
        for ticker in filtered_tickers:
            try:
                # Fetch news for the ticker
                news = polygon_api_client.extract_company_news(ticker=ticker)

                # Append news items to the list            
                for item in news:
                    company_news.append({
                        "date": formatted_date,
                        "ticker_symbol": ticker,
                        "title": item.title,
                        "description": item.description,
                        "source": item.publisher.name,
                    })
            except Exception as e:
                logger.error(f"Error fetching news for {ticker}: {e}", exc_info=True)
        
        # Load data into Snowflake test table
        snowflake_client.load_to_snowflake(connection=snowflake_conn, data=company_news, target_table='COMPANY_NEWS_TST')

        with snowflake_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM INVESTMENT_ANALYTICS.TST.COMPANY_NEWS_TST")
            result = cursor.fetchone()
            assert result[0] == len(company_news)
            cursor.execute("TRUNCATE TABLE INVESTMENT_ANALYTICS.TST.COMPANY_NEWS_TST")
    
    def test_extract_non_company_news(self):
        """
        Tests extract_non_company_news from News API and loads it into Snowflake (test table)
        """
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
        non_company_news = []

        # Execute API call for parameters
        for param in parameters:
            try: 
                news_response = news_api_client.fetch_with_retry(params=param, timeout=10)
            
            except Exception as e:
                logger.warning("Skipping params %s after retries failed: %s", param, e)
                continue

            # Append news items to the list
            for item in news_response["results"]:
                non_company_news.append({
                    "date": item.get("pubDate"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "source": item.get("source_name")
                })
        
        # Load data into Snowflake test table
        snowflake_client.load_to_snowflake(connection=snowflake_conn, data=non_company_news, target_table='NON_COMPANY_NEWS_TST')

        with snowflake_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM INVESTMENT_ANALYTICS.TST.NON_COMPANY_NEWS_TST")
            result = cursor.fetchone()
            assert result[0] == len(non_company_news)
            cursor.execute("TRUNCATE TABLE INVESTMENT_ANALYTICS.TST.NON_COMPANY_NEWS_TST")
    
    def test_extract_reddit_submissions(self):
        """
        Tests extract_reddit_submissions from Reddit API and loads it into Snowflake (test table)
        """
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

        # Instantiate Reddit API client
        reddit_api_client = RedditApi(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=os.getenv("REDDIT_USER_AGENT"),
            username=os.getenv("REDDIT_USERNAME"),
            password=os.getenv("REDDIT_PASSWORD")
        )

        # Define subreddits to fetch submissions from
        subreddit_name = "investing"

        # Create reddit submissions data list
        submissions_data = []

        # Fetch submisions from subreddit
        reddit_submissions = reddit_api_client.extract_reddit_submissions(subreddit_name=subreddit_name, submissions_limit=5)

        for submission in reddit_submissions:
            submission_title = submission.title
            submission_utc = submission.created_utc
            submission_body = submission.selftext
            
            submissions_data.append({
                "date": submission_utc,
                "title": submission_title,
                "description": submission_body,
                "source": 'Reddit'
            })
        
        # Load data into Snowflake test table
        snowflake_client.load_to_snowflake(connection=snowflake_conn, data=submissions_data, target_table='REDDIT_SUBMISSIONS_TST')

        with snowflake_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM INVESTMENT_ANALYTICS.TST.REDDIT_SUBMISSIONS_TST")
            result = cursor.fetchone()
            assert result[0] == len(submissions_data)
            cursor.execute("TRUNCATE TABLE INVESTMENT_ANALYTICS.TST.REDDIT_SUBMISSIONS_TST")

    def test_financials_extraction(self):
        """
        Tests extract_financials from SEC API and loads it into Snowflake (test table)
        """
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

        # Instantiate SEC API client
        sec_api_client = SecApi(user_agent=os.getenv("SEC_API_USER_AGENT"))

        # Retrieve current CIKs from dim_company dimension table in Snowflake
        ciks = snowflake_client.query_current_ciks(connection=snowflake_conn, schema='TST', table_name='DIM_COMPANY')

        # Create financials data list
        financials_data = []

        for cik in ciks[:10]: # Limit first 10 CIKs for testing
            try:
                # Fetch financials for the CIK
                financials = sec_api_client.sec_data_request(cik=cik)

                # Append financials items to the list            
                parsed_financials = sec_api_client.extract_financial_data(cik=cik, response=financials)
                financials_data.extend(parsed_financials)
            except Exception as e:
                logger.error(f"Error fetching financials for CIK {cik}: {e}", exc_info=True)
        
        # Load data into Snowflake test table
        snowflake_client.load_to_snowflake(connection=snowflake_conn, data=financials_data, target_table='FINANCIALS_TST')

        with snowflake_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM INVESTMENT_ANALYTICS.TST.FINANCIALS_TST")
            result = cursor.fetchone()
            assert result[0] == len(financials_data)
            cursor.execute("TRUNCATE TABLE INVESTMENT_ANALYTICS.TST.FINANCIALS_TST")
    
    def test_extract_economic_indicators(self):
        """
        Tests extract_economic_indicators from FRED API and loads it into Snowflake (test table)
        """
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
        snowflake_client.load_to_snowflake(connection=snowflake_conn, data=economic_indicators, target_table='ECONOMIC_INDICATORS_TST')

        with snowflake_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM INVESTMENT_ANALYTICS.TST.ECONOMIC_INDICATORS_TST")
            result = cursor.fetchone()
            assert result[0] == len(economic_indicators)
            cursor.execute("TRUNCATE TABLE INVESTMENT_ANALYTICS.TST.ECONOMIC_INDICATORS_TST")
    
    def test_extract_nasdaq_listed_tickers(self):
        """
        Tests extract_nasdaq_listed_tickers from GitHub URL and create csv with tickers in S3
        """
        # Initantiates S3 class
        s3_client = S3(aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET"))

        # Github CSV file URL
        csv_url = 'https://raw.githubusercontent.com/datasets/nasdaq-listings/refs/heads/main/data/nasdaq-listed.csv'

        # Download raw data from GitHub URL
        response = requests.get(csv_url)
        if response.status_code == 200:
            nasdaq_listings_csv_data = response.content  # raw bytes
        else:
            raise Exception(f"Failed to download CSV. Status code: {response.status_code}")
        
        # Retrieve current data
        today = datetime.now().strftime('%Y%m%d')

        # Create file with todays date
        filename = f'nasdaq_listed_symbols_{today}.csv'

        # Upload csv file to S3
        s3_client.put_object(bucket=os.getenv('AWS_S3_TST_BUCKET'), key=filename, data=nasdaq_listings_csv_data)

        # Update metadata file in S3 with date of latest nasdaq listings extraction
        s3_client.update_metadata(bucket=os.getenv('AWS_S3_TST_BUCKET'), metadata_object='metadata.json', metadata_key='nasdaq_listed_tickers')

        # Assert object exists in S3
        response = s3_client.s3.head_object(Bucket=os.getenv('AWS_S3_TST_BUCKET'), Key=filename)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Assert content matches original CSV content
        s3_object = s3_client.get_object(bucket=os.getenv('AWS_S3_TST_BUCKET'), key=filename)
        s3_csv_data = s3_object['Body'].read()
        assert nasdaq_listings_csv_data == s3_csv_data

    def test_stock_aggregates_stream_producer(self):
        """
        Test stock stream producer loading data into Snowflake test table
        """
        # Avro schema definition
        avro_schema_str = """
        {
        "type": "record",
        "name": "StockAggregatesStream",
        "namespace": "polygon",
        "fields": [
            {"name": "ticker_symbol", "type": "string"},
            {"name": "event_timestamp", "type": "long"},
            {"name": "volume", "type": "long"},
            {"name": "accumulated_volume", "type": "long"},
            {"name": "volume_weighted_average_price", "type": "double"},
            {"name": "closing_price", "type": "double"},
            {"name": "average_trade_size", "type": "double"}
        ]
        }
        """
        # Setup Schema Registry
        schema_registry_url = {"url": os.getenv("SCHEMA_REGISTRY_URL_TST")}
        schema_registry_client = SchemaRegistryClient(schema_registry_url)

        # Create avro serializer
        avro_serializer = AvroSerializer(
            schema_registry_client,
            avro_schema_str
        )

        # Kafka producer setup
        producer_config = {
            'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS_TST"),
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': avro_serializer
        }

        producer = SerializingProducer(producer_config)

        # Create test message
        msg = {
        "ticker_symbol": "TEST",
        "event_timestamp": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
        "volume": 123,
        "accumulated_volume": 456,
        "volume_weighted_average_price": 100.5,
        "closing_price": 101.2,
        "average_trade_size": 12.3
        }

        # Push messages to topic
        producer.produce(topic=os.getenv("KAFKA_TOPIC_TST"), key="TEST", value=msg)
        producer.flush()

        # Sleep to allow for data processing
        time.sleep(60)

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

        with snowflake_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM INVESTMENT_ANALYTICS.TST.STOCK_AGGREGATES_TST")
            result = cursor.fetchone()
            assert result[0] == 1
            cursor.execute("TRUNCATE TABLE INVESTMENT_ANALYTICS.TST.STOCK_AGGREGATES_TST")













        






    

    




    








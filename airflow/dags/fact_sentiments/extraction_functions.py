# Import modules
from datetime import datetime, timedelta
import os
import logging
import pandas as pd
from dotenv import load_dotenv
from dags.api_extraction.polygon_api import PolygonApi
from dags.api_extraction.news_api import NewsApi
from dags.api_extraction.reddit_api import RedditApi
from dags.utils.snowflake_utils import Snowflake
from dags.utils.s3_utils import S3

def extract_company_news() -> None:
    '''Extracts company articles data from Polygon API and loads it into Snowflake.'''
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

    # Retrieve yesterday's date in string format
    yesterday = datetime.today() - timedelta(days=1)
    formatted_date = yesterday.strftime('%Y-%m-%d')

    # Create company news data list
    company_news = []

    # Fetch news for each ticker
    for ticker in tickers:
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
    snowflake_client.load_to_snowflake(connection=snowflake_conn, data=company_news, target_table='RAW_COMPANY_NEWS')

def extract_non_company_news(parameters: dict, timeout: int) -> None:
    '''
    Extracts non company news articles data from News API and loads it into Snowflake.

    Args:
        parameters (dict): Parameters to be passed into News API Client
        timeout (int): Timeout for request to News API Client
    '''

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
        news_response = news_api_client.fetch_with_retry(params=param, timeout=10)
        # Append news items to the list
        for item in news_response["results"]:
            non_company_news.append({
                "date": item.get("pubDate"),
                "title": item.get("title"),
                "description": item.get("description"),
                "source": item.get("source_name")
            })
    
    # Load data into Snowflake test table
    snowflake_client.load_to_snowflake(connection=snowflake_conn, data=non_company_news, target_table='RAW_NON_COMPANY_NEWS')

def extract_reddit_submissions(subreddit_name: str, submissions_limit: int) -> None:
    ''' 
    Extract data from reddit submissions 

    Args:
        subreddit_name: Name of subreddit to retrieve submissions from
        submissions_limit: Limit on number of submissions to retrieve from Reddit API Client
    '''
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

    # Fetch submissions from subreddit
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
    snowflake_client.load_to_snowflake(connection=snowflake_conn, data=submissions_data, target_table='RAW_REDDIT_SUBMISSIONS')


        






        











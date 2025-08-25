# Import modules
from datetime import datetime, timedelta
import os
import json
import logging
import concurrent.futures
import time
import pandas as pd
import praw
from snowflake.connector.pandas_tools import write_pandas
from polygon import RESTClient
from newsdataapi import NewsDataApiClient
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed, before_log, after_log, retry_if_exception_type, RetryError
from utils.utils import create_snowflake_connection, s3_get_object, s3_put_object
from sentiments_fact_table.news_api_extraction_helpers import api_call, call_news_api_with_timeout

def extract_company_news() -> None:
    '''Extracts company articles data from Polygon API and loads it into Snowflake.'''
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

    # Retrieve current ticker symbols from dim_company dimension table in Snowflake
    with snowflake_conn.cursor() as cursor:
        cursor.execute("""
                       select 
                       distinct 
                       ticker_symbol
                       from investment_analytics.core.dim_company
                       where is_current = TRUE """)
        tickers = [row[0] for row in cursor.fetchall()]

    # Initialise Polygon API client
    polygon_api_key = os.getenv("POLYGON_API_KEY")
    polygon_client = RESTClient(polygon_api_key)

    # Create a list to hold company news data
    company_news_data = []

    # Retrieve yesterday's date in string format
    yesterday = datetime.today() - timedelta(days=1)
    formatted_date = yesterday.strftime('%Y-%m-%d')

    # Fetch news for each ticker
    for ticker_symbol in tickers:
        try:
            # Fetch news for the ticker
            news = polygon_client.list_ticker_news(
            ticker=ticker_symbol,
            published_utc_gte=formatted_date,
            order="asc",
            limit=1000,
            sort="published_utc")
        
            for item in news:
                company_news_data.append({
                    "date": formatted_date,
                    "ticker_symbol": ticker_symbol,
                    "title": item.title,
                    "description": item.description,
                    "source": item.publisher.name,
                })
        except Exception as e:
            print(f"Error fetching news for {ticker_symbol}: {e}")
    
    # Load company_news data into Snowflake
    company_news_data_df = pd.DataFrame(company_news_data)
    company_news_data_df.columns = map(str.upper, company_news_data_df.columns)  # Convert column names to uppercase
    success, nchunks, nrows, _ = write_pandas(snowflake_conn, company_news_data_df, table_name="RAW_COMPANY_NEWS", database=os.getenv("SNOWFLAKE_DATABASE"), schema=os.getenv("SNOWFLAKE_RAW_SCHEMA"))
    print(f"Loaded {nrows} rows. Success: {success}")

    # Update metadata with current run date
    ''' metadata['company_news'] = formatted_date
    s3_put_object(bucket=os.getenv('AWS_S3_BUCKET'), key='metadata.json', data=json.dumps(metadata).encode('utf-8')) '''

def extract_non_company_news(parameters: dict, timeout: int) -> None:
    '''
    Extracts non company news articles data from News API and loads it into Snowflake.

    Args:
        parameters (dict): Parameters to be passed into News API Client
        timeout (int): Timeout for request to News API Client
    
    '''
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

    # Initialise News API client
    news_api_key = os.getenv("NEWS_API_KEY")
    news_api_client = NewsDataApiClient(news_api_key)

    # Create a list object to store non_company_news_data
    non_company_news_data = []

    # Setup logging (API request is known to have failures)
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # Retry decorator to retry API call if failure
    @retry(
    retry=retry_if_exception_type((TimeoutError, ValueError)),
    wait=wait_fixed(10),
    stop=stop_after_attempt(3),
    before=before_log(logger, logging.INFO),
    after=after_log(logger, logging.INFO)
    )

    def fetch_with_retry(params: dict, news_api_client: NewsDataApiClient) -> json:
        logger.info(f"Attempting API call with params: {params}")

        def api_call():
            return news_api_client.news_api(
                category=params["category"],
                country=params["country"],
                qInMeta=params["qInMeta"],
                language="en",
                size=10,
                removeduplicate=True
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(api_call)
            try:
                response = future.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                raise TimeoutError(f"API call timed out after {timeout}s for params: {params}")

        if not response or "results" not in response or len(response["results"]) == 0:
            raise ValueError("Empty or invalid response received from NewsData API.")

        return response

    for params in parameters:
        try:
            response = fetch_with_retry(params=params, news_api_client=news_api_client)
            for item in response["results"]:
                non_company_news_data.append({
                    "date": item.get("pubDate"),
                    "title": item.get("title"),
                    "description": item.get("description"),
                    "source": item.get("source_name")
                })
        except RetryError as re:
            logger.error(f"[MAX RETRIES] Failed for params: {params} | Reason: {re.last_attempt.exception()}")
        except Exception as e:
            logger.error(f"Unexpected error for {params}: {e}")
    
    # Load company_news data into Snowflake
    non_company_news_data_df = pd.DataFrame(non_company_news_data)
    non_company_news_data_df.columns = map(str.upper, non_company_news_data_df.columns)  # Convert column names to uppercase
    success, nchunks, nrows, _ = write_pandas(snowflake_conn, non_company_news_data_df, table_name="RAW_NON_COMPANY_NEWS", database=os.getenv("SNOWFLAKE_DATABASE"), schema=os.getenv("SNOWFLAKE_RAW_SCHEMA"))
    print(f"Loaded {nrows} rows. Success: {success}")

    # Update metadata with current run date
    ''' metadata['non_company_news'] = formatted_date
    s3_put_object(bucket=os.getenv('AWS_S3_BUCKET'), key='metadata.json', data=json.dumps(metadata).encode('utf-8')) '''

def extract_reddit_submissions(subreddit_name: str, submissions_limit: int) -> None:
    ''' 
    Extract data from reddit submissions 

    Args:
        subreddit_name: Name of subreddit to retrieve submissions from
        submissions_limit: Limit on number of submissions to retrieve from Reddit API Client
    '''
    # Load environment variables
    load_dotenv()

    # Create Snowflake Connection
    snowflake_conn = create_snowflake_connection(
        user=os.getenv("SNOWFLAKE_USER"),
        private_key_encoded=os.getenv("SNOWFLAKE_PRIVATE_KEY_B64"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse='INVESTMENT_ANALYTICS_DWH',
        database='INVESTMENT_ANALYTICS',
        schema='RAW'
    )

    # Instantiate Reddit API Client
    reddit_client = praw.Reddit(
    client_id= os.getenv("REDDIT_CLIENT_ID"),
    client_secret= os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent= os.getenv("REDDIT_USER_AGENT"),
    username= os.getenv("REDDIT_USERNAME"),
    password= os.getenv("REDDIT_PASSWORD")
    )

    # Fetch the subreddit
    subreddit = reddit_client.subreddit(subreddit_name)

    # Fetch the most recent submissions from the subreddit
    submissions = subreddit.new(limit=submissions_limit)

    # Get the current epoch time
    current_time = time.time()
    one_day_seconds = 846400

    # Store the submissions data in list, only including submissions from the past day
    submissions_filtered = [
        submission for submission in submissions
        if current_time - submission.created_utc <= one_day_seconds
    ]

    # List to store relevant submission data for snowflake ingestion
    submissions_data = []

    # Iterate through each submission and collect data
    for submission in submissions_filtered:
        submission_title = submission.title
        submission_utc = submission.created_utc
        submission_body = submission.selftext
        
        submissions_data.append({
            "date": submission_utc,
            "title": submission_title,
            "description": submission_body,
            "source": 'Reddit'
        })
    
    # Load submissions data into Snowflake
    reddit_submissions_data_df = pd.DataFrame(submissions_data)
    reddit_submissions_data_df.columns = map(str.upper, reddit_submissions_data_df.columns)  # Convert column names to uppercase
    success, nchunks, nrows, _ = write_pandas(snowflake_conn, reddit_submissions_data_df, table_name="RAW_REDDIT_SUBMISSIONS", database=os.getenv("SNOWFLAKE_DATABASE"), schema=os.getenv("SNOWFLAKE_RAW_SCHEMA"))
    print(f"Loaded {nrows} rows. Success: {success}")

    # Update metadata with current run date
    ''' metadata['reddit_submissions'] = formatted_date
    s3_put_object(bucket=os.getenv('AWS_S3_BUCKET'), key='metadata.json', data=json.dumps(metadata).encode('utf-8')) '''

        






        











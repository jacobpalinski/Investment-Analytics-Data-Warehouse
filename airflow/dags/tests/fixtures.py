# Import modules
import pytest
import time
import concurrent.futures
from dags.utils.snowflake_utils import Snowflake
from dags.api_extraction.finnhub_api import FinnhubApi
from dags.api_extraction.sec_api import SecApi

@pytest.fixture
def mock_snowflake_client():
    """ Fixture to create a snowflake client instance for testing """
    return Snowflake(user="test_user", account="test_account", private_key_encoded="test_private_key")

@pytest.fixture
def mock_finnhub_client(monkeypatch):
    """ Fixture to mock the Finnhub Client for FinnhubApi initialization. """
    class MockClient:
        def __init__(self, api_key):
            self.api_key = api_key
            self.initialized = True
        
        def company_profile2(self, symbol):
            return {"name": "Apple", "finnhubIndustry": "Technology"}

    # Replace Client in your module with the mock
    monkeypatch.setattr("dags.api_extraction.finnhub_api.Client", MockClient)
    return MockClient

@pytest.fixture
def mock_fred_client(monkeypatch):
    """ Fixture to mock the Fred API Client for initialization. """
    class MockClient:
        def __init__(self, api_key):
            self.api_key = api_key
            self.initialized = True
        
        def get_series(self, series_id, sort_order, limit):
            self.called_with = (series_id, sort_order, limit)
            return {"2025-10-19": 3.5}

    # Replace Client in your module with the mock
    monkeypatch.setattr("dags.api_extraction.fred_api.Fred", MockClient)
    return MockClient

@pytest.fixture
def mock_news_api_client():
    class MockClient:
        def __init__(self):
            self.response = {"results": [{"headline": "mock headline"}]}
            self.raise_timeout = False
            self.raise_empty = False
            self.call_count = 0
        
        def configure(self, raise_timeout=False, raise_empty=False):
            self.raise_timeout = raise_timeout
            self.raise_empty = raise_empty
        
        def news_api(self, **kwargs):
            self.call_count += 1
            if self.raise_timeout:
                raise concurrent.futures.TimeoutError("Simulated timeout")
            if self.raise_empty:
                return {"results": []}
            return self.response
    
    return MockClient()

@pytest.fixture
def mock_polygon_client(monkeypatch):
    """ Fixture to mock the Polygon Client for PolygonApi initialization. """
    class MockResponse:
        def __init__(self, cik):
            self.cik = cik

    class MockClient:
        def __init__(self, api_key):
            self.api_key = api_key
            self.initialized = True
        
        def get_ticker_details(self, ticker):
            return MockResponse("0000320193")
        
        def list_ticker_news(self, ticker, published_utc_gte, order, limit, sort):
            self.called_with = (ticker, published_utc_gte, order, limit, sort)
            return {"date": "mock date", "ticker_symbol": "mock ticker symbol", "title": "mock title", "description": "mock description", "source": "mock source"}

    # Replace Polygon Client with mock
    monkeypatch.setattr("dags.api_extraction.polygon_api.RESTClient", MockClient)
    return MockClient

@pytest.fixture
def mock_reddit_env(monkeypatch):
    """
    Fixture that mocks Reddit API environment for testing RedditApi class.
    """
    current_time = 10000000
    monkeypatch.setattr(time, "time", lambda: current_time)

    # Define dummy submission class inline
    class DummySubmission:
        def __init__(self, created_utc):
            self.created_utc = created_utc

    # Create submissions — one recent, one old
    new_post = DummySubmission(created_utc=current_time - 100)      # within 1 day
    old_post = DummySubmission(created_utc=current_time - 1000000)  # older than 1 day

    # Mock subreddit and Reddit client
    class DummySubreddit:
        def new(self, limit):
            assert limit == 5
            return [new_post, old_post]

    class DummyRedditClient:
        def __init__(self, raise_error=False):
            self.raise_error = raise_error

        def subreddit(self, name):
            if self.raise_error:
                raise Exception("API failure")
            assert name == "investing"
            return DummySubreddit()

    # Return everything needed for the test
    return {
        "current_time": current_time,
        "new_post": new_post,
        "old_post": old_post,
        "DummyRedditClient": DummyRedditClient
    }

@pytest.fixture
def mock_sec_api():
    " Fixture that mocks SecApi class for testing"
    class MockResponse:
        status_code = 200

    sec_api = SecApi(user_agent="test_agent")

    mock_success_response = MockResponse()

    mock_response_data = {
        "Revenues": {
            "units": {
                "USD": [
                    {
                        "fy": 2024,
                        "fp": "Q4",
                        "filed": "2025-01-10",
                        "val": 500000,
                    }
                ]
            }
        }
    }

    return sec_api, mock_success_response, mock_response_data



            





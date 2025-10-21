# Import modules
import pytest
import concurrent.futures
from dags.utils.snowflake_utils import Snowflake
from dags.api_extraction.finnhub_api import FinnhubApi

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
        
        def company_profile2(self, ticker):
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
            





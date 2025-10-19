# Import modules
import pytest
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
            # Default behavior (can be overridden in tests)
            return {"name": "Apple", "finnhubIndustry": "Technology"}

    # Replace Client in your module with the mock
    monkeypatch.setattr("dags.api_extraction.finnhub_api.Client", MockClient)
    return MockClient

import pytest
from tenacity import RetryError
from dags.api_extraction.news_api import NewsApi
from dags.tests.fixtures import mock_news_api_client

class TestNewsApi:
    """ Test suite for NewsApi class """
    def test_fetch_with_retry_success(self, mock_news_api_client):
        """ Test successful data fetch from News API """
        # Set Mock Client to NewsApi Connection
        news_api = NewsApi("dummy_key")
        news_api.news_api_client = mock_news_api_client
        params = {"category": "business", "country": "us", "qInMeta": "economy AND interest rate"}
        
        # Run test
        result = news_api.fetch_with_retry(params, timeout=2)

        # Assert expected result
        assert "results" in result
        assert result["results"][0]["headline"] == "mock headline"
        assert mock_news_api_client.call_count == 1
    
    def test_fetch_with_retry_timeout(self, mock_news_api_client):
        """ Test timeout handling during data fetch from News API """
        # Set Mock Client to NewsApi Connection
        news_api = NewsApi("dummy_key")
        news_api.news_api_client = mock_news_api_client
        mock_news_api_client.configure(raise_timeout=True)
        params = {"category": "business", "country": "us", "qInMeta": "economy AND interest rate"}

        # Run test and assert RetryError is raised after retries
        with pytest.raises(RetryError):
            news_api.fetch_with_retry(params, timeout=0.1)
        
        # Retry should have been attempted 3 times
        assert mock_news_api_client.call_count == 3
    
    def test_fetch_with_retry_empty_response(self, mock_news_api_client):
        """ Test handling of empty response from News API """
        news_api = NewsApi("dummy_key")
        news_api.news_api_client = mock_news_api_client
        mock_news_api_client.configure(raise_empty=True)
        params = {"category": "business", "country": "us", "qInMeta": "economy AND interest rate"}

        # Run test and assert RetryError
        with pytest.raises(RetryError):
            news_api.fetch_with_retry(params, timeout=0.1)

        # Retry should have been attempted 3 times
        assert mock_news_api_client.call_count == 3
    

        

            








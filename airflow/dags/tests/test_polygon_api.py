# Import modules
import pytest
from dags.api_extraction.polygon_api import PolygonApi
from dags.tests.fixtures import mock_polygon_client

class TestPolygonApi:
    """ Test suite for PolygonApi class """
    def test_extract_company_cik_success(self, mock_polygon_client):
        """ Test successful CIK extraction from Polygon API """
        # Set Mock Client to Polygon API Connection
        polygon_api = PolygonApi("dummy_key")
        polygon_api.polygon_client = mock_polygon_client("dummy_key")
        
        # Run test
        result = polygon_api.extract_company_cik("AAPL")

        # Assert expected result
        assert result == {"cik": "0000320193"}
    
    def test_extract_company_cik_missing_field(self, mock_polygon_client):
        """Test CIK extraction when response has no 'cik' attribute."""
        # Define a mock response without a cik field
        class MockResponse:
            pass

        # Create a subclass of the mock client that returns this response
        class MockClient(mock_polygon_client):
            def get_ticker_details(self, ticker):
                return MockResponse()  # no cik attribute

        # Set Mock Client to Polygon API Connection
        polygon_api = PolygonApi("dummy_key")
        polygon_api.polygon_client = MockClient("dummy_key")

        # Run test
        result = polygon_api.extract_company_cik("AAPL")

        # Assert expected result
        assert result == {"cik": None}
    
    def test_extract_company_cik_exception(self, monkeypatch, mock_polygon_client, caplog):
        """ Test exception handling during CIK extraction from Polygon API """
        # Modify mock to raise exception
        class MockClient(mock_polygon_client):    
            def get_ticker_details(self, ticker):
                raise RuntimeError("API failure")
        
        # Set Mock Client to Polygon API Connection
        monkeypatch.setattr("dags.api_extraction.polygon_api.RESTClient", MockClient)
        polygon_api = PolygonApi("dummy_key")

        # Create error message
        with caplog.at_level("ERROR"):
            result = polygon_api.extract_company_cik("AAPL")

        # Assert expected result
        assert result == {}
        assert "Polygon API error for AAPL" in caplog.text
    
    def test_extract_company_news_success(self, mock_polygon_client):
        """ Test successful company news extraction from Polygon API """
        # Set Mock Client to Polygon API Connection
        polygon_api = PolygonApi("dummy_key")
        polygon_api.polygon_client = mock_polygon_client("dummy_key")

        # Run test
        result = polygon_api.extract_company_news("AAPL")

        # Assert expected result
        assert result == {"date": "mock date", "ticker_symbol": "mock ticker symbol", "title": "mock title", "description": "mock description", "source": "mock source"}
    
    def test_extract_company_news_exception(self, monkeypatch, mock_polygon_client, caplog):
        """ Test exception handling for company news extraction from Polygon API """
        # Modify mock to raise exception
        class MockClient(mock_polygon_client):    
            def list_ticker_news(self, ticker, published_utc_gte, order, limit, sort):
                raise RuntimeError("API failure")
        
        # Set Mock Client to Polygon API Connection
        monkeypatch.setattr("dags.api_extraction.polygon_api.RESTClient", MockClient)
        polygon_api = PolygonApi("dummy_key")

        # Create error message
        with caplog.at_level("ERROR"):
            result = polygon_api.extract_company_news("AAPL")

        # Assert expected result
        assert result == {}
        assert "Failed to return news for AAPL from Polygon API" in caplog.text


    


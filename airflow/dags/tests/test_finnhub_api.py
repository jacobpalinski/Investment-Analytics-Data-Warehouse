# Import modules
import pytest
from dags.api_extraction.finnhub_api import FinnhubApi
from dags.tests.fixtures import mock_finnhub_client

class TestFinnhubApi:
    """ Test suite for FinnhubApi class """
    def test_extract_company_profile_success(self, mock_finnhub_client):
        """ Ensure extract_company_profile returns expected values. """
        # Initialise mocked API
        finnhub_api = FinnhubApi("dummy_key")

        # Mock call to extract company profile for a specific ticker
        result = finnhub_api.extract_company_profile("AAPL")

        # Assert expected result
        assert result == {"company_name": "Apple", "industry": "Technology"}
        
    def test_extract_company_profile_missing_fields(self, monkeypatch, mock_finnhub_client):
        """ Test when API response is missing expected fields. """
        # Mock empty API response
        class MockClient(mock_finnhub_client):
            def company_profile2(self, symbol):
                return {}  # simulate missing fields

        # Initialise mocked API
        monkeypatch.setattr("dags.api_extraction.finnhub_api.Client", MockClient)
        finnhub_api = FinnhubApi("dummy_key")
        result = finnhub_api.extract_company_profile("AAPL")

        # Assert empty API response
        assert result == {"company_name": "", "industry": ""}

    def test_extract_company_profile_exception(self, monkeypatch, mock_finnhub_client, caplog):
        """Test when the client raises an exception."""
        # Mock exception
        class MockClient(mock_finnhub_client):
            def company_profile2(self, symbol):
                raise RuntimeError("API failure")

        # Initialise mocked API
        monkeypatch.setattr("dags.api_extraction.finnhub_api.Client", MockClient)
        finnhub_api = FinnhubApi("dummy_key")

        with caplog.at_level("ERROR"):
            result = finnhub_api.extract_company_profile("AAPL")

        # Assert empty result and exception has been raised
        assert result == {}
        assert "Finnhub API error for AAPL" in caplog.text


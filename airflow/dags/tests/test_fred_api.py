# Import modules
import pytest
from dags.api_extraction.fred_api import FredApi
from dags.tests.fixtures import mock_fred_client

class TestFredApi:
    """ Test suite for FredApi class """
    def test_extract_fred_data_success(self, mock_fred_client):
        """ Test successful data extraction from FRED """
        fred_api = FredApi("dummy_key")
        
        # Run test
        result = fred_api.extract_fred_data("GDP")

        # Assert expected result
        assert result == {"2025-10-19": 3.5}
    
    def test_extract_fred_data_exception(self, monkeypatch, mock_fred_client, caplog):
        """ Test exception handling during data extraction from FRED """
        # Modify mock to raise exception
        class MockClient(mock_fred_client):    
            def get_series(self, series_id):
                raise RuntimeError("API failure")
        
        # Set Mock Client to Fred API Connection
        monkeypatch.setattr("dags.api_extraction.fred_api.Fred", MockClient)
        fred_api = FredApi("dummy_key")

        # Create error message
        with caplog.at_level("ERROR"):
            result = fred_api.extract_fred_data("GDP")

        # Assert expected result
        assert result == {}
        assert "FRED API error for GDP" in caplog.text


            


    



# Import modules
import pytest
import requests
from dags.api_extraction.sec_api import SecApi
from dags.tests.fixtures import sec_api, mock_sec_success_response, mock_sec_full_response

class TestSecApi:
    """ Test suite for SecApi class """
    def test_sec_data_request_success(self, sec_api, mock_sec_success_response, monkeypatch):
        """SEC API returns a valid response"""

        def mock_get(url, headers, timeout):
            return mock_sec_success_response

        monkeypatch.setattr(requests, "get", mock_get)

        response = sec_api.sec_data_request("0001112223")

        assert response.status_code == 200


    def test_sec_data_request_exception(self, sec_api, monkeypatch):
        """SEC API timeout returns empty dict"""

        def mock_get(url, headers, timeout):
            raise requests.exceptions.Timeout("test timeout")

        monkeypatch.setattr(requests, "get", mock_get)

        response = sec_api.sec_data_request("0001112223")

        assert response == {}


    def test_extract_financial_data_success(self, sec_api, mock_sec_full_response):
        """Correct values extracted from SEC data response"""

        result = sec_api.extract_financial_data(
            "0001112223",
            mock_sec_full_response
        )

        assert result == [
            {
                "cik": "0001112223",
                "fiscal_year": 2024,
                "fiscal_quarter": "Q4",
                "filing_date": "2025-01-10",
                "financial_statement": "income_statement",
                "item": "revenues",
                "currency": "USD",
                "value": 500000,
            }
        ]


    def test_extract_financial_data_no_matching_key(self, sec_api):
        """Missing GAAP data returns empty list"""

        response = {
            "facts": {
                "us-gaap": {}
            }
        }

        result = sec_api.extract_financial_data("0001112223", response)

        assert result == []


    def test_extract_financial_data_missing_units(self, sec_api):
        """Missing units field returns empty list"""

        response = {
            "facts": {
                "us-gaap": {
                    "Revenues": {}
                }
            }
        }

        result = sec_api.extract_financial_data("0001112223", response)

        assert result == []
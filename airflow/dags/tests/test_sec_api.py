# Import modules
import pytest
import requests
from dags.api_extraction.sec_api import SecApi
from dags.tests.fixtures import mock_sec_api

class TestSecApi:
    """ Test suite for SecApi class """
    def test_sec_data_request_success(self, mock_sec_api, monkeypatch):
        """ Test SEC API returns a valid response """
        # Mock SEC API and successful response
        sec_api, mock_success_response, _ = mock_sec_api

        def mock_get(url, headers, timeout):
            return mock_success_response

        monkeypatch.setattr(requests, "get", mock_get)

        response = sec_api.sec_data_request("0001112223")

        # Assert successful response
        assert response.status_code == 200


    def test_sec_data_request_exception(self, mock_sec_api, monkeypatch):
        """ Test SEC API times out returns empty dict """
        # Mock SEC API and timed out response
        sec_api, _, _ = mock_sec_api

        def mock_get(url, headers, timeout):
            raise requests.exceptions.Timeout("test timeout")

        monkeypatch.setattr(requests, "get", mock_get)

        response = sec_api.sec_data_request("0001112223")

        # Assert empty result
        assert response == {}


    def test_extract_financial_data_success(self, mock_sec_api):
        """ Test correct values extracted from SEC data response """
        # Mock SEC API and successful response
        sec_api, _, mock_response_data = mock_sec_api

        result = sec_api.extract_financial_data("0001112223", mock_response_data)

        # Assert expected response
        assert result == [
            {"cik": "0001112223",
            "fiscal_year": 2024,
            "fiscal_quarter": "Q4",
            "filing_date": "2025-01-10",
            "financial_statement": "income_statement",
            "item": "revenues",
            "currency": "USD",
            "value": 500000}
        ]

    def test_extract_financial_data_no_matching_key(self, mock_sec_api):
        """ Test missing filter key returns None """
        # Mock SEC API and no matching key response
        sec_api, _, _ = mock_sec_api

        # Assert expected result
        result = sec_api.extract_financial_data("0001112223", {})
        assert result == []


    def test_extract_financial_data_missing_units(self, mock_sec_api):
        """ Missing units field returns None """
        # Mock SEC API and missing units response
        sec_api, _, _ = mock_sec_api

        response = {"Revenues": {}}
        result = sec_api.extract_financial_data("0001112223", response)

        # Assert expected result
        assert result == []
# Import modules
import pytest
import requests
from datetime import datetime, timezone
from freezegun import freeze_time, configure
from dags.api_extraction.sec_api import SecApi
from dags.tests.fixtures import sec_api, mock_sec_success_response, mock_sec_full_response

# Extend the ignore list for freezegun to include transformers to avoid issues with time freezing in tests
configure(extend_ignore_list=["transformers"])

class TestSecApi:
    """ Test suite for SecApi class """
    def test_sec_data_request_success(self, sec_api, mock_sec_success_response, monkeypatch):
        """ Tests that SEC API returns a valid response for sec_data_request method """
        def mock_get(url, headers, timeout):
            return mock_sec_success_response

        monkeypatch.setattr(requests, "get", mock_get)

        response = sec_api.sec_data_request("0001112223")

        assert response.status_code == 200


    def test_sec_data_request_exception(self, sec_api, monkeypatch):
        """ Test that SEC API timeout returns empty dict for an exception generated from sec_data_request_method """

        def mock_get(url, headers, timeout):
            raise requests.exceptions.Timeout("test timeout")

        monkeypatch.setattr(requests, "get", mock_get)

        response = sec_api.sec_data_request("0001112223")

        assert response == {}

    @freeze_time("2026-05-14 11:45:30.123456+00:00")
    def test_extract_financial_data_success(self, sec_api, mock_sec_full_response):
        """ Tests correct values extracted from SEC data response for extract_financial_data method """

        result = sec_api.extract_financial_data(
            "0001112223",
            mock_sec_full_response
        )

        assert result == [
            {   
                "extraction_timestamp": datetime(2026, 5, 14, 11, 45, 30, 123456, tzinfo=timezone.utc),
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
        """ Tests missing GAAP data returns empty list for extract_financial_data method """

        response = {
            "facts": {
                "us-gaap": {}
            }
        }

        result = sec_api.extract_financial_data("0001112223", response)

        assert result == []


    def test_extract_financial_data_missing_units(self, sec_api):
        """ Tests missing units field returns empty list for extract_financial_data method """

        response = {
            "facts": {
                "us-gaap": {
                    "Revenues": {}
                }
            }
        }

        result = sec_api.extract_financial_data("0001112223", response)

        assert result == []
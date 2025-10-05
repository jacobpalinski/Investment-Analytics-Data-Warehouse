# Import modules
import json
import logging
from finnhub import Client

# Create setup for logging
logger = logging.getLogger(__name__)

class FinnhubApi:
    """
    Class for interacting with Finnhub API
    
    Attributes:
        finnhub_client (Client): API client for Finnhub connection

    Methods:
        extract_company_profile(ticker):
            Extracts company_name and industry for a given ticker symbol
    """
    def __init__(self, finnhub_api_key: str):
        self.finnhub_client = Client(finnhub_api_key)
    
    def extract_company_profile(self, ticker: str) -> dict:
        """
        Extracts company_name and industry for a given ticker symbol

        Args:
            ticker (str): Ticker data is being extracted for
        
        Returns:
            dict: Dictionary with company name and industry
        """
        try:
            response = self.finnhub_client.company_profile2(ticker)
            return {
                "company_name": response.get("name", ""),
                "industry": response.get("finnhubIndustry", "")
            }
        except Exception as e:
            logger.exception(f"Finnhub API error for {ticker}: {e}")
            return {}
# Import modules
import json
import logging
from datetime import datetime, timedelta
from polygon import RESTClient

# Create setup for logging
logger = logging.getLogger(__name__)

class PolygonApi:
    """
    Class for interacting with Polygon API
    
    Attributes:
        polygon_client (RESTClient): API client for Polygon connection

    Methods:
        extract_company_cik(ticker):
            Extracts cik for a given ticker symbol
        extract_company_news(connection):
            Extracts company articles data from Polygon API
    """
    def __init__(self, polygon_api_key: str):
        self.polygon_client = RESTClient(polygon_api_key)
    
    def extract_company_cik(self, ticker: str) -> dict:
        """
        Extracts cik for a given ticker symbol

        Args:
            ticker (str): Ticker data is being extracted for
        
        Returns:
            dict: Dictionary with cik
        """
        try:
            response = self.polygon_client.get_ticker_details(ticker)
            return {
                "cik": getattr(response, "cik", None)
            }
        except Exception as e:
            logger.exception(f"Polygon API error for {ticker}: {e}")
            return {}
    
    def extract_company_news(self, ticker: str) -> dict:
        """
        Extracts company articles data from Polygon API

        Args:
            ticker (str): Ticker data is being extracted for
        
        Returns:
            dict: Dictionary with company news data
        """
        # Retrieve yesterday's date in string format
        yesterday = datetime.today() - timedelta(days=1)
        formatted_date = yesterday.strftime('%Y-%m-%d')

        try:
            response = self.polygon_client.list_ticker_news(
            ticker=ticker,
            published_utc_gte=formatted_date,
            order="asc",
            limit=1000,
            sort="published_utc")
            return response
        except Exception as e:
            logger.exception(f"Failed to return news for {ticker} from Polygon API: {e}")
            return {}








        

    

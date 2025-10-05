# Import modules
import json
import logging
from datetime import datetime, timedelta
from fredapi import Fred

# Create setup for logging
logger = logging.getLogger(__name__)

class FredApi:
    """
    Class for interacting with Fred API
    
    Attributes:
        fred_client (Fred): API client for Fred connection

    Methods:
        extract_fred_data(series_id):
            Extract data from Fred economic indicators 
    """
    def __init__(self, fred_api_key: str):
        self.fred_client = Fred(fred_api_key)
    
    def extract_fred_data(self, series_id: str) -> dict:
        """
        Extracts economic indicators from FRED API

        Args:
            series (dict): Dictionary with economic indicators and their corresponding series IDs
        
        Returns:
            dict: Dictionary with economic indicators data
        """
        try:
            response = self.fred_client.get_series(series_id, sort_order='desc', limit=1)
            return response
        except Exception as e:
            logger.exception(f"FRED API error for {series_id}: {e}")
            return {}



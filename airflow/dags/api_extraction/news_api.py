# Import modules
import json
import logging
import concurrent.futures
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type, before_log, after_log
from newsdataapi import NewsDataApiClient

# Create setup for logging
logger = logging.getLogger(__name__)

class NewsApi:
    """
    Class for interacting with News API
    
    Attributes:
        news_api_client (NewsDataApiClient): API client for News connection

    Methods:
        extract_company_profile(params, timeout):
            Fetches non company news data from NewsData API with retry logic for handling timeouts and empty responses
    """
    def __init__(self, news_api_key: str):
        self.news_api_client = NewsDataApiClient(news_api_key)
    
    @retry(
    retry=retry_if_exception_type((TimeoutError, ValueError)),
    wait=wait_fixed(10),
    stop=stop_after_attempt(3),
    before=before_log(logger, logging.INFO),
    after=after_log(logger, logging.INFO),
    )
    def fetch_with_retry(self, params: dict, timeout: int) -> dict:
        """
        Fetches non company news data from NewsData API with retry logic for handling timeouts and empty responses

        Args:
            params (dict): Parameters for the API calls
            timeout (int): Timeout duration in seconds for the API call
        
        Returns:
            dict: The API response data
        """
        logger.info(f"Attempting API call with params: {params}")

        def api_call():
            return self.news_api_client.news_api(
                category=params["category"],
                country=params["country"],
                qInMeta=params["qInMeta"],
                language="en",
                size=10,
                removeduplicate=True
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(api_call)
            try:
                response = future.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                raise TimeoutError(f"API call timed out after {timeout}s for params: {params}")

        if not response or "results" not in response or len(response["results"]) == 0:
            raise ValueError("Empty or invalid response received from NewsData API.")

        return response

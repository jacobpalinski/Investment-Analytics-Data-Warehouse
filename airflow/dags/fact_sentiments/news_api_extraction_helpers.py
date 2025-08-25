import concurrent.futures
import json
import logging
from newsdataapi import NewsDataApiClient
from tenacity import retry, stop_after_attempt, wait_fixed, before_log, after_log, retry_if_exception_type, RetryError
from dotenv import load_dotenv

def api_call(news_api_client: NewsDataApiClient, parameters: dict) -> json:
    """ Makes call to News API for a given set of parameters """
    return news_api_client.news_api(
            category=parameters["category"],
            country=parameters["country"],
            qInMeta=parameters["qInMeta"],
            language="en",
            size=10,
            removeduplicate=True
        )

def call_news_api_with_timeout(parameters: dict, news_api_client: NewsDataApiClient, timeout=10):
    """ Makes call to News API for a given timeout specified """
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(api_call, news_api_client, parameters)
        try:
            response = future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"API call timed out after {timeout}s for params: {parameters}")
    
    if not response or "results" not in response or len(response["results"]) == 0:
        raise ValueError("Empty or invalid response received from NewsData API.")

    return response
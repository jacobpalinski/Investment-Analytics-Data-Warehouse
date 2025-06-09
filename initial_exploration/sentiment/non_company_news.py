# Import modules
import os
import pandas as pd
from newsdataapi import NewsDataApiClient
from dotenv import load_dotenv
from tenacity import retry, wait_fixed, stop_after_attempt, RetryError, before_log, after_log, retry_if_exception_type
import concurrent.futures
import logging

# ------------------- Setup Logging -------------------
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ------------------- Load API Key -------------------
load_dotenv()
news_api_key = os.getenv("NEWS_API_KEY")
client = NewsDataApiClient(news_api_key)

# ------------------- Parameter Combos -------------------
parameter_combinations = [
    {"category": "business", "country": "us", "qInMeta": "economy AND interest rate"},
    {"category": "business", "country": "us", "qInMeta": "economy AND inflation"},
    {"category": "business", "country": "us", "qInMeta": "economy AND liquidity"},
    {"category": "business", "country": "us", "qInMeta": "economy AND Federal Reserve"},
    {"category": "business", "country": "us", "qInMeta": "economy AND consumer confidence"},
    {"category": "business", "country": "us", "qInMeta": "economy and unemployment"},
    {"category": "business", "country": "us", "qInMeta": "economy AND GDP"},
    {"category": "business", "country": "us", "qInMeta": "economy AND tariffs"},
    {"category": "business", "country": "us", "qInMeta": "economy AND treasury yields"},
    {"category": "business", "country": "us", "qInMeta": "economy AND trade balance"},
    {"category": "business", "country": "us", "qInMeta": "economy AND retail sales"},
    {"category": "business", "country": "us", "qInMeta": "economy AND CPI"},
    {"category": "business", "country": "us", "qInMeta": "economy AND bond spreads"},
    {"category": "politics", "country": "us", "qInMeta": "Trump AND Canada"},
    {"category": "politics", "country": "us", "qInMeta": "Trump AND China"},
    {"category": "politics", "country": "us", "qInMeta": "Trump AND Mexico"},
    {"category": "politics", "country": "us", "qInMeta": "Trump AND EU"},
    {"category": "politics", "country": "us", "qInMeta": "Democrats"},
    {"category": "politics", "country": "us", "qInMeta": "Republicans"}
]

# ------------------- Safe API Call with Timeout -------------------
def call_news_api_with_timeout(params, timeout=10):
    def api_call():
        return client.news_api(
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
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"API call timed out after {timeout}s for params: {params}")

# ------------------- Retry Wrapper -------------------
@retry(
    retry=retry_if_exception_type((TimeoutError, ValueError)),
    wait=wait_fixed(10),
    stop=stop_after_attempt(3),
    before=before_log(logger, logging.INFO),
    after=after_log(logger, logging.INFO)
)
def fetch_news(params):
    logger.info(f"Attempting API call with params: {params}")
    response = call_news_api_with_timeout(params, timeout=10)

    if not response or "results" not in response or len(response["results"]) == 0:
        raise ValueError("Empty or invalid response received from NewsData API.")

    return response

# ------------------- Fetch News Loop -------------------
news_data = []
failed_params = []

for params in parameter_combinations:
    print(f"Fetching: {params}")
    try:
        response = fetch_news(params)
        for item in response["results"]:
            news_data.append({
                "date": item.get("pubDate"),
                "date_tz": item.get("pubDateTZ"),
                "title": item.get("title"),
                "description": item.get("description"),
                "source": item.get("source_name")
            })

    except RetryError as re:
        logger.error(f"[MAX RETRIES] Failed for params: {params} | Reason: {re.last_attempt.exception()}")
        failed_params.append((params, str(re.last_attempt.exception())))
    except Exception as e:
        logger.error(f"Unexpected error for {params}: {e}")
        failed_params.append((params, str(e)))

# ------------------- Save Results -------------------
df = pd.DataFrame(news_data)
df.to_json("non_company_news.json", orient="records", indent=4)

if failed_params:
    pd.DataFrame(failed_params, columns=["params", "error"]).to_csv("failed_news_fetches.csv", index=False)

print("✅ News data saved. Errors (if any) logged.")

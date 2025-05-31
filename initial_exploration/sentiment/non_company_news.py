# Import modules
import os
import pandas as pd
from newsdataapi import NewsDataApiClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Access API key from environment variables
news_api_key = os.getenv("NEWS_API_KEY")

# Connect to NewsData API
client = NewsDataApiClient(news_api_key)

# Store news data in a list
news_data = []

# Combinations of parameters to fetch news
parameter_combinations = [
    {"category": "business", "country": "us", "qInMeta": "economy AND interest rate"},
    {"category": "business", "country": "us", "qInMeta": "economy AND inflation"},
    {"category": "business", "country": "us", "qInMeta": "economy AND liquidity"},
    {"category": "business", "country": "us", "qInMeta": "economy AND Federal Reserve"},
    {"category": "business", "country": "us", "qInMeta": "economy AND consumer confidence"},
    {"category": "business", "country": "us", "qInMeta": "economy and undemployment"},
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

for params in parameter_combinations:
    try:
        # Fetch news with the specified parameters
        response = client.news_api(
            category=params["category"],
            country=params["country"],
            qInMeta=params["qInMeta"],
            timeframe="24",
            language="en",
            size=10,
            full_content="1",
            removeduplicate="1"
        )
        
        # Append each news item to the news_data list
        for item in response['results']:
            news_data.append({
                "date": item.pubDate,
                "title": item.title,
                "content": item.content,
                "source": item.source_name,
                "sentiment_scores": item.sentiment_scores
            })
    
    except Exception as e:
        print(f"Error fetching news for {params}: {e}")

# Convert news data list to DataFrame
df = pd.DataFrame(news_data)

# Save to JSON file
df.to_json("non_company_news.json", orient="records", indent=4)

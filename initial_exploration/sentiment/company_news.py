# Import modules
import os
import pandas as pd
from polygon import RESTClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Access API key from environment variables
polygon_api_key = os.getenv("POLYGON_API_KEY")

# Connect to Polygon API
client = RESTClient(polygon_api_key)

# Load ticker symbols from CSV
df = pd.read_csv("../stock_aggregates/nasdaq_listed_symbols_20250528.csv")
tickers = df['Symbol'].dropna().tolist()

# Store news data in a list
news_data = []

# Fetch news for each ticker
for ticker in tickers:
    print(ticker)
    try:
        # Fetch news for the ticker
        news = client.list_ticker_news(
            ticker=ticker,
            published_utc_gte='2025-03-09',
            order="asc",
            limit=1000,
            sort="published_utc")
        
        for item in news:
            news_data.append({
                "date": item.published_utc,
                "ticker": ticker,
                "title": item.title,
                "description": item.description,
                "source": item.publisher.name,
            })
    except Exception as e:
        print(f"Error fetching news for {ticker}: {e}")

# Convert news data list to DataFrame
df = pd.DataFrame(news_data)

# Save to JSON file
df.to_json("company_news.json", orient="records", indent=4)
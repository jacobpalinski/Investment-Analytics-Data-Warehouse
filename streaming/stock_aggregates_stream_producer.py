# Import necessary libraries
import os
import time
import json
import pandas as pd
from typing import List
from dotenv import load_dotenv
from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import snowflake.connector
import random
from datetime import datetime, timezone

# Load environment variables
'''load_dotenv()
polygon_api_key = os.getenv("POLYGON_API_KEY") '''

# Create snowflake connection
''' snowflake_conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    private_key_encoded=os.getenv("SNOWFLAKE_PRIVATE_KEY_B64"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse='INVESTMENT_ANALYTICS_DWH',
    database='INVESTMENT_ANALYTICS',
    schema='CORE'
)

# Retrieve current CIKs from dim_company dimension table in Snowflake
with snowflake_conn.cursor() as cursor:
        cursor.execute("""
                select 
                distinct 
                ticker
                from investment_analytics.core.dim_company
                where is_current = TRUE """)
        tickers = [f"AM.{row[0]}" for row in cursor.fetchall()] '''

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = "localhost:8097" #kafka-1:9092,kafka-2:9092,kafka-3:9093 for access inside containers otherwise use localhost:8097
KAFKA_TOPIC = "stock_aggregates_raw"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

# Avro schema definition
avro_schema_str = """
{
  "type": "record",
  "name": "StockAggregatesStream",
  "namespace": "polygon",
  "fields": [
    {"name": "ticker_symbol", "type": "string"},
    {"name": "event_timestamp", "type": "long"},
    {"name": "volume", "type": "long"},
    {"name": "accumulated_volume", "type": "long"},
    {"name": "volume_weighted_average_price", "type": "double"},
    {"name": "closing_price", "type": "double"},
    {"name": "average_trade_size", "type": "double"}
  ]
}
"""

# Setup Schema Registry
schema_registry_url = {"url": os.getenv("SCHEMA_REGISTRY_URL")}
schema_registry_client = SchemaRegistryClient(schema_registry_url)

# Create avro serializer
avro_serializer = AvroSerializer(
    schema_registry_client,
    avro_schema_str
)

# Kafka producer setup
producer_config = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serializer
}

producer = SerializingProducer(producer_config)

''' Stop kafka stream after 30 minutes commented out for testing purposes '''
# Timer setup
# start_time = time.time()
# run_duration = 30 * 60  # 30 minutes

'''
# WebSocket client setup
ws_client = WebSocketClient(
    api_key=polygon_api_key,
    feed=Feed.Delayed,
    market=Market.Stocks
)

# Subscribe to all tickers
ws_client.subscribe(*tickers)
'''

dummy_tickers = ['AAPL', 'ABNB', 'AAL']

def generate_dummy_message(ticker: str, accumulated_volume:int):
    """Generate a fake stock aggregate message."""
    volume = random.randint(100, 5000)
    closing_price = round(random.uniform(100, 3000), 2)
    vwap = closing_price * random.uniform(0.95, 1.05)
    avg_trade_size = volume / random.randint(10, 50)

    return {
        "ticker_symbol": ticker,
        "event_timestamp": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
        "volume": volume,
        "accumulated_volume": accumulated_volume + volume,
        "volume_weighted_average_price": vwap,
        "closing_price": closing_price,
        "average_trade_size": avg_trade_size
    }

# Handle messages
try:
    accumulated_volumes = {ticker: 0 for ticker in dummy_tickers}
    while True:
        for ticker in dummy_tickers:
            msg = generate_dummy_message(ticker, accumulated_volumes[ticker])
            accumulated_volumes[ticker] = msg["accumulated_volume"]

            try:
                producer.produce(topic=KAFKA_TOPIC, key=ticker, value=msg)
                print(f"Produced dummy: {msg}")
            except Exception as e:
                print(f"Kafka produce error: {e}")

        producer.poll(0)
        time.sleep(60)  # simulate 60-second streaming interval

except KeyboardInterrupt:
    print("Manual interruption. Flushing Kafka producer...")
    producer.flush()

'''def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        if not m.symbol:  # Skip malformed messages
            continue

        message = {
            "ticker_symbol": m.symbol,
            "timestamp": m.end_timestamp,
            "volume": m.volume,
            "accumulated_volume": m.accumulated_volume,
            "volume_weighted_average_price": m.vwap,
            "closing_price": m.close,
            "average_trade_size": m.average_size
        }

        try:
            producer.produce(topic=os.getenv("KAFKA_TOPIC"), key=m.symbol, value=message)
            print(f"Produced: {m.symbol} @ {m.end_timestamp}")
        except Exception as e:
            print(f"Kafka produce error: {e}")

    # Flush periodically
    producer.poll(0)

# Run the WebSocket client
try:
    ws_client.run(handle_msg)
except KeyboardInterrupt:
    print("Manual interruption. Flushing Kafka producer and closing WebSocket...")
    producer.flush()
    ws_client.close()'''




# Import necessary libraries
import os
import logging
import base64
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

# Create setup for logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

# Create setup for logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get Polygon API key from environment variables
polygon_api_key = os.getenv("POLYGON_API_KEY")

# Decode private key for Snowflake connection
private_key_encoded=os.getenv("SNOWFLAKE_PRIVATE_KEY_B64")
private_key_decoded = base64.b64decode(private_key_encoded)

# Create snowflake connection
snowflake_conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    private_key=private_key_decoded,
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
            ticker_symbol
            from investment_analytics.core.dim_company
            where is_current = TRUE """)
    tickers = [f"AM.{row[0]}" for row in cursor.fetchall()]

logger.info("Starting stock aggregates producer...")
logger.info("Loaded %d tickers", len(tickers))
logger.info("Kafka bootstrap servers: %s", os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
logger.info("Kafka topic: %s", os.getenv("KAFKA_TOPIC"))
logger.info("Schema Registry URL: %s", os.getenv("SCHEMA_REGISTRY_URL"))

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
    'value.serializer': avro_serializer,
    "enable.idempotence": True,
    "retries": 5,
    "delivery.timeout.ms": 120000,
    "acks": "all",
}

producer = SerializingProducer(producer_config)

# WebSocket client setup
ws_client = WebSocketClient(
    api_key=polygon_api_key,
    feed=Feed.Delayed,
    market=Market.Stocks
)

# Subscribe to all tickers
ws_client.subscribe(*tickers)

def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        if not m.symbol:  # Skip malformed messages
            continue

        message = {
            "ticker_symbol": m.symbol,
            "event_timestamp": m.end_timestamp,
            "volume": m.volume,
            "accumulated_volume": m.accumulated_volume,
            "volume_weighted_average_price": m.vwap,
            "closing_price": m.close,
            "average_trade_size": m.average_size
        }

        try:
            producer.produce(topic=os.getenv("KAFKA_TOPIC"), key=m.symbol, value=message)
            logger.info(f"Produced: {m.symbol} @ {m.end_timestamp}")
        except Exception as e:
            logger.error(f"Kafka produce error: {e}", exc_info=True)

    # Flush periodically
    producer.poll(0)

# Run the WebSocket client
try:
    logger.info("Starting Polygon WebSocket client...")
    ws_client.run(handle_msg)
except KeyboardInterrupt:
    logger.info("Manual interruption. Flushing Kafka producer and closing WebSocket...")
    producer.flush()
    ws_client.close()
except Exception as e:
    logger.exception(f"WebSocket runtime error: {e}")
    producer.flush()
    ws_client.close()




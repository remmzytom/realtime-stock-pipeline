from pathlib import Path
import os

from dotenv import load_dotenv


# Load .env from the project root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")

# Azure Event Hubs connection string (Kafka-protocol endpoint)
EVENTHUB_CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING", "")
EVENTHUB_NAMESPACE = "eventhub-stock-pipeline.servicebus.windows.net:9093"
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "stocks-raw")

STOCK_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA"]

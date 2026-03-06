import json
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import (
    ALPHA_VANTAGE_API_KEY,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW,
    STOCK_SYMBOLS,
)


ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"


def _to_float(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: str) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def fetch_global_quote(symbol: str) -> dict:
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY,
    }
    response = requests.get(ALPHA_VANTAGE_URL, params=params, timeout=15)
    response.raise_for_status()
    payload = response.json()

    quote = payload.get("Global Quote", {})
    if not quote:
        raise ValueError(f"No quote data returned for {symbol}. Full payload: {payload}")

    return {
        "symbol": quote.get("01. symbol", symbol),
        "open": _to_float(quote.get("02. open")),
        "high": _to_float(quote.get("03. high")),
        "low": _to_float(quote.get("04. low")),
        "close": _to_float(quote.get("05. price")),
        "volume": _to_int(quote.get("06. volume")),
        "latest_trading_day": quote.get("07. latest trading day"),
        "change_percent": quote.get("10. change percent"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def create_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )


def run() -> None:
    if not ALPHA_VANTAGE_API_KEY:
        raise ValueError("ALPHA_VANTAGE_API_KEY is not set. Add it to your .env file.")

    producer = create_kafka_producer()
    print(
        f"Producer started. Sending to topic '{KAFKA_TOPIC_RAW}' "
        f"on '{KAFKA_BOOTSTRAP_SERVERS}'."
    )

    while True:
        for symbol in STOCK_SYMBOLS:
            try:
                record = fetch_global_quote(symbol)
                future = producer.send(
                    topic=KAFKA_TOPIC_RAW,
                    key=symbol,
                    value=record,
                )
                metadata = future.get(timeout=15)
                print(
                    "Sent quote",
                    f"symbol={symbol}",
                    f"topic={metadata.topic}",
                    f"partition={metadata.partition}",
                    f"offset={metadata.offset}",
                )
            except (requests.RequestException, ValueError) as exc:
                print(f"Failed to fetch/parse quote for {symbol}: {exc}")
            except KafkaError as exc:
                print(f"Failed to publish quote for {symbol}: {exc}")
            finally:
                # Alpha Vantage free tier: 5 requests/minute max.
                time.sleep(15)


if __name__ == "__main__":
    run()

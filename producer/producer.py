import json
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from confluent_kafka.error import KafkaError

from config import (
    ALPHA_VANTAGE_API_KEY,
    EVENTHUB_CONNECTION_STRING,
    EVENTHUB_NAMESPACE,
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


def _delivery_report(err, msg) -> None:
    if err:
        print(f"Delivery failed for {msg.key()}: {err}")
    else:
        print(
            f"Sent quote  symbol={msg.key().decode()}  "
            f"topic={msg.topic()}  partition={msg.partition()}  offset={msg.offset()}"
        )


def create_producer() -> Producer:
    # confluent-kafka has full SASL_SSL support, unlike kafka-python which fails
    # to negotiate TLS with Azure Event Hubs. Username must be the literal string
    # "$ConnectionString"; password is the full Event Hubs connection string.
    return Producer({
        "bootstrap.servers":       EVENTHUB_NAMESPACE,
        "security.protocol":       "SASL_SSL",
        "sasl.mechanism":          "PLAIN",
        "sasl.username":           "$ConnectionString",
        "sasl.password":           EVENTHUB_CONNECTION_STRING,
        "socket.timeout.ms":       30000,
        "message.timeout.ms":      30000,
    })


def run() -> None:
    if not ALPHA_VANTAGE_API_KEY:
        raise ValueError("ALPHA_VANTAGE_API_KEY is not set. Add it to your .env file.")
    if not EVENTHUB_CONNECTION_STRING:
        raise ValueError("EVENTHUB_CONNECTION_STRING is not set. Add it to your .env file.")

    producer = create_producer()
    print(f"Producer started. Sending to topic '{KAFKA_TOPIC_RAW}' on '{EVENTHUB_NAMESPACE}'.")

    while True:
        for symbol in STOCK_SYMBOLS:
            try:
                record = fetch_global_quote(symbol)
                producer.produce(
                    topic=KAFKA_TOPIC_RAW,
                    key=symbol.encode("utf-8"),
                    value=json.dumps(record).encode("utf-8"),
                    on_delivery=_delivery_report,
                )
                # Trigger delivery callbacks for any queued messages
                producer.poll(0)
            except (requests.RequestException, ValueError) as exc:
                print(f"Failed to fetch/parse quote for {symbol}: {exc}")
            except KafkaError as exc:
                print(f"Failed to publish quote for {symbol}: {exc}")
            finally:
                # Alpha Vantage free tier: 5 requests/minute max.
                time.sleep(15)


if __name__ == "__main__":
    run()

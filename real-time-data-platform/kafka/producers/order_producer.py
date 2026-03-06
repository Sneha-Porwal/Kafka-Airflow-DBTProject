import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv

from kafka.common import build_producer, produce_with_retry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

TOPIC = "orders_events"
INTERVAL = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))
ORDER_STATUSES = ["PLACED", "CONFIRMED", "CANCELLED", "FULFILLED"]
CURRENCIES = ["USD", "EUR", "INR"]


def generate_order_event() -> dict:
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(12.0, 450.0), 2)
    return {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
        "order_id": f"ORD-{random.randint(100000, 999999)}",
        "user_id": f"USR-{random.randint(1000, 9999)}",
        "product_id": f"PRD-{random.randint(100, 999)}",
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": round(quantity * unit_price, 2),
        "currency": random.choice(CURRENCIES),
        "order_status": random.choice(ORDER_STATUSES),
    }


def main() -> None:
    producer = build_producer()
    logger.info("order_producer_started")

    while True:
        try:
            event = generate_order_event()
            produce_with_retry(producer, TOPIC, event)
            producer.flush(1)
            logger.info("order_event_produced", extra={"event_id": event["event_id"], "order_id": event["order_id"]})
            time.sleep(INTERVAL)
        except Exception as exc:  # noqa: BLE001
            logger.exception("order_producer_failure", extra={"error": str(exc)})
            time.sleep(2)


if __name__ == "__main__":
    main()

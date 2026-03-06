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

TOPIC = "product_catalog_events"
INTERVAL = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))
CATEGORIES = ["Electronics", "Apparel", "Beauty", "Home", "Books"]


def generate_catalog_event() -> dict:
    price = round(random.uniform(8, 800), 2)
    product_num = random.randint(100, 999)
    return {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
        "product_id": f"PRD-{product_num}",
        "sku": f"SKU-{product_num}",
        "product_name": f"Product-{product_num}",
        "category": random.choice(CATEGORIES),
        "unit_price": price,
        "is_active": random.choice([True, True, True, False]),
    }


def main() -> None:
    producer = build_producer()
    logger.info("product_catalog_producer_started")

    while True:
        try:
            event = generate_catalog_event()
            produce_with_retry(producer, TOPIC, event)
            producer.flush(1)
            logger.info("product_catalog_event_produced", extra={"event_id": event["event_id"], "product_id": event["product_id"]})
            time.sleep(INTERVAL)
        except Exception as exc:  # noqa: BLE001
            logger.exception("product_catalog_producer_failure", extra={"error": str(exc)})
            time.sleep(2)


if __name__ == "__main__":
    main()

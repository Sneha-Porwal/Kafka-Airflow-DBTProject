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

TOPIC = "inventory_events"
INTERVAL = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))


def generate_inventory_event() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
        "product_id": f"PRD-{random.randint(100, 999)}",
        "sku": f"SKU-{random.randint(10000, 99999)}",
        "warehouse_id": f"WH-{random.randint(1, 5)}",
        "stock_level": random.randint(0, 120),
        "reorder_level": random.randint(10, 40),
    }


def main() -> None:
    producer = build_producer()
    logger.info("inventory_producer_started")

    while True:
        try:
            event = generate_inventory_event()
            produce_with_retry(producer, TOPIC, event)
            producer.flush(1)
            logger.info("inventory_event_produced", extra={"event_id": event["event_id"], "product_id": event["product_id"]})
            time.sleep(INTERVAL)
        except Exception as exc:  # noqa: BLE001
            logger.exception("inventory_producer_failure", extra={"error": str(exc)})
            time.sleep(2)


if __name__ == "__main__":
    main()

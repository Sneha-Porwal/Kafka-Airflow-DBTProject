import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from kafka.common import build_producer, produce_with_retry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

TOPIC = "shipping_events"
INTERVAL = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))
CARRIERS = ["DHL", "FedEx", "UPS", "BlueDart"]
STATUSES = ["PENDING", "SHIPPED", "IN_TRANSIT", "DELIVERED"]


def generate_shipping_event() -> dict:
    now = datetime.now(timezone.utc)
    eta = now + timedelta(days=random.randint(1, 7))
    status = random.choice(STATUSES)
    delivered_ts = now.isoformat() if status == "DELIVERED" else None
    return {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": now.isoformat(),
        "schema_version": "1.0",
        "shipment_id": f"SHP-{random.randint(100000, 999999)}",
        "order_id": f"ORD-{random.randint(100000, 999999)}",
        "carrier": random.choice(CARRIERS),
        "shipping_status": status,
        "estimated_delivery_date": eta.date().isoformat(),
        "shipped_timestamp": now.isoformat(),
        "delivered_timestamp": delivered_ts,
    }


def main() -> None:
    producer = build_producer()
    logger.info("shipping_producer_started")

    while True:
        try:
            event = generate_shipping_event()
            produce_with_retry(producer, TOPIC, event)
            producer.flush(1)
            logger.info("shipping_event_produced", extra={"event_id": event["event_id"], "shipment_id": event["shipment_id"]})
            time.sleep(INTERVAL)
        except Exception as exc:  # noqa: BLE001
            logger.exception("shipping_producer_failure", extra={"error": str(exc)})
            time.sleep(2)


if __name__ == "__main__":
    main()

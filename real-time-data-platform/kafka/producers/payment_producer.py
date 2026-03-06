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

TOPIC = "payments_events"
INTERVAL = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))
PAYMENT_METHODS = ["CARD", "UPI", "PAYPAL", "NET_BANKING"]
PAYMENT_STATUSES = ["SUCCESS", "FAILED", "PENDING", "REFUNDED"]


def generate_payment_event() -> dict:
    amount = round(random.uniform(10, 2500), 2)
    return {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
        "payment_id": f"PAY-{random.randint(100000, 999999)}",
        "order_id": f"ORD-{random.randint(100000, 999999)}",
        "user_id": f"USR-{random.randint(1000, 9999)}",
        "amount": amount,
        "currency": random.choice(["USD", "EUR", "INR"]),
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_status": random.choice(PAYMENT_STATUSES),
    }


def main() -> None:
    producer = build_producer()
    logger.info("payment_producer_started")

    while True:
        try:
            event = generate_payment_event()
            produce_with_retry(producer, TOPIC, event)
            producer.flush(1)
            logger.info("payment_event_produced", extra={"event_id": event["event_id"], "payment_id": event["payment_id"]})
            time.sleep(INTERVAL)
        except Exception as exc:  # noqa: BLE001
            logger.exception("payment_producer_failure", extra={"error": str(exc)})
            time.sleep(2)


if __name__ == "__main__":
    main()

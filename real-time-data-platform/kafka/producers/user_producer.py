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

TOPIC = "users_events"
INTERVAL = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))
CITIES = ["New York", "San Francisco", "London", "Bangalore", "Berlin"]
COUNTRIES = ["US", "UK", "IN", "DE"]


def generate_user_event() -> dict:
    signup_ts = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 365))
    user_num = random.randint(1000, 9999)
    return {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
        "user_id": f"USR-{user_num}",
        "email": f"user{user_num}@example.com",
        "full_name": f"User {user_num}",
        "city": random.choice(CITIES),
        "country": random.choice(COUNTRIES),
        "signup_ts": signup_ts.isoformat(),
        "is_active": random.choice([True, True, True, False]),
    }


def main() -> None:
    producer = build_producer()
    logger.info("user_producer_started")

    while True:
        try:
            event = generate_user_event()
            produce_with_retry(producer, TOPIC, event)
            producer.flush(1)
            logger.info("user_event_produced", extra={"event_id": event["event_id"], "user_id": event["user_id"]})
            time.sleep(INTERVAL)
        except Exception as exc:  # noqa: BLE001
            logger.exception("user_producer_failure", extra={"error": str(exc)})
            time.sleep(2)


if __name__ == "__main__":
    main()

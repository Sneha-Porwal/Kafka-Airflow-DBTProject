import json
import logging
import os
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv

from kafka.common import build_consumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

TOPIC = "payments_events"
FRAUD_THRESHOLD = float(os.getenv("FRAUD_AMOUNT_THRESHOLD", "1000"))


def get_db_connection():
    host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    port = os.getenv("POSTGRES_PORT", "5433")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "Tiger")
    dbname = os.getenv("POSTGRES_DB", "Ecommerce")
    logger.debug("db_connection_config", extra={
        "host": host,
        "port": port,
        "user": user,
        "dbname": dbname,
    })
    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
    )


def score_risk(amount: float, method: str) -> float:
    score = min((amount / FRAUD_THRESHOLD) * 50, 90)
    if method in {"PAYPAL", "NET_BANKING"}:
        score += 10
    return round(min(score, 99.9), 2)


def main() -> None:
    consumer = build_consumer([TOPIC], group_id="fraud-detection-group")
    logger.info("fraud_detection_consumer_started", extra={"threshold": FRAUD_THRESHOLD})

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                while True:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        logger.error("fraud_consumer_message_error", extra={"error": str(msg.error())})
                        continue

                    try:
                        event = json.loads(msg.value().decode("utf-8"))
                        amount = float(event.get("amount", 0))
                        if amount < FRAUD_THRESHOLD:
                            consumer.commit(asynchronous=False)
                            continue

                        risk_score = score_risk(amount, event.get("payment_method", ""))
                        query = """
                            INSERT INTO bronze.fraud_alerts (
                                event_id, order_id, user_id, amount, risk_score, reason, event_timestamp, created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                            ON CONFLICT (event_id)
                            DO UPDATE SET risk_score = EXCLUDED.risk_score, updated_at = NOW();
                        """
                        cursor.execute(
                            query,
                            (
                                event["event_id"],
                                event.get("order_id"),
                                event.get("user_id"),
                                amount,
                                risk_score,
                                f"Payment above threshold {FRAUD_THRESHOLD}",
                                datetime.fromisoformat(event["event_timestamp"].replace("Z", "+00:00")).astimezone(timezone.utc),
                            ),
                        )
                        conn.commit()
                        consumer.commit(asynchronous=False)
                        logger.info("fraud_alert_created", extra={"event_id": event["event_id"], "risk_score": risk_score})
                    except Exception as exc:  # noqa: BLE001
                        conn.rollback()
                        logger.exception("fraud_consumer_failure", extra={"error": str(exc)})

    except Exception as exc:  # noqa: BLE001
        logger.exception("fraud_consumer_fatal_error", extra={"error": str(exc)})
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

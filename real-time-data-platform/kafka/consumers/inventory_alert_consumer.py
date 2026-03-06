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

TOPIC = "inventory_events"
LOW_STOCK_THRESHOLD = int(os.getenv("LOW_STOCK_THRESHOLD", "15"))


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


def main() -> None:
    consumer = build_consumer([TOPIC], group_id="inventory-alert-group")
    logger.info("inventory_alert_consumer_started", extra={"threshold": LOW_STOCK_THRESHOLD})

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                while True:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        logger.error("inventory_consumer_message_error", extra={"error": str(msg.error())})
                        continue

                    try:
                        event = json.loads(msg.value().decode("utf-8"))
                        stock_level = int(event.get("stock_level", 0))

                        if stock_level > LOW_STOCK_THRESHOLD:
                            consumer.commit(asynchronous=False)
                            continue

                        query = """
                            INSERT INTO bronze.inventory_alerts (
                                event_id, product_id, sku, stock_level, threshold, warehouse_id,
                                alert_message, event_timestamp, created_at, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                            ON CONFLICT (event_id)
                            DO UPDATE SET stock_level = EXCLUDED.stock_level, updated_at = NOW();
                        """

                        cursor.execute(
                            query,
                            (
                                event["event_id"],
                                event.get("product_id"),
                                event.get("sku"),
                                stock_level,
                                LOW_STOCK_THRESHOLD,
                                event.get("warehouse_id"),
                                f"Low stock detected for {event.get('product_id')}: {stock_level}",
                                datetime.fromisoformat(event["event_timestamp"].replace("Z", "+00:00")).astimezone(timezone.utc),
                            ),
                        )
                        conn.commit()
                        consumer.commit(asynchronous=False)
                        logger.info("inventory_alert_created", extra={"event_id": event["event_id"], "stock_level": stock_level})
                    except Exception as exc:  # noqa: BLE001
                        conn.rollback()
                        logger.exception("inventory_alert_consumer_failure", extra={"error": str(exc)})

    except Exception as exc:  # noqa: BLE001
        logger.exception("inventory_alert_consumer_fatal_error", extra={"error": str(exc)})
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

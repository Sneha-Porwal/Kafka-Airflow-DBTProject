import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

TOPICS = [
    "orders_events",
    "payments_events",
    "users_events",
    "inventory_events",
    "product_catalog_events",
    "shipping_events",
    "dlq_events",
]


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=os.getenv("POSTGRES_PORT", "5433"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "Tiger"),
        dbname=os.getenv("POSTGRES_DB", "Ecommerce"),
    )


def get_consumer() -> Consumer:
    conf = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        "group.id": "monitoring-lag-group",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe(TOPICS)
    return consumer


def upsert_metric(cur, metric_name: str, metric_value: float) -> None:
    cur.execute(
        """
        INSERT INTO monitoring.pipeline_metrics(metric_name, metric_value, metric_timestamp, created_at)
        VALUES (%s, %s, %s, NOW());
        """,
        (metric_name, metric_value, datetime.now(timezone.utc)),
    )


def compute_total_revenue(cur) -> float:
    cur.execute(
        """
        SELECT COALESCE(SUM(amount), 0)
        FROM gold.fact_payments
        WHERE payment_status = 'SUCCESS';
        """
    )
    value = cur.fetchone()[0]
    return float(value)


def get_failed_event_count(cur) -> int:
    cur.execute(
        """
        SELECT COUNT(*)
        FROM bronze.events_raw
        WHERE topic_name = 'dlq_events'
          AND ingestion_date = CURRENT_DATE;
        """
    )
    return int(cur.fetchone()[0])


def main() -> None:
    consumer = get_consumer()
    logger.info("metrics_logger_started")

    processed_last_minute = 0
    minute_start = time.time()

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                while True:
                    msg = consumer.poll(0.5)
                    if msg and not msg.error():
                        processed_last_minute += 1

                    elapsed = time.time() - minute_start
                    if elapsed >= 60:
                        lag_simulation = max(0, 600 - processed_last_minute)
                        failed_events = get_failed_event_count(cur)
                        total_revenue = compute_total_revenue(cur)

                        upsert_metric(cur, "events_processed_per_minute", processed_last_minute)
                        upsert_metric(cur, "lag_monitoring_simulation", lag_simulation)
                        upsert_metric(cur, "failed_events", failed_events)
                        upsert_metric(cur, "total_revenue", total_revenue)
                        conn.commit()

                        logger.info(
                            "metrics_snapshot",
                            extra={
                                "events_processed_per_minute": processed_last_minute,
                                "lag_monitoring_simulation": lag_simulation,
                                "failed_events": failed_events,
                                "total_revenue": total_revenue,
                            },
                        )

                        processed_last_minute = 0
                        minute_start = time.time()
    except Exception as exc:  # noqa: BLE001
        logger.exception("metrics_logger_fatal_error", extra={"error": str(exc)})
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

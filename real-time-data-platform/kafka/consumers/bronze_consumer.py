import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from confluent_kafka import KafkaError
from jsonschema import ValidationError, validate
from psycopg2.extras import execute_batch

from kafka.common import TOPIC_TO_SERVICE, build_consumer, build_producer, produce_with_retry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

TOPICS = list(TOPIC_TO_SERVICE.keys())
DLQ_TOPIC = "dlq_events"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
SCHEMAS_DIR = Path(__file__).resolve().parents[1] / "schemas"
SCHEMA_MAP = {
    "orders_events": "order_schema.json",
    "payments_events": "payment_schema.json",
    "users_events": "user_schema.json",
    "inventory_events": "inventory_schema.json",
    "product_catalog_events": "product_schema.json",
    "shipping_events": "shipping_schema.json",
}


def load_schemas() -> dict:
    schemas = {}
    for topic, file_name in SCHEMA_MAP.items():
        with (SCHEMAS_DIR / file_name).open("r", encoding="utf-8") as handle:
            schemas[topic] = json.load(handle)
    return schemas


def get_db_connection():
    # show what values we will try to use (helpful when dotenv isn't loaded)
    host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    port = os.getenv("POSTGRES_PORT", "5433")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "Tiger")
    dbname = os.getenv("POSTGRES_DB", "Ecommerce")  # align with compose .env
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

def normalize_timestamp(raw_ts: str) -> datetime:
    event_ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
    return event_ts.astimezone(timezone.utc)


def is_late_arrival(event_ts: datetime, minutes: int = 30) -> bool:
    return (datetime.now(timezone.utc) - event_ts).total_seconds() > minutes * 60


def upsert_batch(cursor, rows: list[tuple]) -> None:
    query = """
        INSERT INTO bronze.events_raw (
            event_id,
            topic_name,
            event_timestamp,
            ingestion_date,
            payload,
            schema_version,
            source_service,
            ingestion_ts,
            created_at,
            updated_at
        ) VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, NOW(), NOW(), NOW())
        ON CONFLICT (event_id, ingestion_date)
        DO UPDATE SET
            payload = EXCLUDED.payload,
            updated_at = NOW(),
            event_timestamp = EXCLUDED.event_timestamp,
            schema_version = EXCLUDED.schema_version;
    """
    execute_batch(cursor, query, rows, page_size=BATCH_SIZE)


def send_to_dlq(dlq_producer, topic: str, payload: dict, reason: str) -> None:
    dlq_payload = {
        "event_id": payload.get("event_id"),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "source_topic": topic,
        "failure_reason": reason,
        "payload": payload,
    }
    produce_with_retry(dlq_producer, DLQ_TOPIC, dlq_payload)
    dlq_producer.flush(1)


def main() -> None:
    schemas = load_schemas()
    consumer = build_consumer(TOPICS, group_id="bronze-consumer-group")
    dlq_producer = build_producer()
    pending = []

    logger.info("bronze_consumer_started", extra={"topics": TOPICS, "batch_size": BATCH_SIZE})

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                while True:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        if pending:
                            upsert_batch(cursor, pending)
                            conn.commit()
                            consumer.commit(asynchronous=False)
                            logger.info("batch_committed", extra={"records": len(pending)})
                            pending.clear()
                        continue

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            logger.info("kafka_partition_eof_reached")
                            continue
                        logger.error("kafka_message_error: %s", msg.error())
                        continue

                    topic = msg.topic()
                    try:
                        event = json.loads(msg.value().decode("utf-8"))
                        validate(instance=event, schema=schemas[topic])

                        event_ts = normalize_timestamp(event["event_timestamp"])
                        if is_late_arrival(event_ts):
                            logger.warning("late_arrival_detected", extra={"event_id": event.get("event_id"), "topic": topic})

                        pending.append(
                            (
                                event["event_id"],
                                topic,
                                event_ts,
                                datetime.now(timezone.utc).date(),
                                json.dumps(event),
                                event.get("schema_version", "1.0"),
                                TOPIC_TO_SERVICE.get(topic, "unknown_service"),
                            )
                        )

                        if len(pending) >= BATCH_SIZE:
                            upsert_batch(cursor, pending)
                            conn.commit()
                            consumer.commit(asynchronous=False)
                            logger.info("batch_committed", extra={"records": len(pending)})
                            pending.clear()

                    except ValidationError as exc:
                        logger.warning("schema_validation_failed", extra={"topic": topic, "error": str(exc)})
                        send_to_dlq(dlq_producer, topic, json.loads(msg.value().decode("utf-8")), f"schema_validation:{exc.message}")
                    except Exception as exc:  # noqa: BLE001
                        logger.exception("bronze_consumer_processing_failure", extra={"error": str(exc), "topic": topic})
                        send_to_dlq(dlq_producer, topic, {"raw": msg.value().decode("utf-8")}, f"unexpected_error:{str(exc)}")
                        conn.rollback()

    except Exception as exc:  # noqa: BLE001
        logger.exception("bronze_consumer_fatal_error", extra={"error": str(exc)})
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


@dataclass
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    group_id: str = os.getenv("KAFKA_GROUP_ID", "rt-platform-consumers")
    auto_offset_reset: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")


TOPIC_TO_SERVICE = {
    "orders_events": "orders_service",
    "payments_events": "payments_service",
    "users_events": "users_service",
    "inventory_events": "inventory_service",
    "product_catalog_events": "product_catalog_service",
    "shipping_events": "shipping_service",
}


def build_producer() -> Producer:
    conf = {
        "bootstrap.servers": KafkaConfig().bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "retries": 10,
        "linger.ms": 50,
    }
    return Producer(conf)


def build_consumer(topics: list[str], group_id: str | None = None) -> Consumer:
    cfg = KafkaConfig()
    conf = {
        "bootstrap.servers": cfg.bootstrap_servers,
        "group.id": group_id or cfg.group_id,
        "auto.offset.reset": cfg.auto_offset_reset,
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe(topics)
    return consumer


def delivery_callback(err, msg) -> None:
    if err:
        logger.error("delivery_failed", extra={"topic": msg.topic(), "error": str(err)})
        return
    logger.info(
        "delivery_success",
        extra={
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
        },
    )


def serialize_event(event: Dict[str, Any]) -> str:
    return json.dumps(event, default=str)


def produce_with_retry(producer: Producer, topic: str, event: Dict[str, Any], max_attempts: int = 5) -> None:
    payload = serialize_event(event)
    last_exception: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            producer.produce(topic=topic, key=event["event_id"], value=payload, callback=delivery_callback)
            producer.poll(0)
            return
        except Exception as exc:  # noqa: BLE001
            last_exception = exc
            backoff = min(2 ** attempt, 20)
            logger.warning(
                "produce_retry",
                extra={"topic": topic, "attempt": attempt, "max_attempts": max_attempts, "error": str(exc)},
            )
            time.sleep(backoff)

    if last_exception:
        raise last_exception

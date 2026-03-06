import logging
import os
from datetime import timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "sla": timedelta(minutes=4),
}


def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "Tiger"),
        dbname=os.getenv("POSTGRES_DB", "Ecommerce"),
    )


def transform_users() -> None:
    sql = """
    INSERT INTO silver.users_clean (user_id, email, full_name, city, country, signup_ts, is_active, event_timestamp, created_at, updated_at)
    SELECT
        payload ->> 'user_id' AS user_id,
        COALESCE(payload ->> 'email', 'unknown@example.com') AS email,
        COALESCE(payload ->> 'full_name', 'Unknown User') AS full_name,
        payload ->> 'city' AS city,
        payload ->> 'country' AS country,
        NULLIF(payload ->> 'signup_ts', '')::timestamptz AS signup_ts,
        COALESCE((payload ->> 'is_active')::boolean, false) AS is_active,
        event_timestamp AT TIME ZONE 'UTC' AS event_timestamp,
        NOW(), NOW()
    FROM bronze.events_raw
    WHERE topic_name = 'users_events'
    ON CONFLICT (user_id) DO UPDATE SET
      email = EXCLUDED.email,
      full_name = EXCLUDED.full_name,
      city = EXCLUDED.city,
      country = EXCLUDED.country,
      signup_ts = EXCLUDED.signup_ts,
      is_active = EXCLUDED.is_active,
      event_timestamp = EXCLUDED.event_timestamp,
      updated_at = NOW();
    """

    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()


def transform_products() -> None:
    sql = """
    INSERT INTO silver.products_clean (product_id, sku, product_name, category, unit_price, is_active, event_timestamp, created_at, updated_at)
    SELECT
        payload ->> 'product_id' AS product_id,
        payload ->> 'sku' AS sku,
        COALESCE(payload ->> 'product_name', 'Unknown Product') AS product_name,
        payload ->> 'category' AS category,
        COALESCE((payload ->> 'unit_price')::numeric, 0)::numeric(12,2) AS unit_price,
        COALESCE((payload ->> 'is_active')::boolean, true) AS is_active,
        event_timestamp AT TIME ZONE 'UTC' AS event_timestamp,
        NOW(), NOW()
    FROM bronze.events_raw
    WHERE topic_name = 'product_catalog_events'
    ON CONFLICT (product_id) DO UPDATE SET
      sku = EXCLUDED.sku,
      product_name = EXCLUDED.product_name,
      category = EXCLUDED.category,
      unit_price = EXCLUDED.unit_price,
      is_active = EXCLUDED.is_active,
      event_timestamp = EXCLUDED.event_timestamp,
      updated_at = NOW();
    """

    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()


def transform_orders() -> None:
    sql = """
    WITH ranked AS (
        SELECT
            payload ->> 'order_id' AS order_id,
            payload ->> 'user_id' AS user_id,
            payload ->> 'product_id' AS product_id,
            COALESCE((payload ->> 'quantity')::integer, 1) AS quantity,
            COALESCE((payload ->> 'unit_price')::numeric, 0)::numeric(12,2) AS unit_price,
            COALESCE((payload ->> 'total_amount')::numeric, 0)::numeric(12,2) AS total_amount,
            COALESCE(payload ->> 'currency', 'USD') AS currency,
            COALESCE(payload ->> 'order_status', 'PLACED') AS order_status,
            event_timestamp AT TIME ZONE 'UTC' AS order_timestamp,
            event_id AS source_event_id,
            ROW_NUMBER() OVER (PARTITION BY payload ->> 'order_id' ORDER BY event_timestamp DESC, ingestion_ts DESC) AS rn
        FROM bronze.events_raw
        WHERE topic_name = 'orders_events'
    )
    INSERT INTO silver.orders_clean (
        order_id, user_id, product_id, quantity, unit_price, total_amount,
        currency, order_status, order_timestamp, source_event_id, created_at, updated_at
    )
    SELECT
        order_id, user_id, product_id, quantity, unit_price, total_amount,
        currency, order_status, order_timestamp, source_event_id, NOW(), NOW()
    FROM ranked
    WHERE rn = 1
    ON CONFLICT (order_id) DO UPDATE SET
        user_id = EXCLUDED.user_id,
        product_id = EXCLUDED.product_id,
        quantity = EXCLUDED.quantity,
        unit_price = EXCLUDED.unit_price,
        total_amount = EXCLUDED.total_amount,
        currency = EXCLUDED.currency,
        order_status = EXCLUDED.order_status,
        order_timestamp = EXCLUDED.order_timestamp,
        source_event_id = EXCLUDED.source_event_id,
        updated_at = NOW();
    """

    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()


def transform_payments() -> None:
    sql = """
    WITH ranked AS (
        SELECT
            payload ->> 'payment_id' AS payment_id,
            payload ->> 'order_id' AS order_id,
            payload ->> 'user_id' AS user_id,
            COALESCE((payload ->> 'amount')::numeric, 0)::numeric(12,2) AS amount,
            COALESCE(payload ->> 'currency', 'USD') AS currency,
            COALESCE(payload ->> 'payment_method', 'CARD') AS payment_method,
            COALESCE(payload ->> 'payment_status', 'PENDING') AS payment_status,
            event_timestamp AT TIME ZONE 'UTC' AS payment_timestamp,
            event_id AS source_event_id,
            ROW_NUMBER() OVER (PARTITION BY payload ->> 'payment_id' ORDER BY event_timestamp DESC, ingestion_ts DESC) AS rn
        FROM bronze.events_raw
        WHERE topic_name = 'payments_events'
    )
    INSERT INTO silver.payments_clean (
        payment_id, order_id, user_id, amount, currency, payment_method,
        payment_status, payment_timestamp, source_event_id, created_at, updated_at
    )
    SELECT
        payment_id, order_id, user_id, amount, currency, payment_method,
        payment_status, payment_timestamp, source_event_id, NOW(), NOW()
    FROM ranked
    WHERE rn = 1
    ON CONFLICT (payment_id) DO UPDATE SET
        order_id = EXCLUDED.order_id,
        user_id = EXCLUDED.user_id,
        amount = EXCLUDED.amount,
        currency = EXCLUDED.currency,
        payment_method = EXCLUDED.payment_method,
        payment_status = EXCLUDED.payment_status,
        payment_timestamp = EXCLUDED.payment_timestamp,
        source_event_id = EXCLUDED.source_event_id,
        updated_at = NOW();
    """

    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()


def transform_shipping() -> None:
    sql = """
    WITH ranked AS (
        SELECT
            payload ->> 'shipment_id' AS shipment_id,
            payload ->> 'order_id' AS order_id,
            COALESCE(payload ->> 'carrier', 'UNKNOWN') AS carrier,
            COALESCE(payload ->> 'shipping_status', 'PENDING') AS shipping_status,
            NULLIF(payload ->> 'estimated_delivery_date', '')::date AS estimated_delivery_date,
            NULLIF(payload ->> 'shipped_timestamp', '')::timestamptz AS shipped_timestamp,
            NULLIF(payload ->> 'delivered_timestamp', '')::timestamptz AS delivered_timestamp,
            event_id AS source_event_id,
            ROW_NUMBER() OVER (PARTITION BY payload ->> 'shipment_id' ORDER BY event_timestamp DESC, ingestion_ts DESC) AS rn
        FROM bronze.events_raw
        WHERE topic_name = 'shipping_events'
    )
    INSERT INTO silver.shipping_clean (
        shipment_id, order_id, carrier, shipping_status, estimated_delivery_date,
        shipped_timestamp, delivered_timestamp, source_event_id, created_at, updated_at
    )
    SELECT
        shipment_id, order_id, carrier, shipping_status, estimated_delivery_date,
        shipped_timestamp, delivered_timestamp, source_event_id, NOW(), NOW()
    FROM ranked
    WHERE rn = 1
    ON CONFLICT (shipment_id) DO UPDATE SET
        order_id = EXCLUDED.order_id,
        carrier = EXCLUDED.carrier,
        shipping_status = EXCLUDED.shipping_status,
        estimated_delivery_date = EXCLUDED.estimated_delivery_date,
        shipped_timestamp = EXCLUDED.shipped_timestamp,
        delivered_timestamp = EXCLUDED.delivered_timestamp,
        source_event_id = EXCLUDED.source_event_id,
        updated_at = NOW();
    """

    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()


with DAG(
    dag_id="bronze_to_silver_dag",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "bronze", "silver"],
) as dag:
    users_task = PythonOperator(task_id="transform_users", python_callable=transform_users)
    products_task = PythonOperator(task_id="transform_products", python_callable=transform_products)
    orders_task = PythonOperator(task_id="transform_orders", python_callable=transform_orders)
    payments_task = PythonOperator(task_id="transform_payments", python_callable=transform_payments)
    shipping_task = PythonOperator(task_id="transform_shipping", python_callable=transform_shipping)

    [users_task, products_task] >> orders_task >> payments_task >> shipping_task

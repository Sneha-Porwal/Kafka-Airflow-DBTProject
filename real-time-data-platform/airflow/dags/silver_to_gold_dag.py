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
    "retry_delay": timedelta(minutes=3),
    "sla": timedelta(minutes=9),
}


def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "Tiger"),
        dbname=os.getenv("POSTGRES_DB", "Ecommerce"),
    )


def build_dimensions() -> None:
    sql = """
    INSERT INTO gold.dim_users (user_id, email, full_name, city, country, signup_ts, is_active, created_at, updated_at)
    SELECT user_id, email, full_name, city, country, signup_ts, is_active, NOW(), NOW()
    FROM silver.users_clean
    ON CONFLICT (user_id) DO UPDATE SET
      email = EXCLUDED.email,
      full_name = EXCLUDED.full_name,
      city = EXCLUDED.city,
      country = EXCLUDED.country,
      signup_ts = EXCLUDED.signup_ts,
      is_active = EXCLUDED.is_active,
      updated_at = NOW();

    INSERT INTO gold.dim_products (product_id, sku, product_name, category, unit_price, is_active, created_at, updated_at)
    SELECT product_id, sku, product_name, category, unit_price, is_active, NOW(), NOW()
    FROM silver.products_clean
    ON CONFLICT (product_id) DO UPDATE SET
      sku = EXCLUDED.sku,
      product_name = EXCLUDED.product_name,
      category = EXCLUDED.category,
      unit_price = EXCLUDED.unit_price,
      is_active = EXCLUDED.is_active,
      updated_at = NOW();

    INSERT INTO gold.dim_date (date_sk, date_day, day_of_week, month, quarter, year, is_weekend, created_at, updated_at)
    SELECT
      TO_CHAR(d::date, 'YYYYMMDD')::int AS date_sk,
      d::date,
      EXTRACT(ISODOW FROM d)::int,
      EXTRACT(MONTH FROM d)::int,
      EXTRACT(QUARTER FROM d)::int,
      EXTRACT(YEAR FROM d)::int,
      CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN true ELSE false END,
      NOW(), NOW()
    FROM generate_series(CURRENT_DATE - INTERVAL '365 days', CURRENT_DATE + INTERVAL '365 days', INTERVAL '1 day') d
    ON CONFLICT (date_sk) DO NOTHING;
    """
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()


def build_facts() -> None:
    sql = """
    INSERT INTO gold.fact_orders (order_id, user_id, product_id, order_date, quantity, total_amount, order_status, created_at, updated_at)
    SELECT order_id, user_id, product_id, order_timestamp::date, quantity, total_amount, order_status, NOW(), NOW()
    FROM silver.orders_clean
    ON CONFLICT (order_id) DO UPDATE SET
      user_id = EXCLUDED.user_id,
      product_id = EXCLUDED.product_id,
      order_date = EXCLUDED.order_date,
      quantity = EXCLUDED.quantity,
      total_amount = EXCLUDED.total_amount,
      order_status = EXCLUDED.order_status,
      updated_at = NOW();

    INSERT INTO gold.fact_payments (payment_id, order_id, user_id, payment_date, amount, payment_method, payment_status, created_at, updated_at)
    SELECT payment_id, order_id, user_id, payment_timestamp::date, amount, payment_method, payment_status, NOW(), NOW()
    FROM silver.payments_clean
    ON CONFLICT (payment_id) DO UPDATE SET
      order_id = EXCLUDED.order_id,
      user_id = EXCLUDED.user_id,
      payment_date = EXCLUDED.payment_date,
      amount = EXCLUDED.amount,
      payment_method = EXCLUDED.payment_method,
      payment_status = EXCLUDED.payment_status,
      updated_at = NOW();

    INSERT INTO gold.fact_shipping (shipment_id, order_id, shipping_status, carrier, shipped_date, delivered_date, created_at, updated_at)
    SELECT shipment_id, order_id, shipping_status, carrier, shipped_timestamp::date, delivered_timestamp::date, NOW(), NOW()
    FROM silver.shipping_clean
    ON CONFLICT (shipment_id) DO UPDATE SET
      order_id = EXCLUDED.order_id,
      shipping_status = EXCLUDED.shipping_status,
      carrier = EXCLUDED.carrier,
      shipped_date = EXCLUDED.shipped_date,
      delivered_date = EXCLUDED.delivered_date,
      updated_at = NOW();
    """
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()


def calculate_kpis() -> None:
    sql = """
    INSERT INTO monitoring.pipeline_metrics(metric_name, metric_value, metric_timestamp)
    SELECT 'daily_revenue', COALESCE(SUM(amount), 0), NOW()
    FROM gold.fact_payments
    WHERE payment_date = CURRENT_DATE AND payment_status = 'SUCCESS';

    INSERT INTO monitoring.pipeline_metrics(metric_name, metric_value, metric_timestamp)
    SELECT 'daily_orders', COUNT(*), NOW()
    FROM gold.fact_orders
    WHERE order_date = CURRENT_DATE;
    """
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()


with DAG(
    dag_id="silver_to_gold_dag",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "silver", "gold"],
) as dag:
    dims_task = PythonOperator(task_id="build_dimensions", python_callable=build_dimensions)
    facts_task = PythonOperator(task_id="build_facts", python_callable=build_facts)
    kpi_task = PythonOperator(task_id="calculate_kpis", python_callable=calculate_kpis)

    dims_task >> facts_task >> kpi_task

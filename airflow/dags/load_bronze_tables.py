from airflow.decorators import dag, task
from datetime import datetime
import clickhouse_connect
import csv


TABLE_CONFIGS = [
    {
        "table": "customers",
        "csv_file": "olist_customers_dataset.csv",
        "columns": ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
    },
    {
        "table": "orders",
        "csv_file": "olist_orders_dataset.csv",
        "columns": ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"],
    },
    {
        "table": "order_items",
        "csv_file": "olist_order_items_dataset.csv",
        "columns": ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"],
    },
    {
        "table": "order_payments",
        "csv_file": "olist_order_payments_dataset.csv",
        "columns": ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"],
    },
    {
        "table": "order_reviews",
        "csv_file": "olist_order_reviews_dataset.csv",
        "columns": ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message", "review_creation_date", "review_answer_timestamp"],
    },
    {
        "table": "products",
        "csv_file": "olist_products_dataset.csv",
        "columns": ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"],
    },
    {
        "table": "sellers",
        "csv_file": "olist_sellers_dataset.csv",
        "columns": ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    },
    {
        "table": "geolocation",
        "csv_file": "olist_geolocation_dataset.csv",
        "columns": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"],
    },
    {
        "table": "product_category_name_translation",
        "csv_file": "product_category_name_translation.csv",
        "columns": ["product_category_name", "product_category_name_english"],
    },
]


@dag(
    dag_id="load_bronze_tables",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "ingestion"],
)
def load_bronze_tables():

    @task
    def load_table(config: dict):
        client = clickhouse_connect.get_client(
            host="clickhouse",
            port=8123,
            username="default",
            password="clickhouse123",
        )

        csv_path = f"/opt/airflow/data/raw/{config['csv_file']}"
        columns = config["columns"]
        table = config["table"]

        rows = []
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append([row[col] for col in columns])

        client.command(f"TRUNCATE TABLE bronze.{table}")
        client.insert(f"bronze.{table}", rows, column_names=columns)

        print(f"Loaded {len(rows)} rows into bronze.{table}")

    load_table.expand(config=TABLE_CONFIGS)


load_bronze_tables()
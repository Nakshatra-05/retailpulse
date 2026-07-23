from airflow.decorators import dag, task
from datetime import datetime
import clickhouse_connect
import csv


@dag(
    dag_id="load_customers_bronze",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "ingestion"],
)
def load_customers_bronze():

    @task
    def load_customers():
        client = clickhouse_connect.get_client(
            host="clickhouse",
            port=8123,
            username="default",
            password="clickhouse123",
        )

        csv_path = "/opt/airflow/data/raw/olist_customers_dataset.csv"

        rows = []
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append([
                    row["customer_id"],
                    row["customer_unique_id"],
                    row["customer_zip_code_prefix"],
                    row["customer_city"],
                    row["customer_state"],
                ])

        client.command("TRUNCATE TABLE bronze.customers")

        client.insert(
            "bronze.customers",
            rows,
            column_names=[
                "customer_id",
                "customer_unique_id",
                "customer_zip_code_prefix",
                "customer_city",
                "customer_state",
            ],
        )

        print(f"Loaded {len(rows)} rows into bronze.customers")

    load_customers()


load_customers_bronze()
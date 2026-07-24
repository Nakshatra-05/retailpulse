from airflow.decorators import dag, task
from datetime import datetime
import clickhouse_connect
import json

from confluent_kafka import Consumer, KafkaException, TopicPartition

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "retail_events"
CONSUMER_GROUP = "airflow-bronze-consumer"
BATCH_TIMEOUT_SECONDS = 30
MAX_MESSAGES_PER_RUN = 5000


@dag(
    dag_id="consume_retail_events",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "streaming"],
)
def consume_retail_events():

    @task
    def consume_batch():
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe([TOPIC])

        rows = []
        try:
            start = datetime.now()
            while len(rows) < MAX_MESSAGES_PER_RUN:
                elapsed = (datetime.now() - start).total_seconds()
                if elapsed > BATCH_TIMEOUT_SECONDS:
                    break

                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                event = json.loads(msg.value().decode("utf-8"))
                rows.append([
                    event["event_id"],
                    event["event_type"],
                    event["user_id"],
                    event["session_id"],
                    event["timestamp"],
                    event["product_category"],
                    str(event["price"]) if event["price"] is not None else None,
                    event["city"],
                    event["state"],
                ])

            if rows:
                client = clickhouse_connect.get_client(
                    host="clickhouse",
                    port=8123,
                    username="default",
                    password="clickhouse123",
                )
                client.insert(
                    "bronze.retail_events",
                    rows,
                    column_names=[
                        "event_id", "event_type", "user_id", "session_id",
                        "event_timestamp", "product_category", "price",
                        "city", "state",
                    ],
                )
                consumer.commit()
                print(f"Inserted {len(rows)} events into bronze.retail_events")
            else:
                print("No new events available")

        finally:
            consumer.close()

    consume_batch()


consume_retail_events()
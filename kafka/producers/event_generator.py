import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker

fake = Faker("pt_BR")  # Brazilian locale, matching Olist's origin

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "retail_events"

EVENT_TYPES = [
    "user_login",
    "product_view",
    "product_search",
    "add_to_cart",
    "remove_from_cart",
    "checkout_started",
    "order_placed",
    "payment_completed",
    "order_cancelled",
]

# Weighted so funnel events are rarer than browsing events, mimicking real behavior
EVENT_WEIGHTS = [15, 25, 15, 15, 5, 10, 8, 6, 1]

PRODUCT_CATEGORIES = [
    "electronics", "home_appliances", "fashion", "beauty",
    "sports", "toys", "books", "furniture", "groceries",
]


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")


def generate_event(user_id: str, session_id: str) -> dict:
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": user_id,
        "session_id": session_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "product_category": random.choice(PRODUCT_CATEGORIES),
        "price": round(random.uniform(10, 2000), 2) if event_type in (
            "product_view", "add_to_cart", "order_placed", "payment_completed"
        ) else None,
        "city": fake.city(),
        "state": fake.estado_sigla(),
    }
    return event


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    print(f"Starting event generator, sending to topic '{TOPIC}' on {KAFKA_BOOTSTRAP_SERVERS}")

    # Simulate a pool of active users/sessions rather than a brand new user every event
    active_sessions = [
        {"user_id": str(uuid.uuid4()), "session_id": str(uuid.uuid4())}
        for _ in range(20)
    ]

    while True:
        session = random.choice(active_sessions)
        event = generate_event(session["user_id"], session["session_id"])

        producer.produce(
            TOPIC,
            key=event["user_id"],
            value=json.dumps(event),
            callback=delivery_report,
        )
        producer.poll(0)

        print(f"Produced: {event['event_type']} for user {event['user_id'][:8]}")

        time.sleep(random.uniform(0.5, 2.0))


if __name__ == "__main__":
    main()
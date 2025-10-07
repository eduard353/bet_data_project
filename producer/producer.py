import os
import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_NAME = os.getenv("TOPIC_NAME")

games = ["football", "basketball", "tennis", "hockey", "esports"]
statuses = ["win", "lose", "pending"]

def create_event():
    return {
        "bet_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()), 
        "game": random.choice(games),
        "amount": round(random.uniform(5, 500), 2),
        "status": random.choice(statuses),
        "timestamp": datetime.utcnow().isoformat()
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"[PRODUCER] Sending events to topic: {TOPIC_NAME}")
    while True:
        event = create_event()
        producer.send(TOPIC_NAME, event)
        print(f"[PRODUCER] Sent: {event}")
        time.sleep(2)  # каждые 2 секунды новая ставка

if __name__ == "__main__":
    main()

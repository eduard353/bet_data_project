import os
import json
import time
import random
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "bets")

sports = {
    "football": ["Premier League", "La Liga", "Serie A"],
    "basketball": ["NBA", "EuroLeague"],
    "tennis": ["ATP", "WTA"],
    "hockey": ["NHL", "KHL"],
    "esports": ["CS:GO", "Dota 2", "Valorant"]
}

statuses = ["win", "lose", "pending"]
devices = ["mobile", "desktop", "tablet"]
currencies = ["USD", "EUR", "KZT", "RUB"]
countries = ["KZ", "RU", "US", "GB", "DE", "PL", "UZ"]

def create_event():
    sport = random.choice(list(sports.keys()))
    league = random.choice(sports[sport])
    start_time = datetime.utcnow() + timedelta(hours=random.randint(1, 72))
    amount = round(random.uniform(5, 500), 2)
    odds = round(random.uniform(1.1, 5.0), 2)
    status = random.choice(statuses)
    payout = round(amount * odds, 2) if status == "win" else 0.0

    event = {
        "bet_id": str(uuid.uuid4()),
        "user": {
            "user_id": str(uuid.uuid4()),
            "country": random.choice(countries),
            "age": random.randint(18, 65),
            "device": random.choice(devices),
            "ip_address": f"192.168.{random.randint(0, 255)}.{random.randint(0, 255)}"
        },
        "event": {
            "sport": sport,
            "league": league,
            "match": f"Team{random.randint(1,20)} vs Team{random.randint(21,40)}",
            "start_time": start_time.isoformat(),
            "selection": f"Team{random.randint(1,40)} wins",
            "odds": odds
        },
        "bet": {
            "amount": amount,
            "currency": random.choice(currencies),
            "status": status,
            "payout": payout,
            "timestamp": datetime.utcnow().isoformat()
        },
        "metadata": {
            "source": random.choice(["web", "app", "affiliate"]),
            "session_id": str(uuid.uuid4()),
            "created_at": datetime.utcnow().isoformat()
        }
    }
    return event


def main():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"[PRODUCER] Sending enriched bet events to topic: {TOPIC_NAME}")
    while True:
        event = create_event()
        producer.send(TOPIC_NAME, event)
        print(f"[PRODUCER] Sent: {json.dumps(event, indent=2)}")
        time.sleep(2)


if __name__ == "__main__":
    main()

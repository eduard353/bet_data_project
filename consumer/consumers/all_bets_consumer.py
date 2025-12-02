import os
import json
import uuid
from datetime import datetime
from decimal import Decimal

from kafka import KafkaConsumer
from sqlalchemy import (
    create_engine, Column, Integer, String, Numeric, TIMESTAMP, Boolean, JSON
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

# --- Kafka ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")  # Используем имя сервиса из docker-compose
TOPIC_NAME = os.getenv("TOPIC_NAME", "bets")


# --- PostgreSQL ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")  # Используем имя сервиса из docker-compose
POSTGRES_DB = os.getenv("POSTGRES_DB", "bets_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# --- SQLAlchemy setup ---
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# --- SQLAlchemy Model ---
class Bet(Base):
    __tablename__ = "bets"

    bet_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # User info
    user_id = Column(UUID(as_uuid=True), nullable=False)
    country = Column(String(5))
    age = Column(Integer)
    device = Column(String(50))
    ip_address = Column(String(50))

    # Event info
    sport = Column(String(50))
    league = Column(String(100))
    match = Column(String(150))
    selection = Column(String(150))
    odds = Column(Numeric(6, 2))

    # Bet info
    amount = Column(Numeric(10, 2))
    currency = Column(String(10))
    status = Column(String(20))
    payout = Column(Numeric(10, 2))
    bet_timestamp = Column(TIMESTAMP)

    # Metadata
    source = Column(String(50))
    session_id = Column(UUID(as_uuid=True))
    created_at = Column(TIMESTAMP)

    # Tech fields
    exported = Column(Boolean, default=False, nullable=False)
    raw_event = Column(JSON)  # сохраняем весь JSON для отладки

# Создаём таблицу при первом запуске
Base.metadata.create_all(engine)

# --- Consumer Logic ---
def main():
    print(f"[CONSUMER] Connecting to Kafka broker: {KAFKA_BROKER}")
    print(f"[CONSUMER] Subscribing to topic: {TOPIC_NAME}")
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=f"bets-consumer-group-{uuid.uuid4()}"
        )
        print("[CONSUMER] ✅ Connected successfully!")
    except Exception as e:
        print(f"[ERROR] Failed to connect to Kafka: {e}")
        return  

    print(f"[CONSUMER] Listening to topic: {TOPIC_NAME}")
    session = Session()
    print(f"[CONSUMER] Consumer object: {consumer}")
    
    try:
        for msg in consumer:
            if msg is None:
                print("[CONSUMER] No messages yet...")
                continue
                
            event = msg.value
            print(f"[CONSUMER] Received event: {json.dumps(event, indent=2)}")

            try:
                bet = Bet(
                    bet_id=uuid.UUID(event["bet_id"]),
                    user_id=uuid.UUID(event["user"]["user_id"]),
                    country=event["user"].get("country"),
                    age=event["user"].get("age"),
                    device=event["user"].get("device"),
                    ip_address=event["user"].get("ip_address"),

                    sport=event["event"].get("sport"),
                    league=event["event"].get("league"),
                    match=event["event"].get("match"),
                    selection=event["event"].get("selection"),
                    odds=Decimal(str(event["event"].get("odds", 0))),

                    amount=Decimal(str(event["bet"].get("amount", 0))),
                    currency=event["bet"].get("currency"),
                    status=event["bet"].get("status"),
                    payout=Decimal(str(event["bet"].get("payout", 0))),
                    bet_timestamp=datetime.fromisoformat(event["bet"]["timestamp"].replace("Z", "+00:00")),

                    source=event["metadata"].get("source"),
                    session_id=uuid.UUID(event["metadata"]["session_id"]),
                    created_at=datetime.fromisoformat(event["metadata"]["created_at"].replace("Z", "+00:00")),

                    exported=False,
                    raw_event=event
                )

                session.merge(bet)
                session.commit()
                print(f"[CONSUMER] Stored bet {bet.bet_id} in DB.")

            except Exception as e:
                session.rollback()
                print(f"[ERROR] Failed to store event: {e}")
                continue

    except KeyboardInterrupt:
        print("[CONSUMER] Shutting down...")
    except Exception as e:
        print(f"[ERROR] Consumer error: {e}")
    finally:
        consumer.close()
        session.close()

if __name__ == "__main__":
    main()

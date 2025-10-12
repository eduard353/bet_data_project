import os
import json
from datetime import datetime
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, Numeric, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# --- Конфиги Kafka ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "bets")

# --- Конфиги PostgreSQL ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "bets_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# --- SQLAlchemy setup ---
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()


# --- Модель таблицы ---
class TopGame(Base):
    __tablename__ = "top_games"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sport = Column(String, nullable=False)
    league = Column(String)
    total_amount = Column(Numeric(10, 2), nullable=True)
    timestamp = Column(TIMESTAMP, default=datetime.utcnow)
    raw_event = Column(JSON)


# --- Создаём таблицу ---
Base.metadata.create_all(engine)


# --- Основная логика ---
def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="top-games-consumer-group-v2",
    )

    print(f"[CONSUMER] Listening to topic: {TOPIC_NAME}")
    session = Session()

    for msg in consumer:
        event = msg.value
        kafka_ts = datetime.fromtimestamp(msg.timestamp / 1000.0)

        try:
            sport = event.get("event", {}).get("sport", "unknown")
            league = event.get("event", {}).get("league", None)
            amount = event.get("bet", {}).get("amount", 0)

            top_game = TopGame(
                sport=sport,
                league=league,
                total_amount=amount,
                timestamp=kafka_ts,
                raw_event=event,
            )

            session.add(top_game)
            session.commit()

            print(f"[CONSUMER] ✅ Recorded: {sport} ({league}), amount={amount}")

        except Exception as e:
            print(f"[ERROR] {e}")
            session.rollback()

    session.close()


if __name__ == "__main__":
    main()

import os
import json
import uuid 
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Column, Integer, String, Numeric, TIMESTAMP, Boolean
from sqlalchemy.dialects.postgresql import UUID 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Конфиги Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_NAME = os.getenv("TOPIC_NAME")

# Конфиги PostgreSQL
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Определяем модель
class Bet(Base):
    __tablename__ = "bets"
    bet_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    game = Column(String)
    amount = Column(Numeric)
    status = Column(String)
    timestamp = Column(TIMESTAMP)
    exported = Column(Boolean, default=False, nullable=False)

# Создаем таблицу, если не существует
Base.metadata.create_all(engine)

def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="bets-consumer-group"
    )

    print(f"[CONSUMER] Listening to topic: {TOPIC_NAME}")
    session = Session()
    for msg in consumer:
        event = msg.value
        print(f"[CONSUMER] Received: {event}")

        try:
            bet = Bet(
                bet_id=uuid.UUID(event["bet_id"]),
                user_id=uuid.UUID(event["user_id"]),
                game=event.get("game"),
                amount=event.get("amount"),
                status=event.get("status"),
                timestamp=event.get("timestamp"),
                exported=False 
            )

            session.merge(bet)  # merge позволяет избежать дубликатов по PK
            session.commit()
        except Exception as e:
            print(f"[ERROR] Failed to insert bet: {e}")
            session.rollback()

    session.close()

if __name__ == "__main__":
    main()

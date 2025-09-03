import os
import json
from datetime import datetime
from kafka import KafkaConsumer
from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Конфиги Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "top_games")

# Конфиги PostgreSQL
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "bets")
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# Определяем модель для топ-игр (с историей)
class TopGame(Base):
    __tablename__ = "top_games"
    id = Column(Integer, primary_key=True, autoincrement=True)  # уникальный ключ
    game = Column(String, nullable=False)
    count = Column(Integer, nullable=False)
    timestamp = Column(TIMESTAMP, default=datetime.utcnow)  # когда пришло событие

# Создаём таблицу, если не существует
Base.metadata.create_all(engine)

def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="top-games-consumer-group"
    )

    print(f"[CONSUMER] Listening to topic: {TOPIC_NAME}")
    session = Session()
    for msg in consumer:
        event = msg.value
        kafka_ts = datetime.fromtimestamp(msg.timestamp / 1000.0)  # преобразуем миллисекунды в datetime
        print(f"[CONSUMER] Received: {event}")

        try:
            # Записываем каждое событие как новую строку
            top_game = TopGame(
                game=event.get("game"),
                count=event.get("count", 0),
                timestamp=kafka_ts
            )

            session.add(top_game)
            session.commit()
        except Exception as e:
            print(f"[ERROR] {e}")
            session.rollback()

    session.close()

if __name__ == "__main__":
    main()

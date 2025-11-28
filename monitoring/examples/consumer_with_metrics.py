"""
Пример Consumer с метриками Prometheus
Добавьте этот код в consumer/consumers/all_bets_consumer.py для мониторинга
"""
import os
import json
import uuid
import time
from datetime import datetime
from decimal import Decimal

from kafka import KafkaConsumer
from sqlalchemy import (
    create_engine, Column, Integer, String, Numeric, TIMESTAMP, Boolean, JSON
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Prometheus метрики
messages_consumed = Counter(
    'bets_consumer_messages_consumed_total',
    'Total messages consumed from Kafka',
    ['status']
)
db_errors = Counter(
    'bets_consumer_db_errors_total',
    'Total database errors',
    ['error_type']
)
processing_duration = Histogram(
    'bets_consumer_processing_duration_seconds',
    'Message processing duration in seconds',
    ['status']
)
db_operations = Counter(
    'bets_consumer_db_operations_total',
    'Total database operations',
    ['operation', 'status']
)
consumer_lag = Gauge(
    'bets_consumer_lag_messages',
    'Consumer lag in messages'
)
consumer_uptime = Gauge(
    'bets_consumer_uptime_seconds',
    'Consumer uptime in seconds'
)
start_time = time.time()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "bets")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))

# ... остальной код (DATABASE_URL, Bet model) остается без изменений ...

def main():
    # Запуск HTTP сервера для метрик Prometheus
    start_http_server(METRICS_PORT)
    print(f"[CONSUMER] Metrics server started on port {METRICS_PORT}")
    
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
        db_errors.labels(error_type='kafka_connection').inc()
        return  

    print(f"[CONSUMER] Listening to topic: {TOPIC_NAME}")
    session = Session()
    
    try:
        for msg in consumer:
            # Обновляем uptime
            consumer_uptime.set(time.time() - start_time)
            
            if msg is None:
                print("[CONSUMER] No messages yet...")
                continue
                
            event = msg.value
            print(f"[CONSUMER] Received event: {json.dumps(event, indent=2)}")

            try:
                # Измеряем время обработки
                with processing_duration.labels(status='processing').time():
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
                    
                    # Успешная обработка
                    messages_consumed.labels(status='success').inc()
                    db_operations.labels(operation='merge', status='success').inc()
                    print(f"[CONSUMER] Stored bet {bet.bet_id} in DB.")

            except Exception as e:
                session.rollback()
                error_type = type(e).__name__
                print(f"[ERROR] Failed to store event: {e}")
                
                # Ошибка обработки
                messages_consumed.labels(status='error').inc()
                db_errors.labels(error_type=error_type).inc()
                db_operations.labels(operation='merge', status='error').inc()
                processing_duration.labels(status='error').observe(0)
                continue

    except KeyboardInterrupt:
        print("[CONSUMER] Shutting down...")
    except Exception as e:
        print(f"[ERROR] Consumer error: {e}")
        db_errors.labels(error_type='consumer_error').inc()
    finally:
        consumer.close()
        session.close()

if __name__ == "__main__":
    main()


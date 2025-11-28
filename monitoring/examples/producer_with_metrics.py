"""
Пример Producer с метриками Prometheus
Добавьте этот код в producer/bets_producer.py для мониторинга
"""
import os
import json
import time
import random
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Prometheus метрики
messages_sent = Counter(
    'bets_producer_messages_sent_total',
    'Total messages sent to Kafka',
    ['topic', 'status']
)
send_errors = Counter(
    'bets_producer_errors_total',
    'Total send errors',
    ['error_type']
)
send_duration = Histogram(
    'bets_producer_send_duration_seconds',
    'Message send duration in seconds',
    ['topic']
)
producer_uptime = Gauge(
    'bets_producer_uptime_seconds',
    'Producer uptime in seconds'
)
start_time = time.time()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "bets")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

# ... остальной код create_event() остается без изменений ...

def main():
    # Запуск HTTP сервера для метрик Prometheus
    start_http_server(METRICS_PORT)
    print(f"[PRODUCER] Metrics server started on port {METRICS_PORT}")
    
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"[PRODUCER] Sending enriched bet events to topic: {TOPIC_NAME}")
    
    while True:
        try:
            # Обновляем uptime
            producer_uptime.set(time.time() - start_time)
            
            # Создаем событие
            event = create_event()
            
            # Измеряем время отправки
            with send_duration.labels(topic=TOPIC_NAME).time():
                future = producer.send(TOPIC_NAME, event)
                # Ждем подтверждения
                record_metadata = future.get(timeout=10)
                
            # Успешная отправка
            messages_sent.labels(topic=TOPIC_NAME, status='success').inc()
            print(f"[PRODUCER] Sent: {json.dumps(event, indent=2)}")
            
        except Exception as e:
            # Ошибка отправки
            send_errors.labels(error_type=type(e).__name__).inc()
            messages_sent.labels(topic=TOPIC_NAME, status='error').inc()
            print(f"[PRODUCER] Error sending message: {e}")
        
        time.sleep(2)


if __name__ == "__main__":
    main()


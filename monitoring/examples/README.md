# Примеры интеграции метрик Prometheus

Этот каталог содержит примеры кода для добавления метрик Prometheus в ваши приложения.

## Установка зависимостей

Добавьте `prometheus-client` в ваши Dockerfile'ы:

### Producer Dockerfile

```dockerfile
FROM python:3.10-slim

WORKDIR /app

RUN pip install kafka-python prometheus-client

COPY bets_producer.py .

CMD ["python", "bets_producer.py"]
```

### Consumer Dockerfile

```dockerfile
FROM python:3.10-slim

WORKDIR /app

RUN pip install kafka-python sqlalchemy psycopg2-binary prometheus-client

COPY consumers/ ./consumers/

CMD ["python", "consumers/all_bets_consumer.py"]
```

## Обновление docker-compose.yml

Добавьте порты для метрик в ваши сервисы:

```yaml
  producer:
    build: ./producer
    container_name: producer
    ports:
      - "8000:8000"  # Метрики Prometheus
    # ... остальная конфигурация ...

  bets_consumer:
    build: ./consumer
    container_name: bets_consumer
    ports:
      - "8001:8001"  # Метрики Prometheus
    # ... остальная конфигурация ...
```

## Доступные метрики

### Producer метрики

- `bets_producer_messages_sent_total` - общее количество отправленных сообщений
- `bets_producer_errors_total` - общее количество ошибок
- `bets_producer_send_duration_seconds` - длительность отправки сообщений
- `bets_producer_uptime_seconds` - время работы producer

### Consumer метрики

- `bets_consumer_messages_consumed_total` - общее количество обработанных сообщений
- `bets_consumer_db_errors_total` - общее количество ошибок БД
- `bets_consumer_processing_duration_seconds` - длительность обработки сообщений
- `bets_consumer_db_operations_total` - операции с БД
- `bets_consumer_lag_messages` - lag consumer'а
- `bets_consumer_uptime_seconds` - время работы consumer

## Проверка метрик

После запуска сервисов, метрики будут доступны по адресам:

- Producer: http://localhost:8000/metrics
- Consumer: http://localhost:8001/metrics

Prometheus автоматически соберет эти метрики, если они добавлены в `prometheus.yml`.

## Примеры запросов для Grafana

### Producer throughput

```
rate(bets_producer_messages_sent_total[5m])
```

### Consumer processing rate

```
rate(bets_consumer_messages_consumed_total{status="success"}[5m])
```

### Error rate

```
rate(bets_producer_errors_total[5m]) + rate(bets_consumer_db_errors_total[5m])
```

### Average processing time

```
rate(bets_consumer_processing_duration_seconds_sum[5m]) / rate(bets_consumer_processing_duration_seconds_count[5m])
```


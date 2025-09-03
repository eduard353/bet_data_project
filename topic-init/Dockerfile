# topic-init/Dockerfile
FROM python:3.10-slim

# Устанавливаем зависимости (Kafka клиент для Python)
RUN pip install kafka-python

# Копируем скрипт внутрь контейнера
WORKDIR /app
COPY create_topic.py /app/

# Запуск по умолчанию (можно переопределить в docker-compose)
CMD ["python", "create_topic.py"]

# producer/Dockerfile
FROM python:3.10-slim

# Устанавливаем зависимости
RUN pip install kafka-python

# Рабочая директория
WORKDIR /app

# Копируем код продюсера
COPY bets_producer.py /app/

# Запуск по умолчанию
CMD ["python", "bets_producer.py"]

# consumer/Dockerfile
FROM python:3.10-slim

RUN pip install kafka-python psycopg2-binary sqlalchemy

WORKDIR /app

# Копируем оба консюмера
COPY consumers/all_bets_consumer.py /app/
COPY consumers/top_games_consumer.py /app/

# Можно запускать через CMD с аргументом
ARG SCRIPT=all_bets_consumer.py
CMD ["python", "${SCRIPT}"]

FROM python:3.10-slim

RUN pip install kafka-python psycopg2-binary sqlalchemy

WORKDIR /app

COPY consumers/all_bets_consumer.py /app/
COPY consumers/top_games_consumer.py /app/

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python"]
CMD ["all_bets_consumer.py"]

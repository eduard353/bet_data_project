FROM apache/airflow:2.10.3-python3.10

USER root
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow
# Установка зависимостей для работы с данными и метриками
RUN pip install --no-cache-dir boto3 pandas pyarrow sqlalchemy psycopg2-binary prometheus-client

ENTRYPOINT ["/entrypoint.sh"]

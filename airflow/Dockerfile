FROM apache/airflow:2.10.3-python3.10

USER root

# Установка зависимостей Python
RUN pip install --no-cache-dir \
    apache-airflow==2.10.3 \
    boto3 \
    pandas \
    pyarrow \
    sqlalchemy \
    psycopg2-binary

USER airflow

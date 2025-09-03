FROM python:3.10-slim

# Устанавливаем зависимости для psycopg2
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем Python-библиотеки
RUN pip install --no-cache-dir \
    psycopg2-binary \
    pandas \
    pyarrow \
    boto3 \
    sqlalchemy

# Рабочая директория
WORKDIR /app

# Копируем скрипт экспорта
COPY export_to_s3.py .

# Запуск по умолчанию
CMD ["python", "export_to_s3.py"]

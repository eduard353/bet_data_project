#!/bin/bash
set -e

airflow db init

# Проверяем, существует ли пользователь, если нет — создаём
airflow users list | grep -q "${AIRFLOW_ADMIN_USERNAME}" || \
airflow users create \
  --username "${AIRFLOW_ADMIN_USERNAME}" \
  --firstname Admin \
  --lastname Admin \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL}" \
  --password "${AIRFLOW_ADMIN_PASSWORD}"

# Запускаем scheduler в фоне и webserver в форграунде
airflow scheduler &
exec airflow webserver

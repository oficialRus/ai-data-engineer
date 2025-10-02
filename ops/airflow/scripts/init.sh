#!/bin/bash

# Инициализация Airflow
echo "Initializing Airflow..."

# Создание пользователя admin
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Настройка connections для целевых систем
echo "Setting up Airflow connections..."
python /opt/airflow/scripts/setup_connections.py

echo "Airflow initialization completed!"

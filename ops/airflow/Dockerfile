# Базируемся на slim-образе Airflow (минимальный)
FROM apache/airflow:slim-3.1.0-python3.12

# === Установка системных пакетов ===
# apt пакеты всегда ставим под root
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# === Установка Python-зависимостей ===
USER airflow

# Версия Airflow (та же, что в base image) и constraints
ARG AIRFLOW_VERSION=3.1.0
ARG PYTHON_MAJOR_MINOR=3.12
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MAJOR_MINOR}.txt

# requirements.txt должен лежать рядом с Dockerfile
COPY requirements.txt /opt/airflow/requirements.txt

# Ставим Airflow (фиксируем ту же версию) и все пакеты из requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    -r /opt/airflow/requirements.txt \
    --constraint "${CONSTRAINT_URL}"

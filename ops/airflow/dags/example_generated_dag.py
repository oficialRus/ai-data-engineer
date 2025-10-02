"""
Пример автоматически сгенерированного DAG файла
Этот файл показывает, как будет выглядеть DAG после генерации бэкендом
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import json
import logging

# Конфигурация DAG
DAG_ID = "pipe_example_123_v1"
PIPELINE_ID = "example-123"
SOURCE_TYPE = "csv"
TARGET_SYSTEM = "postgresql"
CONNECTION_ID = "pg_target"

# Настройки по умолчанию для задач
default_args = {
    'owner': 'ai-data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Создание DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description=f'Пайплайн данных: {SOURCE_TYPE} → {TARGET_SYSTEM}',
    schedule_interval='@hourly',
    max_active_runs=1,
    tags=['ai-data-engineer', 'pipeline', SOURCE_TYPE, TARGET_SYSTEM],
)

def extract_data(**context):
    """Извлечение данных из источника"""
    logging.info(f"Извлечение данных из {SOURCE_TYPE}")
    logging.info("CSV данные извлечены успешно")
    return {"status": "extracted", "rows": 100}

def normalize_data(**context):
    """Нормализация и очистка данных"""
    logging.info("Нормализация данных")
    ti = context['ti']
    extracted_data = ti.xcom_pull(task_ids='extract')
    
    normalized_data = {
        "status": "normalized",
        "rows": extracted_data.get("rows", 0),
        "transformations_applied": ["type_conversion", "null_handling", "validation"]
    }
    
    logging.info("Данные нормализованы успешно")
    return normalized_data

def load_data(**context):
    """Загрузка данных в целевую систему"""
    logging.info(f"Загрузка данных в {TARGET_SYSTEM}")
    ti = context['ti']
    normalized_data = ti.xcom_pull(task_ids='normalize')
    
    logging.info("Данные загружены в PostgreSQL успешно")
    return {"status": "loaded", "target": "postgresql", "rows": normalized_data.get("rows", 0)}

def validate_data(**context):
    """Валидация загруженных данных"""
    logging.info("Валидация загруженных данных")
    ti = context['ti']
    load_result = ti.xcom_pull(task_ids='load')
    
    validation_result = {
        "status": "validated",
        "target": load_result.get("target"),
        "rows_loaded": load_result.get("rows", 0),
        "validation_checks": {
            "row_count_check": "passed",
            "data_quality_check": "passed",
            "schema_validation": "passed"
        }
    }
    
    logging.info("Валидация завершена успешно")
    return validation_result

# Определение задач
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

normalize_task = PythonOperator(
    task_id='normalize',
    python_callable=normalize_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate',
    python_callable=validate_data,
    dag=dag,
)

# Создание DDL таблицы
create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id=CONNECTION_ID,
    sql="""
        CREATE TABLE IF NOT EXISTS imported_data (
            id SERIAL PRIMARY KEY,
            data JSONB,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    dag=dag,
)

def notify_success(**context):
    """Уведомление об успешном завершении пайплайна"""
    logging.info(f"Пайплайн {PIPELINE_ID} завершен успешно")

success_task = PythonOperator(
    task_id='notify_success',
    python_callable=notify_success,
    dag=dag,
    trigger_rule='all_success',
)

# Зависимости
create_table_task >> extract_task >> normalize_task >> load_task >> validate_task >> success_task

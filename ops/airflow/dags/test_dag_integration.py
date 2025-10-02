"""
Тестовый DAG для проверки интеграции с AI Data Engineer
Создан автоматически при создании пайплайна
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging

# Конфигурация DAG
DAG_ID = "test_ai_data_engineer_integration"
PIPELINE_ID = "test-integration-001"

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
    description='Тестовый пайплайн для проверки интеграции AI Data Engineer',
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['ai-data-engineer', 'test', 'integration'],
)

def test_extract(**context):
    """Тестовое извлечение данных"""
    logging.info("=== ТЕСТОВОЕ ИЗВЛЕЧЕНИЕ ДАННЫХ ===")
    logging.info(f"Pipeline ID: {PIPELINE_ID}")
    logging.info("Источник: CSV файл")
    logging.info("Извлечение данных завершено успешно")
    
    result = {
        "status": "success",
        "rows_extracted": 100,
        "source_type": "csv",
        "timestamp": datetime.now().isoformat()
    }
    
    logging.info(f"Результат извлечения: {result}")
    return result

def test_transform(**context):
    """Тестовая трансформация данных"""
    logging.info("=== ТЕСТОВАЯ ТРАНСФОРМАЦИЯ ДАННЫХ ===")
    
    # Получаем данные из предыдущей задачи
    ti = context['ti']
    extract_result = ti.xcom_pull(task_ids='extract')
    
    logging.info(f"Получены данные для трансформации: {extract_result}")
    
    # Выполняем трансформацию
    transformed_data = {
        "status": "success",
        "rows_transformed": extract_result.get("rows_extracted", 0),
        "transformations": ["normalize_names", "clean_data", "validate_schema"],
        "quality_score": 95.5,
        "timestamp": datetime.now().isoformat()
    }
    
    logging.info(f"Результат трансформации: {transformed_data}")
    return transformed_data

def test_load(**context):
    """Тестовая загрузка данных"""
    logging.info("=== ТЕСТОВАЯ ЗАГРУЗКА ДАННЫХ ===")
    
    # Получаем трансформированные данные
    ti = context['ti']
    transform_result = ti.xcom_pull(task_ids='transform')
    
    logging.info(f"Загружаем данные: {transform_result}")
    logging.info("Целевая система: PostgreSQL")
    
    # Имитация загрузки
    load_result = {
        "status": "success",
        "rows_loaded": transform_result.get("rows_transformed", 0),
        "target_system": "postgresql",
        "table_name": "test_imported_data",
        "load_time": datetime.now().isoformat()
    }
    
    logging.info(f"Результат загрузки: {load_result}")
    return load_result

def test_validate(**context):
    """Тестовая валидация данных"""
    logging.info("=== ТЕСТОВАЯ ВАЛИДАЦИЯ ДАННЫХ ===")
    
    # Получаем результат загрузки
    ti = context['ti']
    load_result = ti.xcom_pull(task_ids='load')
    
    logging.info(f"Валидируем загруженные данные: {load_result}")
    
    # Выполняем валидацию
    validation_result = {
        "status": "success",
        "validation_passed": True,
        "checks": {
            "row_count_check": "✅ PASSED",
            "data_quality_check": "✅ PASSED", 
            "schema_validation": "✅ PASSED",
            "integrity_check": "✅ PASSED"
        },
        "summary": {
            "total_rows": load_result.get("rows_loaded", 0),
            "quality_score": 95.5,
            "validation_time": datetime.now().isoformat()
        }
    }
    
    logging.info(f"Результат валидации: {validation_result}")
    return validation_result

def test_notification(**context):
    """Тестовое уведомление об успешном завершении"""
    logging.info("=== УВЕДОМЛЕНИЕ О ЗАВЕРШЕНИИ ===")
    logging.info(f"Пайплайн {PIPELINE_ID} успешно завершен!")
    
    ti = context['ti']
    validation_result = ti.xcom_pull(task_ids='validate')
    
    logging.info(f"Окончательный результат: {validation_result}")
    logging.info("✅ Пайплайн завершен успешно - данные готовы к использованию!")

# Создание таблицы (если нужно)
create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='pg_target',
    sql="""
        CREATE TABLE IF NOT EXISTS test_imported_data (
            id SERIAL PRIMARY KEY,
            source_data JSONB,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            quality_score NUMERIC(5,2)
        );
        
        -- Создание индексов для оптимизации
        CREATE INDEX IF NOT EXISTS idx_test_imported_data_processed_at 
        ON test_imported_data(processed_at);
    """,
    dag=dag,
)

# Определение задач
extract_task = PythonOperator(
    task_id='extract',
    python_callable=test_extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=test_transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=test_load,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate',
    python_callable=test_validate,
    dag=dag,
)

notification_task = PythonOperator(
    task_id='notification',
    python_callable=test_notification,
    dag=dag,
    trigger_rule='all_success',
)

# Определяем зависимости между задачами
create_table_task >> extract_task >> transform_task >> load_task >> validate_task >> notification_task

# Дополнительная информация для отладки
logging.info(f"DAG '{DAG_ID}' создан успешно")
logging.info(f"Pipeline ID: {PIPELINE_ID}")
logging.info("Задачи: create_table -> extract -> transform -> load -> validate -> notification")
logging.info("Расписание: ежедневно в полночь")

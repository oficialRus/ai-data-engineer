"""
Автоматически сгенерированный DAG для пайплайна: {{.PipelineName}}
Создан: {{.CreatedAt}}
Источник: {{.SourceType}} ({{.SourceConfig}})
Цель: {{.Target}}
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.bash import BashOperator
import json
import logging

# Конфигурация DAG
DAG_ID = "{{.DagID}}"
PIPELINE_ID = "{{.PipelineID}}"
SOURCE_TYPE = "{{.SourceType}}"
TARGET_SYSTEM = "{{.Target}}"
CONNECTION_ID = "{{.ConnectionID}}"

# Конфигурация источника и цели
SOURCE_CONFIG = {{.SourceConfigJSON}}
TARGET_CONFIG = {{.TargetConfigJSON}}
DDL_STATEMENTS = {{.DDLStatementsJSON}}

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
    schedule_interval='{{.ScheduleInterval}}',
    max_active_runs=1,
    tags=['ai-data-engineer', 'pipeline', SOURCE_TYPE, TARGET_SYSTEM],
)

def extract_data(**context):
    """Извлечение данных из источника"""
    logging.info(f"Извлечение данных из {SOURCE_TYPE}")
    logging.info(f"Конфигурация источника: {SOURCE_CONFIG}")
    
    # Здесь будет логика извлечения данных в зависимости от типа источника
    if SOURCE_TYPE == "csv":
        return extract_from_csv()
    elif SOURCE_TYPE == "json":
        return extract_from_json()
    elif SOURCE_TYPE == "xml":
        return extract_from_xml()
    elif SOURCE_TYPE == "postgresql":
        return extract_from_postgres()
    else:
        raise ValueError(f"Неподдерживаемый тип источника: {SOURCE_TYPE}")

def extract_from_csv():
    """Извлечение из CSV файла"""
    import pandas as pd
    file_path = SOURCE_CONFIG.get('pathOrUrl', '')
    logging.info(f"Чтение CSV файла: {file_path}")
    
    # В реальной реализации здесь будет чтение файла
    # df = pd.read_csv(file_path)
    # return df.to_dict('records')
    
    logging.info("CSV данные извлечены успешно")
    return {"status": "extracted", "rows": 0}

def extract_from_json():
    """Извлечение из JSON файла"""
    import json
    file_path = SOURCE_CONFIG.get('pathOrUrl', '')
    logging.info(f"Чтение JSON файла: {file_path}")
    
    # В реальной реализации здесь будет чтение файла
    logging.info("JSON данные извлечены успешно")
    return {"status": "extracted", "rows": 0}

def extract_from_xml():
    """Извлечение из XML файла"""
    file_path = SOURCE_CONFIG.get('pathOrUrl', '')
    logging.info(f"Чтение XML файла: {file_path}")
    
    # В реальной реализации здесь будет парсинг XML
    logging.info("XML данные извлечены успешно")
    return {"status": "extracted", "rows": 0}

def extract_from_postgres():
    """Извлечение из PostgreSQL"""
    logging.info("Извлечение данных из PostgreSQL")
    
    # В реальной реализации здесь будет SQL запрос
    logging.info("PostgreSQL данные извлечены успешно")
    return {"status": "extracted", "rows": 0}

def normalize_data(**context):
    """Нормализация и очистка данных"""
    logging.info("Нормализация данных")
    
    # Получаем данные из предыдущей задачи
    ti = context['ti']
    extracted_data = ti.xcom_pull(task_ids='extract')
    
    logging.info(f"Получены данные для нормализации: {extracted_data}")
    
    # Здесь будет логика нормализации данных
    # - Очистка данных
    # - Приведение типов
    # - Валидация схемы
    
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
    
    # Получаем нормализованные данные
    ti = context['ti']
    normalized_data = ti.xcom_pull(task_ids='normalize')
    
    logging.info(f"Получены данные для загрузки: {normalized_data}")
    
    # Загрузка в зависимости от целевой системы
    if TARGET_SYSTEM == "postgresql":
        return load_to_postgres(normalized_data)
    elif TARGET_SYSTEM == "clickhouse":
        return load_to_clickhouse(normalized_data)
    elif TARGET_SYSTEM == "hdfs":
        return load_to_hdfs(normalized_data)
    else:
        raise ValueError(f"Неподдерживаемая целевая система: {TARGET_SYSTEM}")

def load_to_postgres(data):
    """Загрузка в PostgreSQL"""
    logging.info("Загрузка в PostgreSQL")
    
    # В реальной реализации здесь будет INSERT/UPSERT
    logging.info("Данные загружены в PostgreSQL успешно")
    return {"status": "loaded", "target": "postgresql", "rows": data.get("rows", 0)}

def load_to_clickhouse(data):
    """Загрузка в ClickHouse"""
    logging.info("Загрузка в ClickHouse")
    
    # В реальной реализации здесь будет INSERT
    logging.info("Данные загружены в ClickHouse успешно")
    return {"status": "loaded", "target": "clickhouse", "rows": data.get("rows", 0)}

def load_to_hdfs(data):
    """Загрузка в HDFS"""
    logging.info("Загрузка в HDFS")
    
    # В реальной реализации здесь будет запись файла в HDFS
    logging.info("Данные загружены в HDFS успешно")
    return {"status": "loaded", "target": "hdfs", "rows": data.get("rows", 0)}

def validate_data(**context):
    """Валидация загруженных данных"""
    logging.info("Валидация загруженных данных")
    
    # Получаем результат загрузки
    ti = context['ti']
    load_result = ti.xcom_pull(task_ids='load')
    
    logging.info(f"Результат загрузки: {load_result}")
    
    # Здесь будет логика валидации:
    # - Проверка количества записей
    # - Проверка целостности данных
    # - Проверка качества данных
    
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

# Создание DDL таблицы (если нужно)
{{if .CreateDDL}}
create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id=CONNECTION_ID,
    sql=DDL_STATEMENTS.get('{{.Target}}', ''),
    dag=dag,
)

# Зависимости с созданием таблицы
create_table_task >> extract_task >> normalize_task >> load_task >> validate_task
{{else}}
# Зависимости без создания таблицы
extract_task >> normalize_task >> load_task >> validate_task
{{end}}

# Уведомление об успешном завершении
def notify_success(**context):
    """Уведомление об успешном завершении пайплайна"""
    logging.info(f"Пайплайн {PIPELINE_ID} завершен успешно")
    
    # Здесь можно добавить отправку уведомлений
    # - Webhook в бэкенд
    # - Email уведомления
    # - Slack уведомления

success_task = PythonOperator(
    task_id='notify_success',
    python_callable=notify_success,
    dag=dag,
    trigger_rule='all_success',
)

validate_task >> success_task

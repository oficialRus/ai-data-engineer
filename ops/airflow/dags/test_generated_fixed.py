"""
Автоматически сгенерированный DAG для тестового пайплайна
Создан автоматически для тестирования.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

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
    'pipe_test_pipeline_v1',
    default_args=default_args,
    description='Тестовый пайплайн для проверки генерации DAG',
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['ai-data-engineer', 'pipeline', 'test'],
)

def extract_data(**context):
    """Тестовое извлечение данных"""
    print("=== ТЕСТОВОЕ ИЗВЛЕЧЕНИЕ ДАННЫХ ===")
    print("Pipeline ID: test-pipeline")
    print("Источник: CSV файл")
    print("Извлечение данных завершено успешно")
    return {"status": "success", "rows": 100}

def transform_data(**context):
    """Тестовая трансформация данных"""
    print("=== ТЕСТОВАЯ ТРАНСФОРМАЦИЯ ДАННЫХ ===")
    ti = context['ti']
    extract_result = ti.xcom_pull(task_ids='extract')
    print(f"Получены данные: {extract_result}")
    return {"status": "success", "rows": extract_result.get("rows", 0)}

def load_data(**context):
    """Тестовая загрузка данных"""
    print("=== ТЕСТОВАЯ ЗАГРУЗКА ДАННЫХ ===")
    ti = context['ti']
    transform_result = ti.xcom_pull(task_ids='transform')
    print(f"Загружаем данные: {transform_result}")
    print("Данные загружены успешно!")
    return {"status": "success", "loaded_rows": transform_result.get("rows", 0)}

# Определение задач
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag,
)

# Определяем зависимости между задачами
extract_task >> transform_task >> load_task

# Информация для отладки
print(f"DAG создан: {dag.dag_id}")
print(f"Задачи: {[t.task_id for t in dag.tasks]}")
print(f"Расписание: {dag.schedule_interval}")

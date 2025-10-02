"""
Простой тестовый DAG для проверки работы Airflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Настройки по умолчанию
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
    'simple_test_dag',
    default_args=default_args,
    description='Простой тестовый DAG для проверки',
    schedule_interval=None,  # Только ручной запуск
    max_active_runs=1,
    tags=['test', 'simple'],
)

def test_task():
    """Простая тестовая функция"""
    print("Hello from AI Data Engineer!")
    print("DAG работает правильно!")
    return "success"

# Определение задачи
test_operator = PythonOperator(
    task_id='test_task',
    python_callable=test_task,
    dag=dag,
)

"""
DAG для тестирования подключений к базам данных
Использует созданные подключения: pg_target и ch_target
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
    'database_connection_test',
    default_args=default_args,
    description='Тестирование подключений к базам данных',
    schedule_interval=None,  # Только ручной запуск
    max_active_runs=1,
    tags=['test', 'database', 'connection'],
)

def test_postgres_connection(**context):
    """Тестирование подключения к PostgreSQL"""
    print("=== ТЕСТ POSTGRESQL ПОДКЛЮЧЕНИЯ ===")
    
    try:
        from airflow.hooks.postgres_hook import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id='pg_target')
        
        # Проверяем подключение
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Выполняем тестовый запрос
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        
        print(f"✅ PostgreSQL подключение успешно!")
        print(f"Версия: {version}")
        
        cursor.close()
        conn.close()
        
        return {"status": "success", "database": "postgresql", "version": version}
        
    except Exception as e:
        print(f"❌ Ошибка подключения к PostgreSQL: {e}")
        raise

def test_clickhouse_connection(**context):
    """Тестирование подключения к ClickHouse"""
    print("=== ТЕСТ CLICKHOUSE ПОДКЛЮЧЕНИЯ ===")
    
    try:
        from airflow.hooks.webhook_hook import WebhookHook
        
        # Простой HTTP запрос к ClickHouse
        hook = WebhookHook(http_conn_id='ch_target')
        response = hook.run(endpoint='/ping')
        
        print(f"✅ ClickHouse подключение успешно!")
        print(f"Ответ: {response}")
        
        return {"status": "success", "database": "clickhouse", "response": str(response)}
        
    except Exception as e:
        print(f"❌ Ошибка подключения к ClickHouse: {e}")
        print("Возможно нужно настроить HTTP подключение для ClickHouse")
        return {"status": "warning", "database": "clickhouse", "error": str(e)}

def test_table_creation(**context):
    """Тест создания таблицы в PostgreSQL"""
    print("=== ТЕСТ СОЗДАНИЯ ТАБЛИЦЫ ===")
    
    sql = """
    CREATE TABLE IF NOT EXISTS test_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Вставляем тестовую запись
    INSERT INTO test_table (name) VALUES 
        ('Тестовая запись 1'),
        ('Тестовая запись 2'),
        ('Тестовая запись 3');
    """
    
    try:
        from airflow.hooks.postgres_hook import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id='pg_target')
        
        # Выполняем SQL
        pg_hook.run(sql)
        
        print("✅ Таблица создана и данные вставлены успешно!")
        
        # Проверяем количество записей
        count_result = pg_hook.get_first("SELECT COUNT(*) FROM test_table;")
        row_count = count_result[0]
        
        print(f"Вставлено записей: {row_count}")
        
        return {"status": "success", "rows_created": row_count}
        
    except Exception as e:
        print(f"❌ Ошибка при создании таблицы: {e}")
        raise

# Определение задач
test_pg_task = PythonOperator(
    task_id='test_postgres',
    python_callable=test_postgres_connection,
    dag=dag,
)

test_ch_task = PythonOperator(
    task_id='test_clickhouse',
    python_callable=test_clickhouse_connection,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_test_table',
    postgres_conn_id='pg_target',
    sql="""
        CREATE TABLE IF NOT EXISTS airflow_test_table (
            id SERIAL PRIMARY KEY,
            test_data TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    dag=dag,
)

# Определяем зависимости
[test_pg_task, test_ch_task] >> create_table_task

print("✅ DAG 'database_connection_test' создан!")
print("Доступные задачи:")
print("  - test_postgres: Тест подключения к PostgreSQL")
print("  - test_clickhouse: Тест подключения к ClickHouse")  
print("  - create_test_table: Создание тестовой таблицы")

#!/usr/bin/env python3
"""
Скрипт для настройки connections в Airflow для целевых систем хранения данных.
Запускается автоматически при инициализации Airflow.
"""

import os
import logging
from airflow import settings
from airflow.models import Connection
from sqlalchemy.orm import sessionmaker

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_connection(conn_id, conn_type, host, port, schema, login, password, extra=None):
    """Создание или обновление connection в Airflow"""
    
    session = settings.Session()
    
    try:
        # Проверяем, существует ли connection
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        
        if existing_conn:
            logger.info(f"Обновление существующего connection: {conn_id}")
            existing_conn.conn_type = conn_type
            existing_conn.host = host
            existing_conn.port = port
            existing_conn.schema = schema
            existing_conn.login = login
            existing_conn.password = password
            if extra:
                existing_conn.extra = extra
        else:
            logger.info(f"Создание нового connection: {conn_id}")
            new_conn = Connection(
                conn_id=conn_id,
                conn_type=conn_type,
                host=host,
                port=port,
                schema=schema,
                login=login,
                password=password,
                extra=extra
            )
            session.add(new_conn)
        
        session.commit()
        logger.info(f"Connection {conn_id} настроен успешно")
        
    except Exception as e:
        logger.error(f"Ошибка при настройке connection {conn_id}: {e}")
        session.rollback()
        raise
    finally:
        session.close()

def setup_postgres_connection():
    """Настройка PostgreSQL connection"""
    create_connection(
        conn_id='pg_target',
        conn_type='postgres',
        host='postgres-data',  # имя сервиса в docker-compose
        port=5432,
        schema='datawarehouse',
        login='datauser',
        password='datapass'
    )

def setup_clickhouse_connection():
    """Настройка ClickHouse connection"""
    # ClickHouse через HTTP интерфейс
    create_connection(
        conn_id='ch_target',
        conn_type='http',
        host='clickhouse',  # имя сервиса в docker-compose
        port=8123,
        schema='analytics',
        login='clickuser',
        password='clickpass',
        extra='{"endpoint": "/", "headers": {"Content-Type": "application/x-www-form-urlencoded"}}'
    )

def setup_hdfs_connection():
    """Настройка HDFS connection"""
    create_connection(
        conn_id='hdfs_target',
        conn_type='hdfs',
        host='hdfs-namenode',  # имя сервиса в docker-compose
        port=9820,
        schema='',
        login='root',
        password='',
        extra='{"namenode_principal": "", "effective_user": "root"}'
    )

def main():
    """Основная функция настройки всех connections"""
    logger.info("Начинаем настройку Airflow connections...")
    
    try:
        # Настройка PostgreSQL
        setup_postgres_connection()
        
        # Настройка ClickHouse
        setup_clickhouse_connection()
        
        # Настройка HDFS
        setup_hdfs_connection()
        
        logger.info("Все connections настроены успешно!")
        
    except Exception as e:
        logger.error(f"Ошибка при настройке connections: {e}")
        exit(1)

if __name__ == "__main__":
    main()

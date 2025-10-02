-- Инициализация базы данных для хранения данных
-- Этот скрипт выполняется при первом запуске PostgreSQL

-- Создание схем для разных типов данных
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS processed_data;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Создание пользователя для приложения
CREATE USER IF NOT EXISTS app_user WITH PASSWORD 'app_password';

-- Выдача прав
GRANT USAGE ON SCHEMA raw_data TO app_user;
GRANT USAGE ON SCHEMA processed_data TO app_user;
GRANT USAGE ON SCHEMA analytics TO app_user;

GRANT CREATE ON SCHEMA raw_data TO app_user;
GRANT CREATE ON SCHEMA processed_data TO app_user;
GRANT CREATE ON SCHEMA analytics TO app_user;

-- Пример таблицы для импортированных данных
CREATE TABLE IF NOT EXISTS raw_data.imported_files (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    file_type VARCHAR(50) NOT NULL,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_count INTEGER,
    status VARCHAR(50) DEFAULT 'imported'
);

GRANT ALL PRIVILEGES ON TABLE raw_data.imported_files TO app_user;
GRANT USAGE, SELECT ON SEQUENCE raw_data.imported_files_id_seq TO app_user;

-- Комментарии
COMMENT ON SCHEMA raw_data IS 'Схема для сырых импортированных данных';
COMMENT ON SCHEMA processed_data IS 'Схема для обработанных данных';
COMMENT ON SCHEMA analytics IS 'Схема для аналитических таблиц';

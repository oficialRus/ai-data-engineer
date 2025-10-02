# Pipeline Display System - Полное отображение пайплайнов

## 🎯 Проблема решена!

Теперь у вас есть **полноценная система отображения пайплайнов** с хранением в базе данных PostgreSQL.

## ✅ Что реализовано:

### 1. **Хранение пайплайнов в базе данных**
- Таблица `pipelines` в PostgreSQL
- Автоматическое сохранение при создании
- Полная информация о каждом пайплайне

### 2. **Новые API endpoints**
- **GET `/api/pipelines`** - список всех пайплайнов
- **GET `/api/pipelines/detail?id=X`** - детали конкретного пайплайна
- Данные берутся из базы данных, а не из Airflow

### 3. **Автоматическая инициализация**
- Таблицы создаются автоматически при запуске
- Индексы для быстрого поиска
- Логирование всех операций

## 📋 Структура данных пайплайна

### В базе данных хранится:
```sql
CREATE TABLE pipelines (
    id VARCHAR(255) PRIMARY KEY,           -- UUID пайплайна
    name VARCHAR(500) NOT NULL,            -- Имя пайплайна
    source_type VARCHAR(50) NOT NULL,      -- Тип источника (csv, json, xml, postgresql)
    source JSONB NOT NULL,                 -- Конфигурация источника
    target VARCHAR(50),                    -- Целевая система (postgresql, clickhouse, hdfs)
    ddl JSONB NOT NULL,                    -- DDL для всех целевых систем
    schedule JSONB NOT NULL,               -- Расписание выполнения
    status VARCHAR(50) DEFAULT 'created',  -- Статус (created, ready, dag_error, running)
    created_at TIMESTAMP WITH TIME ZONE,  -- Время создания
    updated_at TIMESTAMP WITH TIME ZONE,  -- Время обновления
    dag_file_path TEXT,                    -- Путь к сгенерированному DAG файлу
    last_run TIMESTAMP WITH TIME ZONE,    -- Время последнего запуска
    run_count INTEGER DEFAULT 0           -- Количество запусков
);
```

### Статусы пайплайна:
- **`created`** - пайплайн создан, DAG генерируется
- **`ready`** - DAG сгенерирован успешно, готов к запуску
- **`dag_error`** - ошибка при генерации DAG файла
- **`running`** - пайплайн выполняется
- **`completed`** - выполнение завершено

## 🚀 Использование

### 1. Создание пайплайна
```bash
curl -X POST http://localhost:8081/api/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "sourceType": "csv",
    "source": {
      "kind": "file",
      "type": "csv",
      "pathOrUrl": "data.csv"
    },
    "target": "postgresql",
    "ddl": {
      "clickhouse": "CREATE TABLE...",
      "postgresql": "CREATE TABLE...",
      "hdfs": "CREATE TABLE..."
    },
    "schedule": {
      "cron": "0 2 * * *",
      "incrementalMode": "none",
      "incrementalColumn": ""
    }
  }'
```

**Ответ:**
```json
{
  "id": "b057a02a-cda0-4ea0-94b1-48258f4d1887"
}
```

### 2. Просмотр всех пайплайнов
```bash
curl http://localhost:8081/api/pipelines
```

**Ответ:**
```json
{
  "pipelines": [
    {
      "id": "b057a02a-cda0-4ea0-94b1-48258f4d1887",
      "name": "Pipeline_b057a02a",
      "source_type": "csv",
      "target": "postgresql",
      "status": "ready",
      "created_at": "2025-01-01T10:00:00Z",
      "last_run": null,
      "run_count": 0
    }
  ],
  "total": 1
}
```

### 3. Детали конкретного пайплайна
```bash
curl "http://localhost:8081/api/pipelines/detail?id=b057a02a-cda0-4ea0-94b1-48258f4d1887"
```

**Ответ:**
```json
{
  "id": "b057a02a-cda0-4ea0-94b1-48258f4d1887",
  "name": "Pipeline_b057a02a",
  "source_type": "csv",
  "source": {
    "kind": "file",
    "type": "csv",
    "pathOrUrl": "data.csv"
  },
  "target": "postgresql",
  "ddl": {
    "clickhouse": "CREATE TABLE...",
    "postgresql": "CREATE TABLE...",
    "hdfs": "CREATE TABLE..."
  },
  "schedule": {
    "cron": "0 2 * * *",
    "incrementalMode": "none"
  },
  "status": "ready",
  "created_at": "2025-01-01T10:00:00Z",
  "updated_at": "2025-01-01T10:00:05Z",
  "dag_file_path": "ops/airflow/dags/pipe_b057a02a_cda0_4ea0_94b1_48258f4d1887_v1.py",
  "last_run": null,
  "run_count": 0
}
```

## 🔧 Что происходит при создании пайплайна

1. **Генерируется UUID** для пайплайна
2. **Сохраняется в базе данных** со статусом `created`
3. **Генерируется DAG файл** в `ops/airflow/dags/`
4. **Обновляется статус** на `ready` или `dag_error`
5. **Сохраняется путь к DAG файлу**
6. **Запускается фоновая обработка** через WebSocket

## 📊 Мониторинг

### Логи создания пайплайна:
```
[PIPELINE] Creating pipeline with ID: b057a02a-cda0-4ea0-94b1-48258f4d1887
[PIPELINE_STORAGE] Saving pipeline: b057a02a-cda0-4ea0-94b1-48258f4d1887
[PIPELINE] Pipeline saved to database: b057a02a-cda0-4ea0-94b1-48258f4d1887
[DAG-GEN] Starting DAG generation for pipeline b057a02a-cda0-4ea0-94b1-48258f4d1887
[DAG-GEN] DAG generation completed successfully
[PIPELINE_STORAGE] Pipeline status updated successfully: b057a02a-cda0-4ea0-94b1-48258f4d1887
```

### Логи получения пайплайнов:
```
[GET_PIPELINES] Fetching pipelines from database
[PIPELINE_STORAGE] Retrieved 5 pipelines
[GET_PIPELINES] SUCCESS: Found 5 pipelines
```

## 🛠️ Настройка базы данных

### Автоматическая инициализация:
При запуске сервера автоматически:
1. ✅ Создается таблица `pipelines`
2. ✅ Создаются индексы для производительности
3. ✅ Логируется процесс инициализации

### Ручная проверка:
```sql
-- Подключение к PostgreSQL
psql -h localhost -p 5433 -U datauser -d datawarehouse

-- Просмотр таблицы пайплайнов
SELECT id, name, source_type, target, status, created_at FROM pipelines;

-- Статистика по статусам
SELECT status, COUNT(*) FROM pipelines GROUP BY status;
```

## 🎯 Преимущества новой системы

### ✅ **Надежность:**
- Данные хранятся в PostgreSQL
- Не зависит от состояния Airflow
- Автоматическое восстановление при перезапуске

### ✅ **Производительность:**
- Быстрые запросы с индексами
- Кэширование не требуется
- Масштабируемость PostgreSQL

### ✅ **Функциональность:**
- Полная история пайплайнов
- Статусы и метрики
- Поиск и фильтрация

### ✅ **Интеграция:**
- Совместимость с Airflow
- WebSocket уведомления
- REST API

## 🚨 Устранение неполадок

### Проблема: Пайплайны не отображаются
**Решение:**
```bash
# Проверить подключение к базе данных
curl http://localhost:8081/api/database/status

# Создать образцы данных если нужно
curl -X POST http://localhost:8081/api/database/init-sample-data

# Проверить логи сервера
# Должны быть сообщения [PIPELINE_STORAGE] при запуске
```

### Проблема: Ошибка при создании пайплайна
**Проверить:**
1. PostgreSQL запущен и доступен
2. Таблица `pipelines` создана
3. Права доступа к базе данных

### Проблема: Статус пайплайна `dag_error`
**Причины:**
- Ошибка в шаблоне DAG
- Недоступна директория `ops/airflow/dags/`
- Неправильные параметры пайплайна

## 🎉 Готово!

Теперь у вас есть **полноценная система управления пайплайнами**:

✅ **Создание** пайплайнов через API  
✅ **Хранение** в PostgreSQL  
✅ **Отображение** списка и деталей  
✅ **Статусы** и метрики  
✅ **Интеграция** с Airflow  
✅ **Логирование** всех операций  

Пайплайн `b057a02a-cda0-4ea0-94b1-48258f4d1887` теперь будет корректно отображаться! 🚀

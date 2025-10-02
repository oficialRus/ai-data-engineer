# API Integration Guide - Airflow & Database Integration

## 🚀 Новые возможности

Ваш бэкенд теперь полностью интегрирован с Airflow и базами данных! Добавлены следующие возможности:

### ✅ Что реализовано:

1. **Интеграция с Airflow API** - полное управление пайплайнами
2. **Подключения к базам данных** - PostgreSQL и ClickHouse
3. **Отображение созданных пайплайнов** - список и детали
4. **Запуск пайплайнов** - триггер выполнения через API
5. **Мониторинг статуса** - проверка подключений и таблиц

## 📋 Новые API Endpoints

### 1. Управление пайплайнами

#### GET `/api/pipelines` - Список всех пайплайнов
Возвращает список всех созданных пайплайнов из Airflow.

**Ответ:**
```json
{
  "pipelines": [
    {
      "id": "pipe_12345678_v1",
      "name": "pipe_12345678_v1", 
      "description": "Generated pipeline",
      "is_paused": false,
      "is_active": true,
      "last_parsed": "2025-01-01T10:00:00Z",
      "runs_count": 3,
      "last_run": {
        "dag_run_id": "manual_1704110400",
        "state": "success",
        "execution_date": "2025-01-01T10:00:00Z",
        "start_date": "2025-01-01T10:01:00Z",
        "end_date": "2025-01-01T10:05:00Z"
      }
    }
  ],
  "total": 1
}
```

#### GET `/api/pipelines/detail?id=pipe_12345678_v1` - Детали пайплайна
Возвращает подробную информацию о конкретном пайплайне.

**Параметры:**
- `id` (query) - ID пайплайна

**Ответ:**
```json
{
  "id": "pipe_12345678_v1",
  "name": "pipe_12345678_v1",
  "description": "Generated pipeline", 
  "is_paused": false,
  "is_active": true,
  "runs": [
    {
      "dag_run_id": "manual_1704110400",
      "state": "success",
      "execution_date": "2025-01-01T10:00:00Z",
      "start_date": "2025-01-01T10:01:00Z", 
      "end_date": "2025-01-01T10:05:00Z"
    }
  ],
  "runs_count": 1
}
```

#### POST `/api/pipelines/trigger` - Запуск пайплайна
Запускает выполнение пайплайна в Airflow.

**Тело запроса:**
```json
{
  "pipeline_id": "pipe_12345678_v1",
  "config": {
    "custom_param": "value"
  }
}
```

**Ответ:**
```json
{
  "message": "Пайплайн успешно запущен",
  "pipeline_id": "pipe_12345678_v1",
  "run_id": "manual_1704110400",
  "state": "queued"
}
```

### 2. Управление базами данных

#### GET `/api/database/status` - Статус баз данных
Проверяет подключения к базам данных и возвращает информацию о таблицах.

**Ответ:**
```json
{
  "connections": [
    {
      "type": "postgresql",
      "host": "localhost:5433",
      "database": "datawarehouse",
      "connected": true
    },
    {
      "type": "clickhouse", 
      "host": "localhost:8123",
      "database": "analytics",
      "connected": true
    }
  ],
  "tables": [
    {
      "name": "users",
      "schema": "public",
      "row_count": 100,
      "column_count": 5
    }
  ],
  "timestamp": "2025-01-01T10:00:00Z"
}
```

#### POST `/api/database/init-sample-data` - Создание образцов данных
Создает тестовые таблицы и данные в PostgreSQL.

**Ответ:**
```json
{
  "message": "Образцы данных успешно созданы",
  "timestamp": "2025-01-01T10:00:00Z"
}
```

## 🔧 Конфигурация

### Переменные окружения

Добавьте в ваш `.env` файл или переменные окружения:

```bash
# База данных PostgreSQL (для пользовательских данных)
POSTGRESQL_DATA_DSN=postgres://datauser:datapass@localhost:5433/datawarehouse?sslmode=disable

# ClickHouse
CLICKHOUSE_URL=http://localhost:8123
CLICKHOUSE_USER=clickuser
CLICKHOUSE_PASS=clickpass
CLICKHOUSE_DB=analytics

# Airflow
AIRFLOW_BASE_URL=http://localhost:8080
AIRFLOW_USERNAME=airflow
AIRFLOW_PASSWORD=airflow
AIRFLOW_TIMEOUT_SEC=30
```

### Автоматическая настройка

При запуске сервер автоматически:
1. ✅ Настраивает подключения в Airflow (`pg_target`, `ch_target`)
2. ✅ Проверяет доступность баз данных
3. ✅ Логирует статус всех подключений

## 🎯 Использование

### 1. Создание пайплайна
```bash
# Создать пайплайн (существующий endpoint)
curl -X POST http://localhost:8081/api/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "sourceType": "csv",
    "source": {...},
    "target": "postgresql",
    "ddl": {...},
    "schedule": {...}
  }'
```

### 2. Просмотр пайплайнов
```bash
# Получить список всех пайплайнов
curl http://localhost:8081/api/pipelines

# Получить детали конкретного пайплайна
curl "http://localhost:8081/api/pipelines/detail?id=pipe_12345678_v1"
```

### 3. Запуск пайплайна
```bash
# Запустить пайплайн
curl -X POST http://localhost:8081/api/pipelines/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_id": "pipe_12345678_v1"
  }'
```

### 4. Проверка баз данных
```bash
# Проверить статус баз данных
curl http://localhost:8081/api/database/status

# Создать образцы данных
curl -X POST http://localhost:8081/api/database/init-sample-data
```

## 🔍 Мониторинг

### Логирование
Все операции подробно логируются с префиксами:
- `[AIRFLOW]` - операции с Airflow API
- `[DATABASE]` - операции с базами данных
- `[GET_PIPELINES]` - получение списка пайплайнов
- `[TRIGGER_PIPELINE]` - запуск пайплайнов

### Проверка статуса
```bash
# Проверить что сервер запущен
curl http://localhost:8081/api/database/status

# Проверить Airflow UI
open http://localhost:8080

# Проверить ClickHouse
curl http://localhost:8123/ping
```

## 🚨 Устранение неполадок

### Проблемы с Airflow
1. Убедитесь что Airflow запущен: `docker-compose ps`
2. Проверьте логи: `docker-compose logs airflow-apiserver`
3. Проверьте доступность: `curl http://localhost:8080/health`

### Проблемы с базами данных
1. PostgreSQL: `docker-compose logs postgres-data`
2. ClickHouse: `docker-compose logs clickhouse`
3. Проверьте порты: `netstat -an | findstr "5433\|8123"`

### Проблемы с подключениями
1. Проверьте переменные окружения
2. Убедитесь что контейнеры в одной сети
3. Проверьте файрволл и антивирус

## 🎉 Готово!

Теперь ваш бэкенд полностью интегрирован с Airflow и базами данных. Вы можете:

✅ Создавать пайплайны через API  
✅ Просматривать список созданных пайплайнов  
✅ Запускать пайплайны по требованию  
✅ Мониторить статус выполнения  
✅ Работать с PostgreSQL и ClickHouse  
✅ Проверять подключения к базам данных  

Все готово для продакшена! 🚀

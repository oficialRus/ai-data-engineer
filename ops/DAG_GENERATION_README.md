# Генерация DAG файлов для Airflow

Система автоматической генерации DAG файлов для Airflow на основе созданных пайплайнов.

## Как это работает

### 1. Создание пайплайна
При создании пайплайна через API `/api/pipelines`, бэкенд:

1. **Парсит запрос** - извлекает конфигурацию источника, цели, расписание
2. **Генерирует DAG ID** - формат: `pipe_{pipeline_id}_v{schema_version}`
3. **Рендерит шаблон** - подставляет данные в шаблон DAG
4. **Сохраняет файл** - в папку `ops/airflow/dags/`

### 2. Структура DAG

Каждый сгенерированный DAG содержит задачи:

```
create_table (если нужно) → extract → normalize → load → validate → notify_success
```

#### Задачи:
- **create_table** - создание таблицы в целевой системе (опционально)
- **extract** - извлечение данных из источника
- **normalize** - нормализация и очистка данных
- **load** - загрузка в целевую систему
- **validate** - валидация загруженных данных
- **notify_success** - уведомление об успешном завершении

### 3. Конфигурация

#### DAG ID
```
pipe_{pipeline_id}_v{schema_version}
```
Пример: `pipe_abc123def_v1`

#### Connection IDs
- **PostgreSQL**: `pg_target`
- **ClickHouse**: `ch_target`  
- **HDFS**: `hdfs_target`

#### Расписание
Берется из поля `schedule.cron` запроса:
- `@hourly` - каждый час
- `@daily` - каждый день
- `0 */6 * * *` - каждые 6 часов
- `0 0 * * 1` - каждый понедельник

## Файлы и компоненты

### Бэкенд
- `backend/templates/dag_template.py` - шаблон DAG файла
- `backend/internal/usecase/dag_generator.go` - сервис генерации DAG
- Интеграция в `PipelineService.CreatePipeline()`

### Airflow
- `ops/airflow/dags/` - папка для сгенерированных DAG файлов
- `ops/airflow/scripts/setup_connections.py` - настройка connections
- `ops/airflow/scripts/init.sh` - инициализация Airflow

### Docker
- Папка `ops/airflow/scripts` монтируется в контейнер
- Connections настраиваются автоматически при запуске

## Пример использования

### 1. Создание пайплайна
```bash
curl -X POST http://localhost:8081/api/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "sourceType": "csv",
    "source": {
      "kind": "file",
      "type": "csv", 
      "pathOrUrl": "/data/input.csv"
    },
    "target": "postgresql",
    "ddl": {
      "postgresql": "CREATE TABLE data (id INT, name TEXT);"
    },
    "schedule": {
      "cron": "@hourly",
      "incrementalMode": "none"
    }
  }'
```

### 2. Результат
Будет создан файл: `ops/airflow/dags/pipe_{pipeline_id}_v1.py`

### 3. Airflow
DAG появится в Airflow UI и будет выполняться по расписанию.

## Настройка connections

Connections настраиваются автоматически при инициализации Airflow:

### PostgreSQL (pg_target)
```
Host: postgres-data
Port: 5432
Schema: datawarehouse
Login: datauser
Password: datapass
```

### ClickHouse (ch_target)
```
Host: clickhouse
Port: 8123
Schema: analytics
Login: clickuser
Password: clickpass
```

### HDFS (hdfs_target)
```
Host: hdfs-namenode
Port: 9820
Login: root
```

## Мониторинг

### Логи генерации
В логах бэкенда:
```
[PIPELINE] DAG generated successfully for pipeline abc-123-def
```

### Airflow UI
- http://localhost:8080 (admin/admin)
- Раздел "DAGs" - список всех пайплайнов
- Раздел "Admin > Connections" - настроенные подключения

### Проверка файлов
```bash
# Список сгенерированных DAG
ls ops/airflow/dags/pipe_*.py

# Содержимое DAG файла
cat ops/airflow/dags/pipe_abc123def_v1.py
```

## Устранение неполадок

### DAG не появляется в Airflow
1. Проверьте логи бэкенда на ошибки генерации
2. Убедитесь, что файл создался: `ls ops/airflow/dags/`
3. Проверьте синтаксис Python в сгенерированном файле
4. Перезапустите Airflow scheduler: `docker-compose restart airflow-scheduler`

### Ошибки выполнения DAG
1. Проверьте connections в Airflow UI
2. Убедитесь, что целевые системы доступны
3. Проверьте логи задач в Airflow UI

### Обновление шаблона
После изменения `backend/templates/dag_template.py`:
1. Перезапустите бэкенд
2. Пересоздайте пайплайны для применения нового шаблона

## Расширение функциональности

### Добавление новых типов источников
1. Обновите шаблон `dag_template.py`
2. Добавьте логику в функции `extract_from_*`
3. Обновите `DAGGeneratorService`

### Добавление новых целевых систем
1. Добавьте connection в `setup_connections.py`
2. Обновите `getConnectionID()` в `dag_generator.go`
3. Добавьте логику в `load_to_*` функции

### Кастомизация задач
Измените шаблон `dag_template.py` для добавления:
- Дополнительных задач
- Условной логики
- Параллельных веток
- Сенсоров и триггеров

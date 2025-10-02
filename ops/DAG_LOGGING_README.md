# Логирование генерации DAG файлов

Детальное логирование всех этапов автоматической генерации DAG файлов для отслеживания ошибок и мониторинга процесса.

## Автоматическая генерация

**Да, DAG генерируется автоматически!** При каждом создании пайплайна через API `/api/pipelines` автоматически создается соответствующий DAG файл в Airflow.

## Префикс логов

Все логи генерации DAG имеют префикс `[DAG-GEN]` для легкой фильтрации:

```bash
# Фильтрация только логов генерации DAG
docker logs backend-container 2>&1 | grep "\[DAG-GEN\]"
```

## Этапы логирования

### 1. Начало генерации
```
[DAG-GEN] Starting DAG generation for pipeline abc-123-def
[DAG-GEN] Source type: csv, Target: postgresql
[DAG-GEN] Schedule: @hourly, Incremental mode: none
```

### 2. Подготовка данных шаблона
```
[DAG-GEN] Preparing template data...
[DAG-GEN] Generating DAG ID for pipeline abc-123-def
[DAG-GEN] Generated DAG ID: pipe_abc_123_def_v1
[DAG-GEN] Marshaling source config...
[DAG-GEN] Source config marshaled successfully. Length: 156 bytes
[DAG-GEN] Determined connection ID: pg_target for target: postgresql
[DAG-GEN] Preparing target config...
[DAG-GEN] Target config prepared successfully
[DAG-GEN] Marshaling DDL statements...
[DAG-GEN] DDL statements marshaled successfully. Length: 245 bytes
[DAG-GEN] Create DDL flag: true
[DAG-GEN] Pipeline name: Pipeline_abc_123_
[DAG-GEN] Template data structure created successfully
[DAG-GEN] Template data prepared successfully. DAG ID: pipe_abc_123_def_v1
```

### 3. Рендеринг шаблона
```
[DAG-GEN] Rendering DAG template...
[DAG-GEN] Reading template file: backend/templates/dag_template.py
[DAG-GEN] Template file read successfully. Size: 8456 bytes
[DAG-GEN] Parsing template...
[DAG-GEN] Template parsed successfully
[DAG-GEN] Executing template with data...
[DAG-GEN] Template executed successfully. Generated content size: 9234 bytes
[DAG-GEN] DAG template rendered successfully. Content length: 9234 bytes
```

### 4. Сохранение файла
```
[DAG-GEN] Saving DAG file...
[DAG-GEN] Target directory: ops/airflow/dags
[DAG-GEN] Ensuring directory exists...
[DAG-GEN] Directory ensured successfully
[DAG-GEN] Target file path: ops/airflow/dags/pipe_abc_123_def_v1.py
[DAG-GEN] Writing DAG file...
[DAG-GEN] DAG file written successfully: ops/airflow/dags/pipe_abc_123_def_v1.py (9234 bytes)
```

### 5. Завершение
```
[DAG-GEN] DAG generation completed successfully for pipeline abc-123-def
```

## Логирование ошибок

### Ошибки подготовки данных
```
[DAG-GEN] ERROR: Failed to marshal source config: invalid character 'x' looking for beginning of value
[DAG-GEN] ERROR: Failed to marshal target config: json: unsupported type: func()
[DAG-GEN] ERROR: Failed to marshal DDL statements: json: unsupported value: +Inf
```

### Ошибки шаблона
```
[DAG-GEN] ERROR: Failed to read template file backend/templates/dag_template.py: no such file or directory
[DAG-GEN] ERROR: Failed to parse template: template: dag:15: unexpected "}" in command
[DAG-GEN] ERROR: Failed to execute template: template: dag:42: executing "dag" at <.InvalidField>: can't evaluate field InvalidField
```

### Ошибки файловой системы
```
[DAG-GEN] ERROR: Failed to create dags directory ops/airflow/dags: permission denied
[DAG-GEN] ERROR: Failed to write DAG file ops/airflow/dags/pipe_abc_123_def_v1.py: no space left on device
```

## Логирование удаления DAG

### Успешное удаление
```
[DAG-GEN] Starting DAG deletion for pipeline abc-123-def
[DAG-GEN] Generated DAG ID for deletion: pipe_abc_123_def_v1
[DAG-GEN] Target file for deletion: ops/airflow/dags/pipe_abc_123_def_v1.py
[DAG-GEN] Attempting to delete DAG file...
[DAG-GEN] DAG file deleted successfully: ops/airflow/dags/pipe_abc_123_def_v1.py
```

### Файл не существует
```
[DAG-GEN] DAG file does not exist (already deleted): ops/airflow/dags/pipe_abc_123_def_v1.py
```

### Ошибка удаления
```
[DAG-GEN] ERROR: Failed to delete DAG file ops/airflow/dags/pipe_abc_123_def_v1.py: permission denied
```

## Логирование списка DAG

```
[DAG-GEN] Listing DAG files in directory: ops/airflow/dags
[DAG-GEN] Found 5 entries in directory
[DAG-GEN] Found generated DAG: pipe_abc_123_def_v1
[DAG-GEN] Found generated DAG: pipe_xyz_789_ghi_v1
[DAG-GEN] Total generated DAGs found: 2
```

## Мониторинг в реальном времени

### Просмотр логов в реальном времени
```bash
# Все логи бэкенда
docker logs -f backend-container

# Только логи генерации DAG
docker logs -f backend-container 2>&1 | grep "\[DAG-GEN\]"

# Логи с временными метками
docker logs -f -t backend-container 2>&1 | grep "\[DAG-GEN\]"
```

### Поиск ошибок
```bash
# Только ошибки генерации DAG
docker logs backend-container 2>&1 | grep "\[DAG-GEN\] ERROR"

# Последние 100 строк логов с ошибками
docker logs --tail 100 backend-container 2>&1 | grep "\[DAG-GEN\] ERROR"
```

## Типичные проблемы и решения

### 1. Шаблон не найден
**Ошибка:**
```
[DAG-GEN] ERROR: Failed to read template file backend/templates/dag_template.py: no such file or directory
```

**Решение:**
- Убедитесь, что файл `backend/templates/dag_template.py` существует
- Проверьте рабочую директорию бэкенда
- Проверьте права доступа к файлу

### 2. Ошибка парсинга шаблона
**Ошибка:**
```
[DAG-GEN] ERROR: Failed to parse template: template: dag:15: unexpected "}" in command
```

**Решение:**
- Проверьте синтаксис шаблона Go template
- Убедитесь, что все `{{` имеют соответствующие `}}`
- Проверьте корректность условных конструкций `{{if}}...{{end}}`

### 3. Папка DAG недоступна
**Ошибка:**
```
[DAG-GEN] ERROR: Failed to create dags directory ops/airflow/dags: permission denied
```

**Решение:**
- Проверьте права доступа к папке `ops/airflow/dags`
- Убедитесь, что бэкенд запущен с правильными правами
- Создайте папку вручную: `mkdir -p ops/airflow/dags`

### 4. Некорректные данные JSON
**Ошибка:**
```
[DAG-GEN] ERROR: Failed to marshal source config: json: unsupported type: func()
```

**Решение:**
- Проверьте структуру данных в запросе создания пайплайна
- Убедитесь, что все поля сериализуемы в JSON
- Проверьте типы данных в `domain.CreatePipelineRequest`

## Интеграция с мониторингом

### Prometheus метрики (будущее расширение)
```go
// Примеры метрик для мониторинга
dag_generation_total{status="success|error"}
dag_generation_duration_seconds
dag_template_size_bytes
dag_file_size_bytes
```

### Алерты
- Частые ошибки генерации DAG
- Превышение времени генерации
- Недоступность папки DAG
- Ошибки шаблона

## Отладка

### Включение дополнительного логирования
Для более детального логирования можно добавить переменную окружения:
```bash
export DAG_DEBUG=true
```

### Проверка сгенерированных файлов
```bash
# Список всех сгенерированных DAG
ls -la ops/airflow/dags/pipe_*.py

# Содержимое конкретного DAG
cat ops/airflow/dags/pipe_abc_123_def_v1.py

# Проверка синтаксиса Python
python -m py_compile ops/airflow/dags/pipe_abc_123_def_v1.py
```


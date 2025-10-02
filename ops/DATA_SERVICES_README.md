# Сервисы для хранения данных

В docker-compose.yml добавлены дополнительные сервисы для хранения и обработки данных.

## Добавленные сервисы

### 1. PostgreSQL для данных (`postgres-data`)
- **Порт**: 5433 (чтобы не конфликтовать с Airflow PostgreSQL на 5432)
- **База данных**: `datawarehouse`
- **Пользователь**: `datauser` / `datapass`
- **Подключение**: `postgresql://datauser:datapass@localhost:5433/datawarehouse`

**Схемы:**
- `raw_data` - сырые импортированные данные
- `processed_data` - обработанные данные  
- `analytics` - аналитические таблицы

### 2. ClickHouse (`clickhouse`)
- **HTTP порт**: 8123
- **Native порт**: 9000
- **База данных**: `analytics`
- **Пользователь**: `clickuser` / `clickpass`
- **Web UI**: http://localhost:8123/play
- **Подключение**: `clickhouse://clickuser:clickpass@localhost:9000/analytics`

### 3. HDFS кластер
#### NameNode (`hdfs-namenode`)
- **Web UI**: http://localhost:9870
- **RPC порт**: 9820

#### DataNode (`hdfs-datanode`)  
- **Web UI**: http://localhost:9864

**HDFS URI**: `hdfs://localhost:9820`

## Запуск сервисов

### Запуск всех сервисов (включая Airflow):
```bash
cd ops
docker-compose up -d
```

### Запуск только сервисов данных:
```bash
cd ops
docker-compose up -d postgres-data clickhouse hdfs-namenode hdfs-datanode
```

### Проверка статуса:
```bash
docker-compose ps
```

## Подключение к сервисам

### PostgreSQL:
```bash
# Через psql
docker exec -it postgres-data psql -U datauser -d datawarehouse

# Или через любой PostgreSQL клиент
# Host: localhost, Port: 5433, DB: datawarehouse, User: datauser, Pass: datapass
```

### ClickHouse:
```bash
# Через clickhouse-client
docker exec -it clickhouse clickhouse-client --user clickuser --password clickpass

# Или через HTTP API
curl "http://localhost:8123/?user=clickuser&password=clickpass" -d "SELECT version()"
```

### HDFS:
```bash
# Через hadoop CLI
docker exec -it hdfs-namenode hdfs dfs -ls /

# Создание директории
docker exec -it hdfs-namenode hdfs dfs -mkdir /data

# Загрузка файла
docker exec -it hdfs-namenode hdfs dfs -put /tmp/file.txt /data/
```

## Конфигурация

### PostgreSQL
- Инициализационные скрипты: `ops/init-scripts/`
- Автоматически создаются схемы и пользователи при первом запуске

### ClickHouse  
- Дополнительные конфиги: `ops/clickhouse-config/`
- Настройки пользователей и безопасности

### HDFS
- Конфигурация через переменные окружения в docker-compose.yml
- WebHDFS включен для REST API доступа

## Volumes (постоянное хранение)

- `postgres-data-volume` - данные PostgreSQL
- `clickhouse-data` - данные ClickHouse  
- `clickhouse-logs` - логи ClickHouse
- `hdfs-namenode` - метаданные HDFS
- `hdfs-datanode` - файлы HDFS

## Сеть

Все сервисы данных подключены к сети `data-network` для изоляции от Airflow сервисов.

## Мониторинг

Все сервисы имеют health checks:
- PostgreSQL: `pg_isready`
- ClickHouse: HTTP ping
- HDFS: Web UI доступность

## Остановка сервисов

```bash
# Остановка всех сервисов
docker-compose down

# Остановка с удалением volumes (ВНИМАНИЕ: удалит все данные!)
docker-compose down -v
```

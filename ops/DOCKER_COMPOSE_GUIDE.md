# Docker Compose Guide - Как правильно запускать проект

## 📋 У вас есть два Docker Compose файла:

### 1. **`docker-compose.yml`** - Полная конфигурация
**Включает:**
- ✅ Airflow (полный стек)
- ✅ PostgreSQL для данных
- ✅ ClickHouse
- ✅ HDFS (NameNode + DataNode)
- ✅ Redis

### 2. **`docker-compose-minimal.yml`** - Минимальная конфигурация
**Включает:**
- ✅ Airflow (полный стек)
- ✅ PostgreSQL для данных
- ✅ ClickHouse
- ❌ Без HDFS (если вызывает проблемы)
- ✅ Redis

## 🚀 Как запускать

### Вариант 1: Полная конфигурация (рекомендуется)

```bash
# Переходим в директорию ops
cd ops

# Запускаем полную конфигурацию
docker-compose up -d

# Проверяем статус
docker-compose ps
```

**Используйте этот вариант если:**
- ✅ У вас достаточно ресурсов (8GB+ RAM)
- ✅ Нужен HDFS для больших данных
- ✅ Хотите полную функциональность

### Вариант 2: Минимальная конфигурация (если есть проблемы)

```bash
# Переходим в директорию ops
cd ops

# Запускаем минимальную конфигурацию
docker-compose -f docker-compose-minimal.yml up -d

# Проверяем статус
docker-compose -f docker-compose-minimal.yml ps
```

**Используйте этот вариант если:**
- ⚠️ HDFS вызывает проблемы с запуском
- ⚠️ Недостаточно ресурсов системы
- ✅ Нужны только основные сервисы

## 🔧 Пошаговый запуск

### Шаг 1: Подготовка
```bash
# Убедитесь что Docker Desktop запущен
docker --version
docker-compose --version

# Переходим в правильную директорию
cd C:\Users\admin\Desktop\ai-data-engineer\ops
```

### Шаг 2: Выбор конфигурации

#### Попробуйте сначала полную:
```bash
# Остановить все контейнеры (если запущены)
docker-compose down -v
docker-compose -f docker-compose-minimal.yml down -v

# Запустить полную конфигурацию
docker-compose up -d
```

#### Если полная не работает, используйте минимальную:
```bash
# Остановить полную конфигурацию
docker-compose down -v

# Запустить минимальную
docker-compose -f docker-compose-minimal.yml up -d
```

### Шаг 3: Проверка запуска
```bash
# Проверить статус всех сервисов
docker-compose ps
# или для минимальной:
docker-compose -f docker-compose-minimal.yml ps

# Проверить логи если есть проблемы
docker-compose logs airflow-apiserver
docker-compose logs postgres-data
docker-compose logs clickhouse
```

## 📊 Что должно работать после запуска

### Обязательные сервисы (в обеих конфигурациях):
- **Airflow UI**: http://localhost:8080 (airflow/airflow)
- **PostgreSQL Data**: localhost:5433 (datauser/datapass)
- **ClickHouse**: http://localhost:8123

### Дополнительно в полной конфигурации:
- **HDFS NameNode**: http://localhost:9870
- **HDFS DataNode**: http://localhost:9864

## 🛠️ Команды управления

### Запуск
```bash
# Полная конфигурация
docker-compose up -d

# Минимальная конфигурация
docker-compose -f docker-compose-minimal.yml up -d

# Запуск с логами (для отладки)
docker-compose up
```

### Остановка
```bash
# Остановить (сохранить данные)
docker-compose stop

# Остановить и удалить контейнеры (сохранить volumes)
docker-compose down

# Полная очистка (УДАЛИТ ВСЕ ДАННЫЕ!)
docker-compose down -v
```

### Перезапуск отдельных сервисов
```bash
# Перезапустить конкретный сервис
docker-compose restart airflow-apiserver
docker-compose restart postgres-data
docker-compose restart clickhouse

# Пересобрать и перезапустить
docker-compose up -d --force-recreate airflow-apiserver
```

## 🔍 Диагностика проблем

### Проверка ресурсов
```bash
# Использование ресурсов
docker stats

# Свободное место
docker system df

# Очистка неиспользуемых ресурсов
docker system prune -f
```

### Проверка сети
```bash
# Список сетей
docker network ls

# Проверка конкретной сети
docker network inspect ops_data-network
```

### Логи сервисов
```bash
# Все логи
docker-compose logs

# Конкретный сервис
docker-compose logs -f airflow-apiserver
docker-compose logs -f postgres-data
docker-compose logs -f clickhouse

# Последние N строк
docker-compose logs --tail=50 airflow-apiserver
```

## 🎯 Рекомендации

### Для разработки:
1. **Начните с минимальной конфигурации** - она быстрее запускается
2. **Используйте полную только если нужен HDFS**
3. **Регулярно очищайте Docker** - `docker system prune`

### Для продакшена:
1. **Используйте полную конфигурацию**
2. **Настройте мониторинг**
3. **Сделайте бэкапы volumes**

## 🚨 Если что-то не работает

### Проблема: HDFS не запускается
**Решение:** Используйте минимальную конфигурацию
```bash
docker-compose down -v
docker-compose -f docker-compose-minimal.yml up -d
```

### Проблема: Недостаточно памяти
**Решение:** 
1. Увеличьте память Docker Desktop до 8GB+
2. Или используйте минимальную конфигурацию

### Проблема: Порты заняты
**Решение:** Проверьте что порты свободны
```bash
netstat -an | findstr "8080\|5433\|8123\|9870"
```

## 📝 Быстрая шпаргалка

```bash
# Полный запуск
cd ops && docker-compose up -d

# Минимальный запуск
cd ops && docker-compose -f docker-compose-minimal.yml up -d

# Проверка статуса
docker-compose ps

# Остановка
docker-compose down

# Полная очистка
docker-compose down -v && docker system prune -f

# Логи
docker-compose logs -f airflow-apiserver
```

## ✅ После успешного запуска

1. **Проверьте Airflow**: http://localhost:8080
2. **Запустите Go бэкенд**: `cd ../backend && go run cmd/server/main.go`
3. **Проверьте интеграцию**: `curl http://localhost:8081/api/database/status`

Готово! 🚀

# HDFS Troubleshooting Guide

## Проблема: `dependency failed to start: container hdfs-namenode is unhealthy`

### Причины проблемы:
1. **Недостаток ресурсов** - HDFS требует много памяти и времени для инициализации
2. **Неправильная конфигурация** - сложная HA конфигурация может вызывать проблемы
3. **Проблемы с форматированием NameNode** - первый запуск требует форматирования
4. **Конфликты портов** или сетевые проблемы

### Решения:

## Вариант 1: Исправленная конфигурация HDFS (рекомендуется)

Я обновил конфигурацию в `docker-compose.yml`:

### Ключевые изменения:
- ✅ Упрощена конфигурация (убрана HA)
- ✅ Добавлено автоматическое форматирование NameNode
- ✅ Увеличено время ожидания healthcheck (120s)
- ✅ Добавлены настройки памяти Java
- ✅ Исправлены сетевые настройки

### Команды для запуска:
```bash
# Остановить все контейнеры
docker-compose down -v

# Удалить старые volumes (ВНИМАНИЕ: удалит данные!)
docker volume prune -f

# Запустить с исправленной конфигурацией
docker-compose up -d
```

## Вариант 2: Запуск без HDFS (быстрое решение)

Если HDFS не критичен для разработки, используйте минимальную конфигурацию:

```bash
# Использовать минимальную версию без HDFS
docker-compose -f docker-compose-minimal.yml up -d
```

Эта версия включает:
- ✅ Airflow (полнофункциональный)
- ✅ PostgreSQL для данных
- ✅ ClickHouse для аналитики
- ❌ Без HDFS (можно добавить позже)

## Вариант 3: Диагностика проблем

### Проверка ресурсов системы:
```bash
# Проверить доступную память
docker system df
docker stats

# Проверить логи контейнера
docker-compose logs hdfs-namenode
```

### Пошаговый запуск:
```bash
# 1. Запустить только базовые сервисы
docker-compose up -d postgres redis postgres-data clickhouse

# 2. Дождаться их готовности
docker-compose ps

# 3. Запустить Airflow
docker-compose up -d airflow-init
docker-compose up -d airflow-apiserver airflow-scheduler airflow-worker

# 4. Запустить HDFS отдельно
docker-compose up -d hdfs-namenode
```

## Вариант 4: Альтернативные образы HDFS

Если проблемы продолжаются, можно попробовать другие образы:

```yaml
# В docker-compose.yml замените:
image: apache/hadoop:3.3.6
# На:
image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
```

## Мониторинг и проверка

### После успешного запуска проверьте:
- **Airflow UI**: http://localhost:8080 (airflow/airflow)
- **ClickHouse**: http://localhost:8123
- **HDFS NameNode**: http://localhost:9870
- **PostgreSQL**: localhost:5433 (datauser/datapass)

### Полезные команды:
```bash
# Проверить статус всех сервисов
docker-compose ps

# Проверить логи конкретного сервиса
docker-compose logs -f hdfs-namenode

# Перезапустить проблемный сервис
docker-compose restart hdfs-namenode

# Полная очистка и перезапуск
docker-compose down -v
docker system prune -f
docker-compose up -d
```

## Рекомендации для продакшена:

1. **Увеличьте ресурсы Docker Desktop**:
   - Память: минимум 8GB
   - CPU: минимум 4 ядра
   - Диск: минимум 50GB

2. **Используйте внешние volumes** для важных данных

3. **Настройте мониторинг** контейнеров

4. **Рассмотрите использование Kubernetes** для продакшена

## Быстрое решение (TL;DR):

```bash
# Если нужно быстро запустить без HDFS:
docker-compose -f docker-compose-minimal.yml up -d

# Если нужен HDFS, используйте исправленную версию:
docker-compose down -v
docker-compose up -d
```

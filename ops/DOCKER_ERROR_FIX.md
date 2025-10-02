# Исправление ошибки ContainerConfig в Docker Compose

## 🚨 Проблема
```
ERROR: for postgres  'ContainerConfig'
KeyError: 'ContainerConfig'
```

Эта ошибка возникает из-за поврежденных контейнеров или проблем с Docker Compose 1.29.2.

## 🔧 Решение (выполните на Linux сервере)

### Шаг 1: Полная очистка Docker
```bash
# Переходим в директорию проекта
cd ~/ai-data-engineer/ops

# Останавливаем все контейнеры
docker-compose down -v --remove-orphans
docker-compose -f docker-compose-minimal.yml down -v --remove-orphans

# Удаляем все контейнеры проекта
docker ps -a --filter "label=com.docker.compose.project=ops" -q | xargs -r docker rm -f

# Удаляем все volumes проекта
docker volume ls --filter "label=com.docker.compose.project=ops" -q | xargs -r docker volume rm

# Общая очистка Docker
docker system prune -af --volumes

# Удаляем сети проекта
docker network ls --filter "label=com.docker.compose.project=ops" -q | xargs -r docker network rm 2>/dev/null || true
```

### Шаг 2: Обновление Docker Compose (рекомендуется)
```bash
# Проверяем текущую версию
docker-compose --version

# Обновляем Docker Compose до последней версии
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Проверяем новую версию
docker-compose --version
```

### Шаг 3: Запуск минимальной конфигурации
```bash
# Сначала попробуйте минимальную версию (без HDFS)
docker-compose -f docker-compose-minimal.yml up -d

# Проверяем статус
docker-compose -f docker-compose-minimal.yml ps
```

### Шаг 4: Если минимальная работает, попробуйте полную
```bash
# Останавливаем минимальную
docker-compose -f docker-compose-minimal.yml down

# Запускаем полную версию
docker-compose up -d

# Проверяем статус
docker-compose ps
```

## 🛠️ Альтернативное решение

### Если проблема сохраняется, используйте Docker Compose V2:
```bash
# Установка Docker Compose V2
sudo apt-get update
sudo apt-get install docker-compose-plugin

# Использование новой команды
docker compose up -d
# вместо
docker-compose up -d
```

## 🔍 Диагностика

### Проверка Docker:
```bash
# Статус Docker
sudo systemctl status docker

# Перезапуск Docker если нужно
sudo systemctl restart docker

# Проверка версий
docker --version
docker-compose --version
```

### Проверка ресурсов:
```bash
# Свободное место
df -h

# Память
free -h

# Процессы Docker
ps aux | grep docker
```

## 📋 Быстрый скрипт очистки

Создайте файл `cleanup.sh`:
```bash
#!/bin/bash
echo "🔧 Очистка Docker для исправления ContainerConfig..."

# Остановка всех контейнеров проекта
docker-compose down -v --remove-orphans 2>/dev/null || true
docker-compose -f docker-compose-minimal.yml down -v --remove-orphans 2>/dev/null || true

# Удаление контейнеров проекта
docker ps -a --filter "label=com.docker.compose.project=ops" -q | xargs -r docker rm -f

# Удаление volumes
docker volume ls --filter "label=com.docker.compose.project=ops" -q | xargs -r docker volume rm

# Общая очистка
docker system prune -af --volumes

echo "✅ Очистка завершена!"
echo "Теперь запустите: docker-compose -f docker-compose-minimal.yml up -d"
```

Запуск:
```bash
chmod +x cleanup.sh
./cleanup.sh
```

## 🎯 Рекомендуемая последовательность

1. **Выполните полную очистку** (Шаг 1)
2. **Запустите минимальную версию** (без HDFS)
3. **Если работает** - попробуйте полную версию
4. **Если не работает** - обновите Docker Compose

## ⚠️ Важные замечания

- Команды выше **удалят все данные** в контейнерах
- Сохраните важные данные перед очисткой
- После очистки нужно будет заново создать образцы данных
- Используйте `docker-compose-minimal.yml` если HDFS вызывает проблемы

## 🚀 После успешного запуска

Проверьте что все сервисы работают:
```bash
# Статус контейнеров
docker-compose ps

# Проверка логов
docker-compose logs airflow-apiserver
docker-compose logs postgres-data
docker-compose logs clickhouse

# Проверка доступности
curl http://localhost:8080  # Airflow
curl http://localhost:8123/ping  # ClickHouse
```

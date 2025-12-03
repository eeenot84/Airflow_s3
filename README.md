# ETL Pipeline с Apache Airflow

Этот проект представляет собой ETL-пайплайн для получения данных о температуре из Singapore API, их хранения в MinIO и обработки с помощью Spark для загрузки в PostgreSQL.

## 🏗️ Архитектура

- **Apache Airflow** - оркестрация ETL-процессов
- **MinIO** - S3-совместимое объектное хранилище  
- **PostgreSQL** - реляционная база данных
- **PySpark** - обработка данных
- **Docker** - контейнеризация

## 🚀 Быстрый старт

### 1. Клонирование репозитория
```bash
git clone <your-repo-url>
cd <project-directory>
```

### 2. Настройка переменных окружения
```bash
# Копируем шаблон переменных окружения
cp .env.example .env

# Редактируем .env файл своими значениями
nano .env
```

**ВАЖНО**: Обязательно измените все пароли и ключи в `.env` файле!

### 3. Запуск проекта
```bash
# Сборка и запуск контейнеров
docker-compose up --build -d

# Проверка статуса
docker-compose ps
```

### 4. Доступ к сервисам

- **Airflow Web UI**: http://localhost:8080
  - Логин: значение из `AIRFLOW_ADMIN_USERNAME`
  - Пароль: значение из `AIRFLOW_ADMIN_PASSWORD`

- **MinIO Console**: http://localhost:9001
  - Логин: значение из `MINIO_ROOT_USER`
  - Пароль: значение из `MINIO_ROOT_PASSWORD`

- **PostgreSQL**: localhost:5433
  - База данных: значение из `POSTGRES_DB`
  - Пользователь: значение из `POSTGRES_USER`
  - Пароль: значение из `POSTGRES_PASSWORD`

## 🔒 Безопасность

### Обязательные действия перед запуском:

1. **Измените все пароли** в `.env` файле
2. **Сгенерируйте новый SECRET_KEY** для Airflow (32 символа)
3. **Никогда не коммитьте** `.env` файл в Git

### Рекомендации по паролям:
- Используйте сложные пароли (минимум 16 символов)
- Включайте буквы, цифры и специальные символы
- Используйте разные пароли для разных сервисов

## 📝 Конфигурация Airflow Connection

Для работы с MinIO в Airflow необходимо настроить подключение:

1. Зайдите в Airflow Web UI
2. Admin → Connections
3. Создайте новое подключение:
   - **Connection Id**: `minio_default`
   - **Connection Type**: `Amazon Web Services`
   - **Host**: `minio` (имя контейнера)
   - **Login**: значение из `MINIO_ROOT_USER`
   - **Password**: значение из `MINIO_ROOT_PASSWORD`
   - **Extra**: `{"endpoint_url": "http://minio:9000"}`

## 🔄 Workflow

### ETL Pipeline (температурные данные)
1. **Fetch Data Task**: Получает данные о температуре из Singapore API
2. **Upload to MinIO Task**: Сохраняет данные в MinIO бакет
3. **Spark Processing**: Обрабатывает данные и загружает в PostgreSQL

### Photo Animation Pipeline (анимация фотографий) 🎬
1. **List Photos**: Находит фотографии в MinIO (папка `photos/input/`)
2. **Download Photo**: Загружает фотографию из MinIO
3. **Animate Photo**: Создает анимированную версию (эффект Кена Бернса)
4. **Upload Animated**: Сохраняет анимацию обратно в MinIO (папка `photos/animated/`)

## 🛠️ Разработка

### Структура проекта
```
.
├── dags/                      # Airflow DAGs
│   ├── simple_dag.py         # DAG для температурных данных
│   └── photo_animation_dag.py # DAG для анимации фотографий
├── libs/                      # Библиотеки (JDBC драйверы)
├── minio_data/               # Данные MinIO (игнорируется Git)
├── docker-compose.yaml       # Docker конфигурация
├── Dockerfile                # Airflow образ
├── requirements.txt          # Python зависимости
├── spark_jobs.py            # Spark задания
├── photo_animator.py        # Модуль для анимации фотографий
├── .env.example             # Шаблон переменных окружения
└── .gitignore               # Игнорируемые файлы
```

### Запуск Spark задания
```bash
# Из контейнера Airflow
docker-compose exec airflow-webserver python /opt/airflow/spark_jobs.py
```

### Использование функции анимации фотографий 🎬

#### 1. Загрузка фотографий в MinIO
```bash
# Используйте MinIO Console или CLI для загрузки фотографий
# Путь в MinIO: photos/input/your_photo.jpg
```

#### 2. Запуск DAG в Airflow
- Откройте Airflow Web UI (http://localhost:8080)
- Найдите DAG `photo_animation`
- Включите и запустите DAG

#### 3. Результат
Анимированные фотографии будут сохранены в MinIO по пути: `photos/animated/your_photo_animated.gif`

#### Доступные эффекты анимации:
- **ken_burns** (по умолчанию) - комбинация zoom и pan
- **zoom** - эффект приближения
- **pan** - панорамирование (движение камеры)
- **fade** - эффект затухания/появления

#### Использование модуля напрямую:
```python
from photo_animator import PhotoAnimator

# Создаем аниматор
animator = PhotoAnimator(output_format='gif')

# Создаем анимацию
animator.animate_photo(
    image_path='path/to/photo.jpg',
    output_path='path/to/output.gif',
    effect='ken_burns',
    frames=60,
    zoom_factor=1.3,
    pan_direction='right',
    duration=3.0
)
```

## 🐛 Отладка

### Просмотр логов
```bash
# Все сервисы
docker-compose logs -f

# Конкретный сервис
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-scheduler
```

### Подключение к контейнерам
```bash
# Airflow
docker-compose exec airflow-webserver bash

# PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow
```

### Проверка подключений
```bash
# MinIO
curl http://localhost:9000/minio/health/live

# Airflow
curl http://localhost:8080/health
```

## 🔧 Остановка проекта

```bash
# Остановка контейнеров
docker-compose down

# Остановка с удалением volumes
docker-compose down -v
```

## 📋 Требования

- Docker
- Docker Compose
- Python 3.9+ (для локальной разработки)

## 🤝 Contributing

1. Создайте feature branch
2. Внесите изменения
3. Убедитесь, что `.env` не попал в коммит
4. Создайте Pull Request

## ⚠️ Важные заметки

- **НЕ коммитьте** файлы с паролями и ключами
- Регулярно обновляйте пароли
- Используйте разные пароли для разных окружений
- В продакшене используйте внешние системы управления секретами
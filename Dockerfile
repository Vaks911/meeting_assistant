# Базовый образ Python 3.12 (slim — лёгкая версия)
FROM python:3.12-slim

# Системные пакеты:
# - build-essential: компилятор для сборки C-расширений
# - ffmpeg: для обработки аудио в Whisper
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Рабочая директория внутри контейнера
WORKDIR /app

# Копируем список зависимостей
COPY requirements.txt .

# Устанавливаем Python-пакеты
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код проекта
COPY . .

# Создаём папку для загруженных аудио
RUN mkdir -p /app/uploads

# ИНФОРМИРУЕМ, что приложение слушает порт 8011 (заменили с 8000!)
EXPOSE 8011

# Команда запуска при старте контейнера. ИЗМЕНИЛИ порт на 8011!
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8011"]
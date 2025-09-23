FROM python:3.11-slim

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Рабочая директория
WORKDIR /app

# Копирование файла зависимостей
COPY requirements_parser.txt .

# Установка Python зависимостей
RUN pip install --no-cache-dir -r requirements_parser.txt

# Копирование кода парсера
COPY address.py .
COPY moscow.py .
COPY moscow_improved.py .
COPY parsing_worker.py .
COPY api_server.py .

# Создание директории для данных
RUN mkdir -p data

# Копирование базовой таблицы данных (если есть)
COPY data/moscow_improved_*.csv data/ 2>/dev/null || true

# Создание пользователя для безопасности
RUN useradd -m -u 1000 parseruser && chown -R parseruser:parseruser /app
USER parseruser

# Переменные окружения
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python3 -c "import redis; r=redis.from_url('$REDIS_PUBLIC_URL'); r.ping()" || exit 1

# Запуск воркера по умолчанию
CMD ["python3", "parsing_worker.py"]
FROM python:3.9-slim

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Создание рабочей директории
WORKDIR /app

# Копирование файла зависимостей
COPY requirements.txt .

# Установка Python зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода
COPY address.py .
COPY moscow.py .

# Создание директории для данных
RUN mkdir -p data

# Установка переменных окружения
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Открытие порта (для Railway)
EXPOSE 8080

# Команда по умолчанию - быстрый парсер
CMD ["python3", "address.py", "Москва, Тверская улица, 12", "1500"]

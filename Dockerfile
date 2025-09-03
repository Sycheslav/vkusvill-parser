# Используем официальный образ Python
FROM python:3.11-slim

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    procps \
    libxss1 \
    libnss3 \
    libatk-bridge2.0-0 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libxshmfence1 \
    libpango-1.0-0 \
    libcairo2 \
    libatspi2.0-0 \
    libgtk-3-0 \
    libgdk-pixbuf2.0-0 \
    libxss1 \
    libxrandr2 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrender1 \
    libxtst6 \
    libxi6 \
    libxrandr2 \
    libxss1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrender1 \
    libxtst6 \
    libxi6 \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файлы зависимостей
COPY requirements.txt pyproject.toml ./

# Устанавливаем Python зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем Playwright браузеры
RUN playwright install chromium
RUN playwright install-deps chromium

# Копируем исходный код
COPY src/ ./src/
COPY config.yaml ./
COPY env-example.txt ./

# Создаем директории для данных
RUN mkdir -p data/out data/images

# Создаем пользователя для безопасности
RUN useradd -m -u 1000 scraper && \
    chown -R scraper:scraper /app
USER scraper

# Устанавливаем переменные окружения
ENV PYTHONPATH=/app
ENV PLAYWRIGHT_BROWSERS_PATH=/app/.cache/ms-playwright

# Точка входа
ENTRYPOINT ["python", "-m", "src.main"]

# По умолчанию показываем справку
CMD ["--help"]

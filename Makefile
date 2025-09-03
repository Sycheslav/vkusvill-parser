.PHONY: help install test clean run-samokat run-all docker-build docker-run

# Переменные
PYTHON = python3
PIP = pip3
PYTEST = pytest
PLAYWRIGHT = playwright

# Цвета для вывода
GREEN = \033[0;32m
YELLOW = \033[1;33m
RED = \033[0;31m
NC = \033[0m # No Color

help: ## Показать справку
	@echo "$(GREEN)Доступные команды:$(NC)"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(YELLOW)%-15s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Установить зависимости
	@echo "$(GREEN)Устанавливаем зависимости...$(NC)"
	$(PIP) install -r requirements.txt
	@echo "$(GREEN)Устанавливаем браузеры Playwright...$(NC)"
	$(PLAYWRIGHT) install chromium
	@echo "$(GREEN)Установка завершена!$(NC)"

install-dev: ## Установить зависимости для разработки
	@echo "$(GREEN)Устанавливаем зависимости для разработки...$(NC)"
	$(PIP) install -r requirements.txt
	$(PIP) install -r requirements-dev.txt
	$(PLAYWRIGHT) install chromium
	@echo "$(GREEN)Установка завершена!$(NC)"

test: ## Запустить тесты
	@echo "$(GREEN)Запускаем тесты...$(NC)"
	$(PYTEST) tests/ -v

test-coverage: ## Запустить тесты с покрытием
	@echo "$(GREEN)Запускаем тесты с покрытием...$(NC)"
	$(PYTEST) tests/ --cov=src --cov-report=html --cov-report=term

quick-test: ## Быстрый тест основных функций
	@echo "$(GREEN)Запускаем быстрый тест...$(NC)"
	$(PYTHON) quick_test.py

clean: ## Очистить временные файлы
	@echo "$(GREEN)Очищаем временные файлы...$(NC)"
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -delete
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
	@echo "$(GREEN)Очистка завершена!$(NC)"

clean-data: ## Очистить данные
	@echo "$(YELLOW)ВНИМАНИЕ: Удаляем все данные!$(NC)"
	rm -rf data/out/*
	rm -rf data/images/*
	@echo "$(GREEN)Данные очищены!$(NC)"

run-samokat: ## Запустить скрапинг Самоката
	@echo "$(GREEN)Запускаем скрапинг Самоката...$(NC)"
	$(PYTHON) -m src.main --source samokat --city "Москва" --out data/out/samokat.csv

run-lavka: ## Запустить скрапинг Яндекс.Лавки
	@echo "$(GREEN)Запускаем скрапинг Яндекс.Лавки...$(NC)"
	$(PYTHON) -m src.main --source lavka --city "Москва" --out data/out/lavka.csv

run-vkusvill: ## Запустить скрапинг ВкусВилла
	@echo "$(GREEN)Запускаем скрапинг ВкусВилла...$(NC)"
	$(PYTHON) -m src.main --source vkusvill --city "Москва" --out data/out/vkusvill.csv

run-all: ## Запустить скрапинг всех источников
	@echo "$(GREEN)Запускаем скрапинг всех источников...$(NC)"
	$(PYTHON) -m src.main --source all --city "Москва" --out data/out/all_sources.sqlite

run-all-with-images: ## Запустить скрапинг всех источников с загрузкой изображений
	@echo "$(GREEN)Запускаем скрапинг всех источников с изображениями...$(NC)"
	$(PYTHON) -m src.main --source all --city "Москва" --download-images --out data/out/all_sources.sqlite

docker-build: ## Собрать Docker образ
	@echo "$(GREEN)Собираем Docker образ...$(NC)"
	docker build -t food-scraper .

docker-run: ## Запустить Docker контейнер
	@echo "$(GREEN)Запускаем Docker контейнер...$(NC)"
	docker run -it --rm -v $(PWD)/data:/app/data food-scraper --help

docker-run-samokat: ## Запустить скрапинг Самоката в Docker
	@echo "$(GREEN)Запускаем скрапинг Самоката в Docker...$(NC)"
	docker run -it --rm -v $(PWD)/data:/app/data food-scraper --source samokat --city "Москва" --out data/out/samokat.csv

docker-run-all: ## Запустить скрапинг всех источников в Docker
	@echo "$(GREEN)Запускаем скрапинг всех источников в Docker...$(NC)"
	docker run -it --rm -v $(PWD)/data:/app/data food-scraper --source all --city "Москва" --download-images --out data/out/all_sources.sqlite

docker-compose-up: ## Запустить через docker-compose
	@echo "$(GREEN)Запускаем через docker-compose...$(NC)"
	docker-compose up food-scraper

docker-compose-samokat: ## Запустить скрапинг Самоката через docker-compose
	@echo "$(GREEN)Запускаем скрапинг Самоката через docker-compose...$(NC)"
	docker-compose up samokat-scraper

docker-compose-all: ## Запустить скрапинг всех источников через docker-compose
	@echo "$(GREEN)Запускаем скрапинг всех источников через docker-compose...$(NC)"
	docker-compose up all-sources-scraper

setup-env: ## Настроить переменные окружения
	@echo "$(GREEN)Настраиваем переменные окружения...$(NC)"
	@if [ ! -f .env ]; then \
		cp env-example.txt .env; \
		echo "$(GREEN)Файл .env создан из env-example.txt$(NC)"; \
		echo "$(YELLOW)Отредактируйте .env для настройки параметров$(NC)"; \
	else \
		echo "$(YELLOW)Файл .env уже существует$(NC)"; \
	fi

check-deps: ## Проверить зависимости
	@echo "$(GREEN)Проверяем зависимости...$(NC)"
	@$(PYTHON) -c "import playwright" 2>/dev/null || echo "$(RED)Playwright не установлен$(NC)"
	@$(PYTHON) -c "import click" 2>/dev/null || echo "$(RED)Click не установлен$(NC)"
	@$(PYTHON) -c "import pandas" 2>/dev/null || echo "$(RED)Pandas не установлен$(NC)"
	@echo "$(GREEN)Проверка завершена!$(NC)"

stats: ## Показать статистику данных
	@echo "$(GREEN)Статистика данных:$(NC)"
	@if [ -f data/out/products.db ]; then \
		$(PYTHON) -c "from src.utils.storage import DataStorage; s = DataStorage(); print(s.get_statistics())"; \
	else \
		echo "$(YELLOW)База данных не найдена$(NC)"; \
	fi

format: ## Форматировать код
	@echo "$(GREEN)Форматируем код...$(NC)"
	black src/ tests/
	isort src/ tests/

lint: ## Проверить код линтером
	@echo "$(GREEN)Проверяем код линтером...$(NC)"
	flake8 src/ tests/
	black --check src/ tests/
	isort --check-only src/ tests/

# Команды по умолчанию
.DEFAULT_GOAL := help

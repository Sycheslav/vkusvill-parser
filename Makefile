# Makefile для Ready Food Scraper

.PHONY: help install install-dev setup test lint format clean build run run-dev docker-build docker-run docker-stop docker-clean

# Переменные
PYTHON := python3.11
PIP := pip
DOCKER_IMAGE := ready-food-scraper
DOCKER_CONTAINER := ready-food-scraper

# Помощь
help:
	@echo "Ready Food Scraper - команды сборки и запуска"
	@echo ""
	@echo "Установка и настройка:"
	@echo "  install        Установка зависимостей"
	@echo "  install-dev    Установка зависимостей для разработки"
	@echo "  setup          Полная настройка окружения"
	@echo "  install-browser Установка браузера Playwright"
	@echo ""
	@echo "Разработка:"
	@echo "  test           Запуск тестов"
	@echo "  lint           Проверка кода линтерами"
	@echo "  format         Форматирование кода"
	@echo "  clean          Очистка временных файлов"
	@echo ""
	@echo "Запуск:"
	@echo "  run            Запуск скрейпинга (все магазины)"
	@echo "  run-samokat    Запуск скрейпинга Самокат"
	@echo "  run-lavka      Запуск скрейпинга Яндекс Лавка"
	@echo "  run-vkusvill   Запуск скрейпинга ВкусВилл"
	@echo "  run-dev        Запуск в режиме разработки"
	@echo ""
	@echo "Docker:"
	@echo "  docker-build   Сборка Docker образа"
	@echo "  docker-run     Запуск в Docker"
	@echo "  docker-stop    Остановка Docker контейнера"
	@echo "  docker-clean   Очистка Docker ресурсов"
	@echo ""
	@echo "Утилиты:"
	@echo "  validate-config Валидация конфигурации"
	@echo "  cleanup-images  Очистка старых изображений"

# Установка зависимостей
install:
	$(PIP) install -r requirements.txt

install-dev:
	$(PIP) install -r requirements-dev.txt

install-browser:
	playwright install chromium

setup: install install-browser
	@echo "Создание необходимых директорий..."
	mkdir -p data images logs
	@echo "Копирование примера конфигурации..."
	@if [ ! -f .env ]; then cp env-example.txt .env; fi
	@echo "Настройка завершена!"

# Разработка
test:
	pytest tests/ -v --cov=app --cov-report=html --cov-report=term

lint:
	flake8 app/ tests/
	mypy app/

format:
	black app/ tests/
	isort app/ tests/

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/ dist/ .coverage htmlcov/ .pytest_cache/

# Запуск
run:
	$(PYTHON) -m app scrape --shop all --city "Москва" --address "Красная площадь, 1" --out data/foods.csv

run-samokat:
	$(PYTHON) -m app scrape --shop samokat --city "Москва" --address "Красная площадь, 1" --out data/samokat.csv

run-lavka:
	$(PYTHON) -m app scrape --shop lavka --city "Москва" --address "Красная площадь, 1" --out data/lavka.csv

run-vkusvill:
	$(PYTHON) -m app scrape --shop vkusvill --city "Москва" --address "Красная площадь, 1" --out data/vkusvill.csv

run-dev:
	$(PYTHON) -m app scrape --shop samokat --city "Москва" --address "Красная площадь, 1" --out data/foods.csv --no-headless --log-level DEBUG

run-with-images:
	$(PYTHON) -m app scrape --shop all --city "Москва" --address "Красная площадь, 1" --out data/foods.csv --download-images

# Docker
docker-build:
	docker build -t $(DOCKER_IMAGE) .

docker-run: docker-build
	docker-compose up scraper

docker-run-detached: docker-build
	docker-compose up -d scraper

docker-stop:
	docker-compose down

docker-clean:
	docker-compose down --rmi all --volumes --remove-orphans
	docker system prune -f

# Специальные задачи Docker
docker-task:
	docker-compose run --rm scraper-task scrape --shop all --city "Москва" --out data/foods.csv

docker-samokat:
	docker-compose run --rm scraper-task scrape --shop samokat --city "Москва" --out data/samokat.csv

docker-lavka:
	docker-compose run --rm scraper-task scrape --shop lavka --city "Москва" --out data/lavka.csv

docker-vkusvill:
	docker-compose run --rm scraper-task scrape --shop vkusvill --city "Москва" --out data/vkusvill.csv

# Утилиты
validate-config:
	$(PYTHON) -m app validate-config

cleanup-images:
	$(PYTHON) -m app cleanup-images --days 7

# Примеры запуска с различными параметрами
example-spb:
	$(PYTHON) -m app scrape --shop all --city "Санкт-Петербург" --address "Невский проспект, 1" --out data/spb_foods.csv

example-with-proxy:
	$(PYTHON) -m app scrape --shop samokat --city "Москва" --proxy "http://proxy:8080" --out data/foods.csv

example-parallel:
	$(PYTHON) -m app scrape --shop all --city "Москва" --parallel 6 --out data/foods.csv

example-formats:
	$(PYTHON) -m app scrape --shop all --city "Москва" --format csv json parquet --out data/foods.csv

# Сборка пакета
build:
	$(PYTHON) -m build

# Публикация (для разработчиков)
publish-test:
	$(PYTHON) -m twine upload --repository testpypi dist/*

publish:
	$(PYTHON) -m twine upload dist/*

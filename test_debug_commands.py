#!/usr/bin/env python3
"""
Тестовый скрипт для проверки новых отладочных команд
"""
import asyncio
import logging
import sys
import os

# Добавляем путь к src
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.main import load_config
from src.sources.samokat import SamokatScraper
from src.sources.lavka import LavkaScraper

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('debug_test.log')
    ]
)

async def test_samokat_scraper():
    """Тестирование парсера Самоката"""
    print("🧪 Тестирование парсера Самоката...")
    
    config = {
        'city': 'Москва',
        'headless': False,  # Показываем браузер для отладки
        'limit': 10
    }
    
    scraper = SamokatScraper(config)
    
    try:
        async with scraper:
            print("✅ Скрейпер Самоката инициализирован")
            
            # Получаем категории
            categories = await scraper.get_categories()
            print(f"📋 Категории Самоката: {categories}")
            
            if categories:
                # Тестируем первую категорию
                test_category = categories[0]
                print(f"🔄 Тестируем категорию: {test_category}")
                
                products = await scraper.scrape_category(test_category, 5)
                print(f"📦 Получено товаров: {len(products)}")
                
                # Показываем первые 3 товара
                for i, product in enumerate(products[:3], 1):
                    print(f"  {i}. {product.name} - {product.price} руб.")
                    
    except Exception as e:
        print(f"❌ Ошибка в парсере Самоката: {e}")
        import traceback
        traceback.print_exc()

async def test_lavka_scraper():
    """Тестирование парсера Яндекс Лавки"""
    print("\n🧪 Тестирование парсера Яндекс Лавки...")
    
    config = {
        'city': 'Москва',
        'headless': False,  # Показываем браузер для отладки
        'limit': 10
    }
    
    scraper = LavkaScraper(config)
    
    try:
        async with scraper:
            print("✅ Скрейпер Яндекс Лавки инициализирован")
            
            # Получаем категории
            categories = await scraper.get_categories()
            print(f"📋 Категории Яндекс Лавки: {categories}")
            
            if categories:
                # Тестируем первую категорию
                test_category = categories[0]
                print(f"🔄 Тестируем категорию: {test_category}")
                
                products = await scraper.scrape_category(test_category, 5)
                print(f"📦 Получено товаров: {len(products)}")
                
                # Показываем первые 3 товара
                for i, product in enumerate(products[:3], 1):
                    print(f"  {i}. {product.name} - {product.price} руб.")
                    
    except Exception as e:
        print(f"❌ Ошибка в парсере Яндекс Лавки: {e}")
        import traceback
        traceback.print_exc()

async def main():
    """Главная функция"""
    print("🚀 Запуск тестирования отладочных команд...")
    
    # Тестируем Самокат
    await test_samokat_scraper()
    
    # Тестируем Яндекс Лавку
    await test_lavka_scraper()
    
    print("\n✅ Тестирование завершено!")

if __name__ == "__main__":
    asyncio.run(main())

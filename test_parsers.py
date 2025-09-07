#!/usr/bin/env python3
"""
Тестовый скрипт для проверки парсеров Самоката и ВкусВилла
"""
import asyncio
import sys
import os

# Добавляем путь к src в PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.sources.samokat import SamokatScraper
from src.sources.vkusvill import VkusvillScraper

async def test_samokat():
    """Тестирование парсера Самоката"""
    print("🧪 Тестирование парсера Самоката...")
    
    config = {
        'city': 'Москва',
        'headless': True,
        'max_concurrent': 1,
        'throttle_min': 0.1,
        'throttle_max': 0.3
    }
    
    scraper = SamokatScraper(config)
    
    try:
        async with scraper:
            print("✅ Браузер инициализирован")
            
            # Получаем категории
            categories = await scraper.get_categories()
            print(f"📋 Категории: {categories[:3]}...")
            
            # Тестируем парсинг одной категории
            test_category = categories[0] if categories else 'Вся готовая еда'
            print(f"🔍 Тестируем категорию: {test_category}")
            
            products = await scraper.scrape_category(test_category, limit=5)
            print(f"📦 Найдено товаров: {len(products)}")
            
            # Показываем детали первых товаров
            for i, product in enumerate(products[:3], 1):
                print(f"\n📦 Товар {i}:")
                print(f"  Название: {product.name}")
                print(f"  Цена: {product.price} руб.")
                print(f"  URL: {product.url[:50]}..." if product.url else "  URL: не указан")
                print(f"  Состав: {product.composition[:100]}..." if product.composition else "  Состав: не указан")
                print(f"  Порция: {product.portion_g}г" if product.portion_g else "  Порция: не указана")
                print(f"  Калории: {product.kcal_100g} ккал/100г" if product.kcal_100g else "  Калории: не указаны")
                print(f"  Белки: {product.protein_100g}г/100г" if product.protein_100g else "  Белки: не указаны")
                print(f"  Жиры: {product.fat_100g}г/100г" if product.fat_100g else "  Жиры: не указаны")
                print(f"  Углеводы: {product.carb_100g}г/100г" if product.carb_100g else "  Углеводы: не указаны")
            
            return len(products) > 0
            
    except Exception as e:
        print(f"❌ Ошибка тестирования Самоката: {e}")
        return False

async def test_vkusvill():
    """Тестирование парсера ВкусВилла"""
    print("\n🧪 Тестирование парсера ВкусВилла...")
    
    config = {
        'city': 'Москва',
        'headless': True,
        'max_concurrent': 1,
        'throttle_min': 0.1,
        'throttle_max': 0.3
    }
    
    scraper = VkusvillScraper(config)
    
    try:
        async with scraper:
            print("✅ Браузер инициализирован")
            
            # Получаем категории
            categories = await scraper.get_categories()
            print(f"📋 Категории: {categories[:3]}...")
            
            # Тестируем парсинг одной категории
            test_category = categories[0] if categories else 'Хаб «Готовая еда»'
            print(f"🔍 Тестируем категорию: {test_category}")
            
            products = await scraper.scrape_category(test_category, limit=5)
            print(f"📦 Найдено товаров: {len(products)}")
            
            # Показываем детали первых товаров
            for i, product in enumerate(products[:3], 1):
                print(f"\n📦 Товар {i}:")
                print(f"  Название: {product.name}")
                print(f"  Цена: {product.price} руб.")
                print(f"  URL: {product.url[:50]}..." if product.url else "  URL: не указан")
                print(f"  Состав: {product.composition[:100]}..." if product.composition else "  Состав: не указан")
                print(f"  Порция: {product.portion_g}г" if product.portion_g else "  Порция: не указана")
                print(f"  Калории: {product.kcal_100g} ккал/100г" if product.kcal_100g else "  Калории: не указаны")
                print(f"  Белки: {product.protein_100g}г/100г" if product.protein_100g else "  Белки: не указаны")
                print(f"  Жиры: {product.fat_100g}г/100г" if product.fat_100g else "  Жиры: не указаны")
                print(f"  Углеводы: {product.carb_100g}г/100г" if product.carb_100g else "  Углеводы: не указаны")
            
            return len(products) > 0
            
    except Exception as e:
        print(f"❌ Ошибка тестирования ВкусВилла: {e}")
        return False

async def main():
    """Основная функция тестирования"""
    print("🚀 Запуск тестирования парсеров...")
    
    # Тестируем Самокат
    samokat_success = await test_samokat()
    
    # Тестируем ВкусВилл
    vkusvill_success = await test_vkusvill()
    
    print(f"\n📊 Результаты тестирования:")
    print(f"  Самокат: {'✅ Успешно' if samokat_success else '❌ Ошибка'}")
    print(f"  ВкусВилл: {'✅ Успешно' if vkusvill_success else '❌ Ошибка'}")
    
    if samokat_success and vkusvill_success:
        print("\n🎉 Все парсеры работают корректно!")
        return True
    else:
        print("\n⚠️ Некоторые парсеры требуют доработки")
        return False

if __name__ == '__main__':
    success = asyncio.run(main())
    sys.exit(0 if success else 1)

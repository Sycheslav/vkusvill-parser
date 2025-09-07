#!/usr/bin/env python3
"""
Тестовый скрипт для проверки детального парсинга ВкусВилла
"""
import asyncio
import sys
import os

# Добавляем путь к src в PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.sources.vkusvill import VkusvillScraper

async def test_vkusvill_detailed():
    """Тестирование детального парсинга ВкусВилла"""
    print("🧪 Тестирование детального парсинга ВкусВилла...")
    
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
            
            # Тестируем парсинг одной категории с детальным парсингом
            test_category = 'Хаб «Готовая еда»'
            print(f"🔍 Тестируем категорию: {test_category}")
            
            products = await scraper.scrape_category(test_category, limit=3)  # Ограничиваем до 3 для тестирования
            print(f"📦 Найдено товаров: {len(products)}")
            
            # Показываем детали всех товаров
            for i, product in enumerate(products, 1):
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
            
            # Проверяем, есть ли товары с пищевой ценностью
            products_with_nutrition = [p for p in products if any([p.kcal_100g, p.protein_100g, p.fat_100g, p.carb_100g])]
            print(f"\n📊 Товаров с пищевой ценностью: {len(products_with_nutrition)} из {len(products)}")
            
            return len(products) > 0
            
    except Exception as e:
        print(f"❌ Ошибка тестирования ВкусВилла: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Основная функция тестирования"""
    print("🚀 Запуск тестирования детального парсинга ВкусВилла...")
    
    success = await test_vkusvill_detailed()
    
    if success:
        print("\n🎉 Детальный парсинг ВкусВилла работает!")
        return True
    else:
        print("\n⚠️ Детальный парсинг ВкусВилла требует доработки")
        return False

if __name__ == '__main__':
    success = asyncio.run(main())
    sys.exit(0 if success else 1)

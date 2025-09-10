#!/usr/bin/env python3
"""Быстрый тест скрейпера - получение нескольких товаров для проверки."""

import asyncio
from app.models import ScrapingConfig
from app.scrapers import SamokratScraper
from app.utils.logger import setup_logger


async def quick_test():
    """Быстрый тест одного магазина."""
    setup_logger(level="INFO")
    
    # Конфигурация для теста
    config = ScrapingConfig(
        city="Москва",
        address="Красная площадь, 1",
        parallel_workers=2,
        max_retries=1,
        request_delay_min=1.0,
        request_delay_max=2.0,
        headless=True
    )
    
    print("🚀 Запуск быстрого теста скрейпера...")
    print(f"Магазин: Самокат")
    print(f"Город: {config.city}")
    print(f"Адрес: {config.address}")
    print("-" * 50)
    
    try:
        async with SamokratScraper(config) as scraper:
            # Ограничиваем количество товаров для теста
            original_scrape_category = scraper._scrape_category
            
            async def limited_scrape_category(category):
                items = await original_scrape_category(category)
                # Берем только первые 3 товара для теста
                return items[:3]
            
            scraper._scrape_category = limited_scrape_category
            
            result = await scraper.scrape()
            
            print(f"✅ Тест завершен успешно!")
            print(f"Найдено товаров: {result.total_found}")
            print(f"Обработано: {result.successful}")
            print(f"Ошибок: {result.failed}")
            print(f"Время выполнения: {result.duration_seconds:.1f} сек")
            
            if result.items:
                print("\n📦 Примеры товаров:")
                for i, item in enumerate(result.items[:3], 1):
                    print(f"{i}. {item.name}")
                    print(f"   Цена: {item.price} руб")
                    print(f"   Категория: {item.category}")
                    if item.has_complete_nutrients():
                        print(f"   КБЖУ: {item.kcal_100g}ккал, Б{item.protein_100g}г, Ж{item.fat_100g}г, У{item.carb_100g}г")
                    print(f"   URL: {item.url}")
                    print()
                
                # Проверяем полноту нутриентов
                completeness = scraper.get_nutrient_completeness_rate(result.items)
                print(f"🥗 Полнота нутриентов: {completeness:.1%}")
                
                if completeness >= 0.99:
                    print("✅ Качество данных: ОТЛИЧНО")
                elif completeness >= 0.90:
                    print("⚠️ Качество данных: ХОРОШО")
                else:
                    print("❌ Качество данных: ТРЕБУЕТ УЛУЧШЕНИЯ")
            
    except Exception as e:
        print(f"❌ Ошибка при тестировании: {e}")
        return False
    
    return True


if __name__ == "__main__":
    success = asyncio.run(quick_test())
    if success:
        print("\n🎉 Быстрый тест прошел успешно! Скрейпер готов к работе.")
    else:
        print("\n💥 Тест не прошел. Проверьте настройки и подключение к интернету.")
        exit(1)

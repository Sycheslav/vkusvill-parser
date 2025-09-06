#!/usr/bin/env python3
"""
Тестовый скрипт для проверки улучшений парсера
"""
import asyncio
import time
import sys
import os
from pathlib import Path

# Добавляем путь к модулям
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.main import FoodScraper, load_config

async def test_improvements():
    """Тестируем улучшения парсера"""
    print("🚀 Тестируем улучшения парсера...")
    
    # Загружаем конфигурацию
    config = load_config('config.yaml')
    
    # Устанавливаем тестовые параметры
    config['limit'] = 1000  # 1000 товаров на каждый источник
    config['headless'] = True  # Скрытый браузер для ускорения
    config['max_concurrent'] = 3  # Параллельность
    config['throttle_min'] = 0.1  # Быстрые задержки
    config['throttle_max'] = 0.3
    
    print(f"📊 Конфигурация:")
    print(f"   • Лимит товаров: {config['limit']}")
    print(f"   • Источники: {config['sources']}")
    print(f"   • Параллельность: {config['max_concurrent']}")
    print(f"   • Скрытый браузер: {config['headless']}")
    
    # Создаем скрейпер
    scraper = FoodScraper(config)
    
    start_time = time.time()
    
    try:
        # Запускаем парсинг
        print("\n🔍 Начинаем парсинг всех источников...")
        all_products = await scraper.scrape_all()
        
        # Подсчитываем результаты
        total_products = 0
        for shop_name, products in all_products.items():
            count = len(products)
            total_products += count
            print(f"✅ {shop_name}: {count} товаров")
        
        duration = time.time() - start_time
        
        print(f"\n🎉 Парсинг завершен!")
        print(f"📊 Результаты:")
        print(f"   • Всего товаров: {total_products}")
        print(f"   • Время выполнения: {duration:.1f} сек")
        print(f"   • Средняя скорость: {total_products/duration:.1f} товаров/сек")
        
        # Проверяем, достигли ли мы цели
        expected_total = len(config['sources']) * config['limit']
        if total_products >= expected_total:
            print(f"✅ Цель достигнута! Ожидалось: {expected_total}, получено: {total_products}")
        else:
            print(f"⚠️  Цель не достигнута. Ожидалось: {expected_total}, получено: {total_products}")
        
        # Сохраняем результаты
        if total_products > 0:
            print(f"\n💾 Сохраняем {total_products} товаров в БД...")
            saved_count = await scraper.save_products(all_products)
            print(f"✅ Сохранено в БД: {saved_count}")
            
            # Экспортируем в CSV
            output_file = f"data/out/test_products_{int(time.time())}.csv"
            print(f"📁 Экспортируем в {output_file}...")
            export_success = await scraper.export_data(output_file, all_products)
            if export_success:
                print(f"✅ Данные экспортированы в: {output_file}")
            else:
                print(f"❌ Ошибка экспорта данных")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🧪 Тест улучшений парсера готовой еды")
    print("=" * 50)
    
    try:
        success = asyncio.run(test_improvements())
        if success:
            print("\n✅ Тест завершен успешно!")
            sys.exit(0)
        else:
            print("\n❌ Тест завершен с ошибками")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️  Тест прерван пользователем")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        sys.exit(1)

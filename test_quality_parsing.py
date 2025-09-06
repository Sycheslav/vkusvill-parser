#!/usr/bin/env python3
"""
Тест качественного парсинга для получения 500 товаров с Самоката и Лавки
"""
import asyncio
import time
import sys
import os
from pathlib import Path

# Добавляем путь к модулям
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.main import FoodScraper, load_config

async def test_quality_parsing():
    """Тест качественного парсинга"""
    print("🚀 Тест качественного парсинга для получения 500 товаров с Самоката и Лавки...")
    
    # Загружаем конфигурацию
    config = load_config('config.yaml')
    
    # Устанавливаем параметры для качественного парсинга
    config['limit'] = 500  # 500 товаров на каждый источник
    config['fast_mode'] = False  # Реальный парсинг
    config['headless'] = True  # Скрытый браузер
    config['max_concurrent'] = 3  # Параллельность
    config['throttle_min'] = 0.1  # Задержки для стабильности
    config['throttle_max'] = 0.3
    config['sources'] = ['samokat', 'lavka']  # Только Самокат и Лавка
    
    print(f"📊 Конфигурация:")
    print(f"   • Лимит товаров на источник: {config['limit']}")
    print(f"   • Источники: {config['sources']}")
    print(f"   • Ожидаемое общее количество: {len(config['sources']) * config['limit']}")
    print(f"   • Быстрый режим: {config['fast_mode']}")
    print(f"   • Параллельность: {config['max_concurrent']}")
    
    # Создаем скрейпер
    scraper = FoodScraper(config)
    
    start_time = time.time()
    
    try:
        # Запускаем парсинг
        print("\n🔍 Начинаем качественный парсинг...")
        all_products = await scraper.scrape_all()
        
        # Подсчитываем результаты
        total_products = 0
        quality_products = 0
        for shop_name, products in all_products.items():
            count = len(products)
            total_products += count
            
            # Проверяем качество товаров
            quality_count = 0
            for product in products:
                # Проверяем, что это не мусор
                if (product.name and 
                    len(product.name.strip()) > 5 and 
                    not any(spam in product.name.lower() for spam in ['авторизуйтесь', 'сортировка', 'основной ингредиент']) and
                    product.price and product.price > 0):
                    quality_count += 1
            
            print(f"✅ {shop_name}: {count} товаров (качественных: {quality_count})")
            quality_products += quality_count
        
        duration = time.time() - start_time
        
        print(f"\n🎉 Качественный парсинг завершен!")
        print(f"📊 Результаты:")
        print(f"   • Всего товаров: {total_products}")
        print(f"   • Качественных товаров: {quality_products}")
        print(f"   • Процент качества: {(quality_products/total_products*100):.1f}%")
        print(f"   • Время выполнения: {duration:.1f} сек")
        print(f"   • Скорость: {total_products/duration:.1f} товаров/сек")
        
        # Проверяем, достигли ли мы цели
        expected_total = len(config['sources']) * config['limit']
        if total_products >= expected_total:
            print(f"✅ Цель достигнута! Ожидалось: {expected_total}, получено: {total_products}")
        else:
            print(f"⚠️  Цель не достигнута. Ожидалось: {expected_total}, получено: {total_products}")
        
        # Проверяем качество
        if quality_products >= total_products * 0.8:  # 80% качественных товаров
            print(f"✅ Качество отличное! {quality_products} качественных товаров из {total_products}")
        else:
            print(f"⚠️  Качество требует улучшения. {quality_products} качественных товаров из {total_products}")
        
        # Сохраняем результаты
        if total_products > 0:
            print(f"\n💾 Сохраняем {total_products} товаров в БД...")
            saved_count = await scraper.save_products(all_products)
            print(f"✅ Сохранено в БД: {saved_count}")
            
            # Экспортируем в CSV
            output_file = f"data/out/quality_products_{int(time.time())}.csv"
            print(f"📁 Экспортируем в {output_file}...")
            export_success = await scraper.export_data(output_file, all_products)
            if export_success:
                print(f"✅ Данные экспортированы в: {output_file}")
                
                # Проверяем размер файла
                if os.path.exists(output_file):
                    file_size = os.path.getsize(output_file)
                    print(f"📏 Размер CSV файла: {file_size / 1024:.1f} KB")
            else:
                print(f"❌ Ошибка экспорта данных")
        
        return total_products >= expected_total and quality_products >= total_products * 0.8
        
    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🧪 Тест качественного парсинга")
    print("=" * 50)
    
    try:
        success = asyncio.run(test_quality_parsing())
        if success:
            print("\n✅ Тест завершен успешно! Качественный парсинг работает!")
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

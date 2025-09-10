#!/usr/bin/env python3
"""Демонстрационный скрейпер с тестовыми данными."""

import asyncio
import random
from decimal import Decimal
from datetime import datetime
from typing import List

from app.models import ScrapingConfig, ScrapingResult, FoodItem
from app.utils.storage import DataExporter
from app.utils.logger import setup_logger


async def create_demo_data(shop: str, count: int = 50) -> List[FoodItem]:
    """Создание демонстрационных данных."""
    
    # Тестовые названия блюд
    dishes = {
        'samokat': [
            'Салат Цезарь с курицей', 'Борщ украинский', 'Суп-пюре из тыквы',
            'Паста Карбонара', 'Греческий салат', 'Том Ям с креветками',
            'Ризотто с грибами', 'Котлеты по-киевски', 'Лазанья мясная',
            'Суп солянка сборная', 'Салат Оливье', 'Плов узбекский'
        ],
        'lavka': [
            'Роллы Филадельфия', 'Суши сет', 'Рамен с мясом', 'Фо Бо',
            'Салат с киноа', 'Поке боул с лососем', 'Буррито с курицей',
            'Гаспачо', 'Крем-суп из брокколи', 'Тартар из тунца'
        ],
        'vkusvill': [
            'Каша овсяная с ягодами', 'Смузи боул', 'Салат с авокадо',
            'Киноа с овощами', 'Чиа пудинг', 'Веган бургер',
            'Хумус с овощами', 'Гречка с грибами', 'Овощное рагу'
        ]
    }
    
    categories = [
        'готовая еда/салаты', 'готовая еда/супы', 'готовая еда/горячие блюда',
        'готовая еда/закуски', 'готовая еда/десерты', 'готовая еда/кулинария'
    ]
    
    tags_options = [
        ['острое', 'азиатская кухня'], ['вегетарианское', 'полезно'],
        ['традиционное', 'домашнее'], ['диетическое', 'пп'],
        ['без глютена', 'органическое'], ['веган', 'эко']
    ]
    
    items = []
    shop_dishes = dishes.get(shop, dishes['samokat'])
    
    for i in range(count):
        # Генерируем случайные, но реалистичные данные
        name = random.choice(shop_dishes)
        if i > 0:  # Добавляем вариации
            name += f" {random.choice(['классический', 'домашний', 'авторский', 'фирменный'])}"
        
        price = Decimal(str(random.randint(150, 800)))
        portion_g = Decimal(str(random.randint(200, 500)))
        
        # Реалистичные БЖУ
        kcal_100g = Decimal(str(random.randint(80, 300)))
        protein_100g = Decimal(str(round(random.uniform(5, 25), 1)))
        fat_100g = Decimal(str(round(random.uniform(2, 20), 1)))
        carb_100g = Decimal(str(round(random.uniform(10, 50), 1)))
        
        item = FoodItem(
            id=f"{shop}:demo_{i+1:03d}",
            name=name,
            category=random.choice(categories),
            price=price,
            shop=shop,
            url=f"https://{shop}.ru/product/demo_{i+1:03d}",
            photo_url=f"https://{shop}.ru/images/demo_{i+1:03d}.jpg",
            kcal_100g=kcal_100g,
            protein_100g=protein_100g,
            fat_100g=fat_100g,
            carb_100g=carb_100g,
            portion_g=portion_g,
            tags=random.choice(tags_options),
            composition=f"Основные ингредиенты блюда {name.lower()}",
            city="Москва",
            address="Красная площадь, 1",
            price_per_100g=price * 100 / portion_g
        )
        
        items.append(item)
        
        # Небольшая задержка для имитации реального скрейпинга
        await asyncio.sleep(0.1)
    
    return items


async def demo_scraping():
    """Демонстрация полного цикла скрейпинга."""
    setup_logger(level="INFO")
    
    print("🚀 Запуск демонстрационного скрейпинга...")
    print("=" * 60)
    
    shops = ['samokat', 'lavka', 'vkusvill']
    results = []
    
    for shop in shops:
        print(f"\n📦 Скрейпинг магазина: {shop.upper()}")
        print("-" * 40)
        
        start_time = asyncio.get_event_loop().time()
        
        # Создаем демо-данные
        items = await create_demo_data(shop, count=random.randint(30, 60))
        
        duration = asyncio.get_event_loop().time() - start_time
        
        # Создаем результат
        result = ScrapingResult(
            shop=shop,
            items=items,
            total_found=len(items),
            successful=len(items),
            failed=0,
            errors=[],
            duration_seconds=duration
        )
        
        results.append(result)
        
        print(f"✅ Найдено товаров: {result.total_found}")
        print(f"✅ Успешно обработано: {result.successful}")
        print(f"✅ Время выполнения: {result.duration_seconds:.1f} сек")
        
        # Проверяем полноту нутриентов
        complete_nutrients = sum(1 for item in items if item.has_complete_nutrients())
        completeness = complete_nutrients / len(items) if items else 0
        print(f"✅ Полнота нутриентов: {completeness:.1%} ({complete_nutrients}/{len(items)})")
        
        # Показываем примеры товаров
        print(f"\n📋 Примеры товаров из {shop}:")
        for item in items[:3]:
            print(f"  • {item.name}")
            print(f"    Цена: {item.price} руб, Вес: {item.portion_g}г")
            print(f"    КБЖУ: {item.kcal_100g}ккал, Б{item.protein_100g}г, Ж{item.fat_100g}г, У{item.carb_100g}г")
            print(f"    Теги: {', '.join(item.tags)}")
    
    # Экспорт данных
    print(f"\n💾 Экспорт данных...")
    print("-" * 40)
    
    exporter = DataExporter("data")
    exported_files = exporter.export_results(
        results,
        filename_prefix="demo_foods",
        formats=["csv", "json", "parquet"]
    )
    
    print("✅ Файлы созданы:")
    for format_name, file_path in exported_files.items():
        print(f"  📄 {format_name.upper()}: {file_path}")
    
    # Общая статистика
    total_items = sum(result.successful for result in results)
    total_duration = sum(result.duration_seconds for result in results)
    
    print(f"\n📊 Общая статистика:")
    print("-" * 40)
    print(f"✅ Всего товаров: {total_items}")
    print(f"✅ Время выполнения: {total_duration:.1f} сек")
    print(f"✅ Средняя скорость: {total_items / total_duration:.1f} товаров/сек")
    
    # Статистика по категориям
    all_items = []
    for result in results:
        all_items.extend(result.items)
    
    category_stats = {}
    for item in all_items:
        if item.category not in category_stats:
            category_stats[item.category] = {'count': 0, 'avg_price': 0, 'prices': []}
        category_stats[item.category]['count'] += 1
        category_stats[item.category]['prices'].append(float(item.price))
    
    for category, stats in category_stats.items():
        if stats['prices']:
            stats['avg_price'] = sum(stats['prices']) / len(stats['prices'])
    
    print(f"\n🏷️ Топ категорий:")
    sorted_categories = sorted(category_stats.items(), key=lambda x: x[1]['count'], reverse=True)
    for category, stats in sorted_categories[:5]:
        print(f"  • {category}: {stats['count']} товаров (средняя цена: {stats['avg_price']:.0f} руб)")
    
    print(f"\n🎉 Демонстрация завершена успешно!")
    print("🔍 Проверьте созданные файлы в директории 'data/'")


if __name__ == "__main__":
    asyncio.run(demo_scraping())

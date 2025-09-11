#!/usr/bin/env python3
"""
Тестовый скрипт для проверки работы скрейперов.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Добавляем текущую директорию в путь для импорта
sys.path.insert(0, str(Path(__file__).parent))

from main import AntiBotClient, ScraperOrchestrator


async def test_vkusvill():
    """Тест скрейпера ВкусВилл."""
    print("Тестирование ВкусВилл скрейпера...")
    
    try:
        # Создаем антибот-клиент
        antibot_client = AntiBotClient(concurrency=2, timeout=30)
        
        # Создаем оркестратор
        orchestrator = ScraperOrchestrator(
            antibot_client=antibot_client,
            output_path="test_vkusvill.csv"
        )
        
        # Тестируем скрейпинг с лимитом 5 товаров
        await orchestrator.scrape_shop(
            shop='vkusvill',
            city='Москва',
            coords='55.75,37.61',
            limit=5
        )
        
        # Экспортируем данные
        orchestrator.export_data()
        orchestrator.print_stats()
        
        print("✅ ВкусВилл тест завершен успешно")
        
    except Exception as e:
        print(f"❌ Ошибка в тесте ВкусВилл: {e}")
    finally:
        await antibot_client.close()


async def test_samokat():
    """Тест скрейпера Самоката."""
    print("Тестирование Самокат скрейпера...")
    
    try:
        # Создаем антибот-клиент
        antibot_client = AntiBotClient(concurrency=2, timeout=30)
        
        # Создаем оркестратор
        orchestrator = ScraperOrchestrator(
            antibot_client=antibot_client,
            output_path="test_samokat.csv"
        )
        
        # Тестируем скрейпинг с лимитом 3 товара
        await orchestrator.scrape_shop(
            shop='samokat',
            city='Москва',
            coords='55.75,37.61',
            limit=3
        )
        
        # Экспортируем данные
        orchestrator.export_data()
        orchestrator.print_stats()
        
        print("✅ Самокат тест завершен успешно")
        
    except Exception as e:
        print(f"❌ Ошибка в тесте Самокат: {e}")
    finally:
        await antibot_client.close()


async def test_lavka():
    """Тест скрейпера Лавки."""
    print("Тестирование Лавка скрейпера...")
    
    try:
        # Создаем антибот-клиент с низкой конкурентностью
        antibot_client = AntiBotClient(concurrency=1, timeout=60)
        
        # Создаем оркестратор
        orchestrator = ScraperOrchestrator(
            antibot_client=antibot_client,
            output_path="test_lavka.csv"
        )
        
        # Тестируем скрейпинг с лимитом 2 товара
        await orchestrator.scrape_shop(
            shop='lavka',
            city='Москва',
            coords='55.75,37.61',
            limit=2
        )
        
        # Экспортируем данные
        orchestrator.export_data()
        orchestrator.print_stats()
        
        print("✅ Лавка тест завершен успешно")
        
    except Exception as e:
        print(f"❌ Ошибка в тесте Лавка: {e}")
    finally:
        await antibot_client.close()


async def main():
    """Главная функция тестирования."""
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    print("🚀 Запуск тестирования скрейперов готовой еды")
    print("=" * 60)
    
    # Тестируем каждый скрейпер
    await test_vkusvill()
    print()
    
    await test_samokat()
    print()
    
    await test_lavka()
    print()
    
    print("=" * 60)
    print("🏁 Тестирование завершено")
    print("\nПроверьте созданные CSV файлы:")
    print("- test_vkusvill.csv")
    print("- test_samokat.csv") 
    print("- test_lavka.csv")


if __name__ == '__main__':
    asyncio.run(main())


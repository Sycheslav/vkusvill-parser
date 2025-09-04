#!/usr/bin/env python3
"""
Скрипт для запуска Telegram бота
"""
import asyncio
import sys
import os

# Добавляем путь к src в PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from telegram_bot import FoodScraperBot
from main import load_config

async def main():
    """Основная функция"""
    print("🤖 Запуск Telegram бота...")
    
    try:
        # Загружаем конфигурацию
        config = load_config('config.yaml')
        
        # Проверяем наличие токена
        if not config.get('telegram_bot_token'):
            print("❌ Ошибка: Не указан токен Telegram бота в config.yaml")
            print("Добавьте строку: telegram_bot_token: 'ваш_токен'")
            return
        
        print(f"📍 Город: {config.get('city', 'Москва')}")
        print(f"🔍 Источники: {', '.join(config.get('sources', ['samokat', 'lavka', 'vkusvill']))}")
        print(f"🏷️ Категории: {', '.join(config.get('categories', [])) or 'Автоопределение'}")
        print()
        
        # Создаем и запускаем бота
        bot = FoodScraperBot(config)
        
        print("✅ Бот создан успешно")
        print("🚀 Запуск бота...")
        print("💡 Используйте команду /start в Telegram для начала работы")
        print("💡 Используйте команду /scrape_all для парсинга всех источников")
        print()
        
        # Запускаем бота
        await bot.run()
        
    except KeyboardInterrupt:
        print("\n⚠️ Получен сигнал остановки")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(main())

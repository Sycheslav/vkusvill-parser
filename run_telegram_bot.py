#!/usr/bin/env python3
"""
Скрипт запуска Telegram бота для парсера готовой еды
"""
import asyncio
import os
import sys
from pathlib import Path

# Добавляем путь к src в sys.path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from telegram_bot import FoodScraperBot
from main import load_config


async def main():
    """Основная функция запуска бота"""
    
    # Загружаем конфигурацию
    config = load_config()
    
    # Проверяем наличие токена бота
    bot_token = config.get('telegram_bot_token')
    if not bot_token:
        print("❌ Ошибка: не указан токен Telegram бота!")
        print("Добавьте в config.yaml или .env файл:")
        print("telegram_bot_token: 'YOUR_BOT_TOKEN'")
        sys.exit(1)
    
    # Создаем и запускаем бота
    try:
        bot = FoodScraperBot(config)
        print("🤖 Запуск Telegram бота...")
        print(f"📍 Город: {config.get('city', 'Москва')}")
        print(f"🔍 Источники: {', '.join(config.get('sources', ['samokat']))}")
        print(f"🏷️ Категории: {', '.join(config.get('categories', [])) or 'Все'}")
        print("💡 Отправьте /start в Telegram для начала работы")
        
        await bot.run()
        
    except KeyboardInterrupt:
        print("\n⚠️  Получен сигнал остановки")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == '__main__':
    # Запускаем асинхронную функцию
    asyncio.run(main())

#!/usr/bin/env python3
"""
Тестовый скрипт для проверки Telegram бота
"""
import asyncio
import sys
from pathlib import Path

# Добавляем путь к src в sys.path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

try:
    from telegram_bot import FoodScraperBot
    from main import load_config
    print("✅ Модули импортированы успешно")
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    sys.exit(1)


async def test_bot_initialization():
    """Тест инициализации бота"""
    print("\n🧪 Тестирование инициализации бота...")
    
    try:
        # Загружаем конфигурацию
        config = load_config()
        print(f"✅ Конфигурация загружена: {len(config)} параметров")
        
        # Проверяем наличие токена
        bot_token = config.get('telegram_bot_token')
        if not bot_token:
            print("⚠️  Токен бота не найден в конфигурации")
            print("   Добавьте в config.yaml:")
            print("   telegram_bot_token: 'YOUR_BOT_TOKEN'")
            return False
        
        print(f"✅ Токен бота найден: {bot_token[:10]}...")
        
        # Создаем бота
        bot = FoodScraperBot(config)
        print("✅ Бот создан успешно")
        
        # Проверяем настройки
        print(f"   Город: {config.get('city', 'Москва')}")
        print(f"   Источники: {', '.join(config.get('sources', ['samokat']))}")
        print(f"   Категории: {', '.join(config.get('categories', [])) or 'Все'}")
        print(f"   Разрешенные пользователи: {len(config.get('telegram_allowed_users', []))}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка инициализации: {e}")
        return False


async def test_config_loading():
    """Тест загрузки конфигурации"""
    print("\n🧪 Тестирование загрузки конфигурации...")
    
    try:
        config = load_config()
        
        # Проверяем обязательные параметры
        required_params = ['city', 'sources', 'headless', 'max_concurrent']
        missing_params = [param for param in required_params if param not in config]
        
        if missing_params:
            print(f"❌ Отсутствуют параметры: {missing_params}")
            return False
        
        print("✅ Все обязательные параметры найдены")
        
        # Проверяем значения
        print(f"   Город: {config['city']}")
        print(f"   Источники: {config['sources']}")
        print(f"   Headless: {config['headless']}")
        print(f"   Максимум потоков: {config['max_concurrent']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка загрузки конфигурации: {e}")
        return False


async def test_environment_variables():
    """Тест переменных окружения"""
    print("\n🧪 Тестирование переменных окружения...")
    
    import os
    from dotenv import load_dotenv
    
    # Загружаем .env файл
    load_dotenv()
    
    # Проверяем ключевые переменные
    env_vars = {
        'CITY_NAME': os.getenv('CITY_NAME'),
        'HEADLESS': os.getenv('HEADLESS'),
        'MAX_CONCURRENT': os.getenv('MAX_CONCURRENT'),
        'TELEGRAM_BOT_TOKEN': os.getenv('TELEGRAM_BOT_TOKEN'),
        'TELEGRAM_ALLOWED_USERS': os.getenv('TELEGRAM_ALLOWED_USERS')
    }
    
    print("Переменные окружения:")
    for var, value in env_vars.items():
        if value:
            if var == 'TELEGRAM_BOT_TOKEN':
                print(f"   {var}: {value[:10]}...")
            else:
                print(f"   {var}: {value}")
        else:
            print(f"   {var}: не установлена")
    
    return True


async def main():
    """Основная функция тестирования"""
    print("🤖 Тестирование Telegram бота для парсера готовой еды")
    print("=" * 60)
    
    tests = [
        test_environment_variables,
        test_config_loading,
        test_bot_initialization
    ]
    
    results = []
    
    for test in tests:
        try:
            result = await test()
            results.append(result)
        except Exception as e:
            print(f"❌ Критическая ошибка в тесте: {e}")
            results.append(False)
    
    # Выводим итоги
    print("\n" + "=" * 60)
    print("📊 Результаты тестирования:")
    
    passed = sum(results)
    total = len(results)
    
    for i, result in enumerate(results, 1):
        status = "✅ ПРОЙДЕН" if result else "❌ ПРОВАЛЕН"
        print(f"   Тест {i}: {status}")
    
    print(f"\nИтого: {passed}/{total} тестов пройдено")
    
    if passed == total:
        print("🎉 Все тесты пройдены успешно!")
        print("💡 Теперь можно запускать бота командой: make run-bot")
        return 0
    else:
        print("⚠️  Некоторые тесты не пройдены")
        print("💡 Проверьте конфигурацию и зависимости")
        return 1


if __name__ == '__main__':
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n⚠️  Тестирование прервано пользователем")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        sys.exit(1)

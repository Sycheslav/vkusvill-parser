#!/usr/bin/env python3
"""
Тест доступности Telegram API
"""

import asyncio
import aiohttp
import time

async def test_telegram_api():
    """Тестируем доступность Telegram API"""
    
    print("🔍 Проверяем доступность Telegram API...")
    print("=" * 50)
    
    # Тест 1: HTTP запрос к Telegram API
    try:
        async with aiohttp.ClientSession() as session:
            print("🌐 Проверяем: https://api.telegram.org")
            start_time = time.time()
            
            async with session.get("https://api.telegram.org", timeout=30) as response:
                load_time = time.time() - start_time
                print(f"✅ Статус: {response.status} | Время: {load_time:.2f}с")
                
                if response.status == 200:
                    content = await response.text()
                    print(f"   📄 Размер: {len(content)} символов")
                else:
                    print(f"   ❌ HTTP ошибка: {response.status}")
                    
    except asyncio.TimeoutError:
        print("⏰ Таймаут (30с)")
    except Exception as e:
        print(f"❌ HTTP запрос не удался: {e}")
    
    print("-" * 30)
    
    # Тест 2: Проверяем другие сайты для сравнения
    sites = [
        "https://google.com",
        "https://yandex.ru",
        "https://github.com"
    ]
    
    async with aiohttp.ClientSession() as session:
        for site in sites:
            try:
                print(f"🌐 Проверяем: {site}")
                start_time = time.time()
                
                async with session.get(site, timeout=30) as response:
                    load_time = time.time() - start_time
                    print(f"✅ Статус: {response.status} | Время: {load_time:.2f}с")
                    
            except asyncio.TimeoutError:
                print(f"⏰ Таймаут (30с)")
            except Exception as e:
                print(f"❌ Ошибка: {e}")
            
            print("-" * 30)
            await asyncio.sleep(1)
    
    print("\n🎯 Рекомендации:")
    print("• Если все сайты не загружаются - проблема с интернетом")
    print("• Если только Telegram не загружается - проблема с блокировкой")
    print("• Если интернет медленный - нужно увеличить таймауты")

if __name__ == "__main__":
    asyncio.run(test_telegram_api())


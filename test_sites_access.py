#!/usr/bin/env python3
"""
Тест доступности всех сайтов
"""

import asyncio
import aiohttp
import time

async def test_sites_access():
    """Тестируем доступность всех сайтов"""
    
    sites = [
        "https://samokat.ru",
        "https://lavka.yandex.ru", 
        "https://vkusvill.ru",
        "https://google.com",  # Для сравнения
        "https://yandex.ru"    # Для сравнения
    ]
    
    print("🔍 Проверяем доступность сайтов...")
    print("=" * 50)
    
    async with aiohttp.ClientSession() as session:
        for site in sites:
            try:
                print(f"🌐 Проверяем: {site}")
                start_time = time.time()
                
                async with session.get(site, timeout=30) as response:
                    load_time = time.time() - start_time
                    print(f"✅ Статус: {response.status} | Время: {load_time:.2f}с")
                    
                    if response.status == 200:
                        content = await response.text()
                        print(f"   📄 Размер: {len(content)} символов")
                        
                        # Ищем ключевые слова
                        if "еда" in content.lower() or "food" in content.lower():
                            print("   🍽️ Ключевые слова еды найдены")
                        else:
                            print("   ❌ Ключевые слова еды не найдены")
                    else:
                        print(f"   ❌ HTTP ошибка: {response.status}")
                        
            except asyncio.TimeoutError:
                print(f"⏰ Таймаут (30с)")
            except Exception as e:
                print(f"❌ Ошибка: {e}")
            
            print("-" * 30)
            await asyncio.sleep(1)
    
    print("\n🎯 Рекомендации:")
    print("• Если Google/Yandex загружаются - проблема с конкретными сайтами")
    print("• Если все сайты не загружаются - проблема с интернетом/блокировкой")
    print("• Если сайты загружаются, но медленно - нужно увеличить таймауты")

if __name__ == "__main__":
    asyncio.run(test_sites_access())


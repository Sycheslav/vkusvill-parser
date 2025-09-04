#!/usr/bin/env python3
"""
Простой тест доступности сайта Самоката
"""

import asyncio
import aiohttp
from playwright.async_api import async_playwright

async def test_samokat_access():
    """Тестируем доступность сайта Самоката"""
    
    print("🔍 Проверяем доступность сайта Самоката...")
    
    # Тест 1: HTTP запрос
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://samokat.ru", timeout=30) as response:
                print(f"✅ HTTP статус: {response.status}")
                if response.status == 200:
                    content = await response.text()
                    print(f"📄 Размер страницы: {len(content)} символов")
                    
                    # Ищем ключевые слова
                    if "еда" in content.lower() or "food" in content.lower():
                        print("🍽️ Ключевые слова еды найдены")
                    else:
                        print("❌ Ключевые слова еды не найдены")
                else:
                    print(f"❌ HTTP ошибка: {response.status}")
    except Exception as e:
        print(f"❌ HTTP запрос не удался: {e}")
    
    # Тест 2: Playwright с минимальным ожиданием
    try:
        async with async_playwright() as p:
            browser = await p.webkit.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            
            print("🌐 Переходим на сайт...")
            await page.goto("https://samokat.ru", timeout=30000)
            
            # Ждем только DOM
            await page.wait_for_load_state("domcontentloaded", timeout=30000)
            
            print("📸 Делаем скриншот...")
            await page.screenshot(path="data/out/samokat_quick.png")
            
            # Получаем заголовок
            title = await page.title()
            print(f"📝 Заголовок страницы: {title}")
            
            # Ищем любые ссылки
            links = await page.query_selector_all("a")
            print(f"🔗 Найдено ссылок: {len(links)}")
            
            # Ищем ссылки с текстом
            text_links = []
            for link in links[:10]:
                try:
                    text = await link.text_content()
                    href = await link.get_attribute("href")
                    if text and href:
                        text_links.append((text.strip(), href))
                except:
                    continue
            
            print(f"📝 Ссылки с текстом: {len(text_links)}")
            for text, href in text_links[:5]:
                print(f"  • {text} -> {href}")
            
            await browser.close()
            
    except Exception as e:
        print(f"❌ Playwright тест не удался: {e}")

if __name__ == "__main__":
    asyncio.run(test_samokat_access())


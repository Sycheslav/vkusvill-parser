#!/usr/bin/env python3
"""
Простой тест Playwright
"""

import asyncio
from playwright.async_api import async_playwright

async def test_playwright():
    """Тестируем Playwright"""
    print("Запускаем тест Playwright с Firefox...")
    
    try:
        async with async_playwright() as p:
            # Пробуем запустить Firefox
            browser = await p.firefox.launch(
                headless=True,  # Используем headless режим
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage'
                ]
            )
            
            print("Браузер Firefox запущен успешно")
            
            # Создаем контекст
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 720}
            )
            
            print("Контекст создан успешно")
            
            # Создаем страницу
            page = await context.new_page()
            print("Страница создана успешно")
            
            # Переходим на простую страницу
            await page.goto('https://example.com')
            print("Переход на страницу выполнен успешно")
            
            # Получаем заголовок
            title = await page.title()
            print(f"Заголовок страницы: {title}")
            
            # Закрываем браузер
            await browser.close()
            print("Браузер закрыт успешно")
            
            return True
            
    except Exception as e:
        print(f"Ошибка: {e}")
        return False

if __name__ == "__main__":
    result = asyncio.run(test_playwright())
    if result:
        print("✅ Тест прошел успешно!")
    else:
        print("❌ Тест не прошел!")

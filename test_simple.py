#!/usr/bin/env python3
"""
Простой тест для проверки работы Playwright
"""
import asyncio
from playwright.async_api import async_playwright

async def test_browser():
    """Тест браузера"""
    try:
        playwright = await async_playwright().start()
        browser = await playwright.webkit.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        
        print("Браузер запущен успешно!")
        
        # Пробуем зайти на простой сайт
        await page.goto("https://www.google.com")
        print("Сайт загружен!")
        
        # Ждем немного
        await asyncio.sleep(5)
        
        # Закрываем браузер
        await page.close()
        await context.close()
        await browser.close()
        await playwright.stop()
        
        print("Браузер закрыт успешно!")
        
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    asyncio.run(test_browser())

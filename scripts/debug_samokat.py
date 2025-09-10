#!/usr/bin/env python3
"""Диагностический скрипт для Самоката с подробным логированием."""

import asyncio
from playwright.async_api import async_playwright
from app.utils.logger import setup_logger


async def debug_samokat():
    """Диагностика проблем с Самокатом."""
    setup_logger(level="INFO")
    
    print("🔍 ДИАГНОСТИКА САМОКАТА")
    print("📋 Проверяем доступность, капчи, селекторы")
    print("=" * 50)
    
    playwright = await async_playwright().start()
    browser = await playwright.firefox.launch(
        headless=False,
        firefox_user_prefs={
            "dom.webdriver.enabled": False,
            "useAutomationExtension": False
        }
    )
    
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/119.0",
        locale='ru-RU',
        timezone_id='Europe/Moscow'
    )
    
    page = await context.new_page()
    
    try:
        # 1. Проверяем главную страницу
        print("\n📄 ПРОВЕРКА ГЛАВНОЙ СТРАНИЦЫ")
        print("-" * 30)
        
        await page.goto("https://samokat.ru", wait_until="domcontentloaded", timeout=60000)
        title = await page.title()
        print(f"✅ Заголовок: {title}")
        
        if "сломались" in title.lower() or "ошибка" in title.lower():
            print("❌ ПРОБЛЕМА: Самокат показывает ошибку")
            print("💡 РЕШЕНИЕ: Попробуйте позже когда техработы закончатся")
            return False
        
        # 2. Проверяем категорию готовой еды
        print("\n📦 ПРОВЕРКА КАТЕГОРИИ ГОТОВОЙ ЕДЫ")
        print("-" * 30)
        
        category_url = "https://samokat.ru/category/vsya-gotovaya-eda-13"
        print(f"🔗 URL: {category_url}")
        
        await page.goto(category_url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(5)
        
        cat_title = await page.title()
        print(f"📄 Заголовок категории: {cat_title}")
        
        if "сломались" in cat_title.lower():
            print("❌ ПРОБЛЕМА: Категория недоступна")
            return False
        
        # 3. Анализируем содержимое страницы
        print("\n🔍 АНАЛИЗ СОДЕРЖИМОГО СТРАНИЦЫ")
        print("-" * 30)
        
        page_content = await page.content()
        content_length = len(page_content)
        print(f"📄 Размер страницы: {content_length} символов")
        
        # Ищем ключевые слова
        keywords = ['товар', 'product', 'цена', 'price', 'карточка', 'card']
        for keyword in keywords:
            count = page_content.lower().count(keyword)
            print(f"🔍 '{keyword}': {count} упоминаний")
        
        # 4. Тестируем селекторы
        print("\n🎯 ТЕСТИРОВАНИЕ СЕЛЕКТОРОВ")
        print("-" * 30)
        
        selectors = [
            '[class*="Product"]',
            '[class*="Card"]',
            '[class*="product"]',
            'a[href*="/product/"]',
            '.product-card',
            '.catalog-item',
            '.goods-tile',
            'article',
            'section'
        ]
        
        max_elements = 0
        best_selector = ""
        
        for selector in selectors:
            try:
                elements = await page.locator(selector).all()
                count = len(elements)
                print(f"   {selector}: {count} элементов")
                
                if count > max_elements:
                    max_elements = count
                    best_selector = selector
                    
                    # Проверяем содержимое первого элемента
                    if elements:
                        try:
                            first_text = await elements[0].inner_text()
                            print(f"      📝 Первый элемент: {first_text[:50]}...")
                        except:
                            print(f"      ❌ Не удалось получить текст")
                            
            except Exception as e:
                print(f"   {selector}: ОШИБКА - {e}")
        
        if max_elements > 0:
            print(f"\n✅ ЛУЧШИЙ СЕЛЕКТОР: {best_selector} ({max_elements} элементов)")
        else:
            print(f"\n❌ НИ ОДИН СЕЛЕКТОР НЕ НАШЕЛ ЭЛЕМЕНТЫ")
        
        # 5. Проверяем адрес доставки
        print("\n📍 ПРОВЕРКА АДРЕСА ДОСТАВКИ")
        print("-" * 30)
        
        # Ищем индикаторы адреса
        address_indicators = await page.locator('text="адрес", text="доставка", text="Выберите"').all()
        print(f"🏠 Найдено индикаторов адреса: {len(address_indicators)}")
        
        for indicator in address_indicators[:3]:
            try:
                text = await indicator.inner_text()
                print(f"   📍 {text}")
            except:
                pass
        
        # Ждем для визуального осмотра
        print(f"\n👀 ВИЗУАЛЬНЫЙ ОСМОТР")
        print("⏰ Ждем 15 секунд для осмотра браузера...")
        await asyncio.sleep(15)
        
        return max_elements > 0
        
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        await page.close()
        await context.close()
        await browser.close()
        await playwright.stop()


if __name__ == "__main__":
    print("🔍 ДИАГНОСТИКА САМОКАТА")
    print("🎯 Выявляем проблемы с парсингом")
    
    success = asyncio.run(debug_samokat())
    
    if success:
        print("\n✅ Самокат доступен для парсинга!")
    else:
        print("\n❌ Самокат недоступен (техработы или блокировка)")
        print("💡 Попробуйте позже или используйте другой магазин")
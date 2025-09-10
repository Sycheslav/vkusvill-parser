#!/usr/bin/env python3
"""Диагностический скрипт для Яндекс Лавки с подробным логированием."""

import asyncio
from playwright.async_api import async_playwright
from app.utils.logger import setup_logger


async def debug_lavka():
    """Диагностика проблем с Яндекс Лавкой."""
    setup_logger(level="INFO")
    
    print("🔍 ДИАГНОСТИКА ЯНДЕКС ЛАВКИ")
    print("📋 Проверяем капчу, доступность, селекторы")
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
        
        await page.goto("https://lavka.yandex.ru", wait_until="domcontentloaded", timeout=60000)
        title = await page.title()
        print(f"✅ Заголовок: {title}")
        
        if "робот" in title.lower() or "captcha" in title.lower():
            print("🛡️ ОБНАРУЖЕНА КАПЧА на главной!")
            print("👤 ИНСТРУКЦИЯ ДЛЯ ПРОХОЖДЕНИЯ КАПЧИ:")
            print("   1. В браузере нажмите 'Я не робот'")
            print("   2. Выполните проверку если потребуется")
            print("   3. Дождитесь перехода на нормальную страницу")
            
            # Даем время пройти капчу
            print("\n⏰ У вас есть 90 секунд для прохождения капчи...")
            for i in range(90, 0, -10):
                print(f"   ⏰ Осталось {i} секунд...")
                await asyncio.sleep(10)
                
                try:
                    current_title = await page.title()
                    current_url = page.url
                    print(f"      📄 Текущий заголовок: {current_title[:50]}...")
                    
                    if "робот" not in current_title.lower() and "captcha" not in current_title.lower():
                        print("✅ Капча пройдена!")
                        break
                except:
                    pass
            else:
                print("❌ Капча не пройдена за 90 секунд")
                return False
        
        # 2. Проверяем категорию готовой еды
        print("\n📦 ПРОВЕРКА КАТЕГОРИИ ГОТОВОЙ ЕДЫ")
        print("-" * 30)
        
        category_url = "https://lavka.yandex.ru/category/gotovaya_eda"
        print(f"🔗 URL: {category_url}")
        
        await page.goto(category_url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(5)
        
        cat_title = await page.title()
        print(f"📄 Заголовок категории: {cat_title}")
        
        if "робот" in cat_title.lower():
            print("🛡️ КАПЧА НА СТРАНИЦЕ КАТЕГОРИИ!")
            return False
        
        # 3. Ждем загрузки и анализируем
        print("\n⏳ ОЖИДАНИЕ ЗАГРУЗКИ ТОВАРОВ")
        print("-" * 30)
        
        print("⏳ Ждем загрузки товаров (15 сек)...")
        await asyncio.sleep(15)
        
        page_content = await page.content()
        content_length = len(page_content)
        print(f"📄 Размер страницы: {content_length} символов")
        
        # Ищем ключевые слова
        keywords = ['товар', 'product', 'цена', 'price', 'карточка', 'card', 'готовая', 'блюдо']
        for keyword in keywords:
            count = page_content.lower().count(keyword)
            print(f"🔍 '{keyword}': {count} упоминаний")
        
        # 4. Прокрутка для загрузки товаров
        print("\n🔄 ПРОКРУТКА ДЛЯ ЗАГРУЗКИ ТОВАРОВ")
        print("-" * 30)
        
        print("📜 Прокручиваем страницу...")
        for i in range(10):
            await page.mouse.wheel(0, 3000)
            await asyncio.sleep(1)
            print(f"   Прокрутка {i+1}/10")
        
        # 5. Тестируем селекторы после прокрутки
        print("\n🎯 ТЕСТИРОВАНИЕ СЕЛЕКТОРОВ ПОСЛЕ ПРОКРУТКИ")
        print("-" * 30)
        
        selectors = [
            '[data-testid*="product"]',
            '[data-testid*="item"]', 
            '[data-testid*="card"]',
            '[class*="Product"]',
            '[class*="Card"]',
            '[class*="Item"]',
            '[class*="Tile"]',
            '[class*="product"]',
            'a[href*="/product/"]',
            'a[href*="/goods/"]',
            '.product',
            '.item',
            'article',
            'section',
            'div[role="listitem"]',
            'li'
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
                    
                    # Проверяем содержимое первых элементов
                    if elements:
                        print(f"      📝 Содержимое элементов:")
                        for i, el in enumerate(elements[:3]):
                            try:
                                text = await el.inner_text()
                                if text and len(text.strip()) > 5:
                                    print(f"         {i+1}. {text[:60]}...")
                                    
                                    # Ищем цену в тексте
                                    import re
                                    price_match = re.search(r'(\d+)\s*₽', text)
                                    if price_match:
                                        print(f"            💰 Найдена цена: {price_match.group(1)}₽")
                                else:
                                    print(f"         {i+1}. [Пустой элемент]")
                            except:
                                print(f"         {i+1}. [Ошибка получения текста]")
                            
            except Exception as e:
                print(f"   {selector}: ОШИБКА - {e}")
        
        # 6. Итоговый результат
        print(f"\n📊 ИТОГОВЫЙ РЕЗУЛЬТАТ")
        print("-" * 30)
        
        if max_elements > 0:
            print(f"✅ ТОВАРЫ НАЙДЕНЫ: {max_elements} элементов")
            print(f"🎯 Лучший селектор: {best_selector}")
            print("💡 Лавка доступна для парсинга!")
            return True
        else:
            print("❌ ТОВАРЫ НЕ НАЙДЕНЫ")
            print("💡 Возможные причины:")
            print("   - Нужно выбрать адрес доставки")
            print("   - Товары загружаются асинхронно")
            print("   - Требуется больше времени ожидания")
            return False
        
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Ждем для финального осмотра
        print("\n👀 Финальный осмотр (15 сек)...")
        await asyncio.sleep(15)
        
        await page.close()
        await context.close()
        await browser.close()
        await playwright.stop()


if __name__ == "__main__":
    print("🔍 ЗАПУСК ДИАГНОСТИКИ ЯНДЕКС ЛАВКИ")
    print("🛡️ Будьте готовы пройти капчу вручную!")
    
    success = asyncio.run(debug_lavka())
    
    if success:
        print("\n🎉 ДИАГНОСТИКА ЗАВЕРШЕНА: Лавка доступна!")
        print("💡 Можно запускать парсинг")
    else:
        print("\n💥 ДИАГНОСТИКА ЗАВЕРШЕНА: Лавка недоступна")
        print("💡 Рекомендация: используйте ВкусВилл (275 товаров уже собрано)")

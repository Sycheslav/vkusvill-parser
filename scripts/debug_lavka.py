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
            print("👤 Для продолжения нужно пройти капчу вручную")
            
            # Даем время пройти капчу
            print("⏰ У вас есть 60 секунд для прохождения капчи...")
            for i in range(60, 0, -10):
                print(f"   ⏰ Осталось {i} секунд...")
                await asyncio.sleep(10)
                
                try:
                    current_title = await page.title()
                    if "робот" not in current_title.lower():
                        print("✅ Капча пройдена!")
                        break
                except:
                    pass
            else:
                print("❌ Капча не пройдена")
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
            print("👤 Пройдите капчу для продолжения...")
            
            for i in range(60, 0, -10):
                print(f"   ⏰ Осталось {i} секунд...")
                await asyncio.sleep(10)
                
                try:
                    current_title = await page.title()
                    if "робот" not in current_title.lower():
                        print("✅ Капча пройдена!")
                        break
                except:
                    pass
            else:
                print("❌ Капча не пройдена")
                return False
        
        # 3. Анализируем содержимое страницы
        print("\n🔍 АНАЛИЗ СОДЕРЖИМОГО СТРАНИЦЫ")
        print("-" * 30)
        
        # Ждем загрузки товаров
        print("⏳ Ждем загрузки товаров (10 сек)...")
        await asyncio.sleep(10)
        
        page_content = await page.content()
        content_length = len(page_content)
        print(f"📄 Размер страницы: {content_length} символов")
        
        # Ищем ключевые слова
        keywords = ['товар', 'product', 'цена', 'price', 'карточка', 'card', 'готовая еда']
        for keyword in keywords:
            count = page_content.lower().count(keyword)
            print(f"🔍 '{keyword}': {count} упоминаний")
        
        # 4. Тестируем селекторы
        print("\n🎯 ТЕСТИРОВАНИЕ СЕЛЕКТОРОВ")
        print("-" * 30)
        
        selectors = [
            '[data-testid*="product"]',
            '[data-testid*="item"]',
            '[class*="Product"]',
            '[class*="Card"]',
            '[class*="Item"]',
            '[class*="product"]',
            'a[href*="/product/"]',
            'a[href*="/goods/"]',
            '.product',
            '.item',
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
                    
                    # Проверяем содержимое первых элементов
                    if elements:
                        print(f"      📝 Содержимое первых элементов:")
                        for i, el in enumerate(elements[:3]):
                            try:
                                text = await el.inner_text()
                                print(f"         {i+1}. {text[:50]}...")
                            except:
                                print(f"         {i+1}. [Ошибка получения текста]")
                            
            except Exception as e:
                print(f"   {selector}: ОШИБКА - {e}")
        
        # 5. Прокрутка и повторная проверка
        print("\n🔄 ПРОКРУТКА И ПОВТОРНАЯ ПРОВЕРКА")
        print("-" * 30)
        
        print("📜 Прокручиваем страницу...")
        for i in range(5):
            await page.mouse.wheel(0, 2000)
            await asyncio.sleep(1)
            print(f"   Прокрутка {i+1}/5")
        
        # Повторно проверяем лучший селектор
        if best_selector:
            elements_after_scroll = await page.locator(best_selector).all()
            print(f"📊 После прокрутки: {len(elements_after_scroll)} элементов")
            
            if len(elements_after_scroll) > max_elements:
                print("✅ Прокрутка добавила новые элементы!")
                max_elements = len(elements_after_scroll)
        
        # 6. Итоговый результат
        print(f"\n📊 ИТОГОВЫЙ РЕЗУЛЬТАТ")
        print("-" * 30)
        
        if max_elements > 0:
            print(f"✅ ТОВАРЫ НАЙДЕНЫ: {max_elements} элементов")
            print(f"🎯 Лучший селектор: {best_selector}")
            print("💡 Самокат доступен для парсинга!")
            
            # Пробуем извлечь данные из первого товара
            try:
                elements = await page.locator(best_selector).all()
                if elements:
                    first_element = elements[0]
                    
                    print(f"\n🧪 ТЕСТ ИЗВЛЕЧЕНИЯ ДАННЫХ")
                    print("-" * 30)
                    
                    # Название
                    try:
                        name = await first_element.inner_text()
                        print(f"📝 Текст элемента: {name[:100]}...")
                    except:
                        print("❌ Не удалось получить текст")
                    
                    # Ссылка
                    try:
                        link_locator = first_element.locator('a[href]')
                        if await link_locator.count() > 0:
                            href = await link_locator.first.get_attribute('href')
                            print(f"🔗 Ссылка: {href}")
                        else:
                            print("❌ Ссылка не найдена")
                    except Exception as e:
                        print(f"❌ Ошибка поиска ссылки: {e}")
                    
                    # Цена
                    try:
                        price_selectors = ['.ProductCardActions_text__3Uohy', '[class*="price"]', 'text=₽']
                        for price_sel in price_selectors:
                            price_locator = first_element.locator(price_sel)
                            if await price_locator.count() > 0:
                                price_text = await price_locator.first.inner_text()
                                print(f"💰 Цена ({price_sel}): {price_text}")
                                break
                        else:
                            print("❌ Цена не найдена")
                    except Exception as e:
                        print(f"❌ Ошибка поиска цены: {e}")
            
            except Exception as e:
                print(f"❌ Ошибка тестирования извлечения: {e}")
            
            return True
        else:
            print("❌ ТОВАРЫ НЕ НАЙДЕНЫ")
            print("💡 Возможные причины:")
            print("   - Нужно выбрать адрес доставки")
            print("   - Товары загружаются через JavaScript")
            print("   - Антибот блокирует доступ")
            return False
        
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Ждем для финального осмотра
        print("\n👀 Финальный осмотр (10 сек)...")
        await asyncio.sleep(10)


if __name__ == "__main__":
    print("🔍 ЗАПУСК ДИАГНОСТИКИ САМОКАТА")
    
    success = asyncio.run(debug_samokat())
    
    if success:
        print("\n🎉 ДИАГНОСТИКА ЗАВЕРШЕНА: Самокат доступен!")
    else:
        print("\n💥 ДИАГНОСТИКА ЗАВЕРШЕНА: Самокат недоступен")
        print("💡 Рекомендация: используйте ВкусВилл (работает отлично)")

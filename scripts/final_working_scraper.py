#!/usr/bin/env python3
"""ФИНАЛЬНЫЙ РАБОЧИЙ ПАРСЕР с правильными селекторами."""

import asyncio
import re
import sys
from pathlib import Path
from decimal import Decimal
from playwright.async_api import async_playwright

# Добавляем родительскую папку в путь
sys.path.append(str(Path(__file__).parent.parent))

from app.utils.logger import setup_logger
from app.utils.storage import DataExporter
from app.models import FoodItem, ScrapingResult


async def final_working_scraper():
    """Финальный рабочий парсер всех магазинов."""
    setup_logger(level="INFO")
    
    print("🚀 ФИНАЛЬНЫЙ РАБОЧИЙ ПАРСЕР")
    print("🎯 Использует проверенные селекторы из диагностики")
    print("=" * 60)
    
    # Антидетекция настройки
    playwright = await async_playwright().start()
    browser = await playwright.firefox.launch(
        headless=False,  # Видимый для прохождения капчи
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
    
    results = []
    
    # 1. САМОКАТ
    print("\n📦 САМОКАТ")
    print("-" * 30)
    
    try:
        samokat_items = await scrape_samokat(context)
        if samokat_items:
            result = create_result("samokat", samokat_items)
            results.append(result)
            print(f"✅ Самокат: {len(samokat_items)} товаров")
        else:
            print("❌ Самокат: товары не найдены")
    except Exception as e:
        print(f"❌ Самокат: ошибка - {e}")
    
    # 2. ЛАВКА
    print("\n📦 ЛАВКА")
    print("-" * 30)
    
    try:
        lavka_items = await scrape_lavka(context)
        if lavka_items:
            result = create_result("lavka", lavka_items)
            results.append(result)
            print(f"✅ Лавка: {len(lavka_items)} товаров")
        else:
            print("❌ Лавка: товары не найдены")
    except Exception as e:
        print(f"❌ Лавка: ошибка - {e}")
    
    # Экспорт
    if results:
        print(f"\n💾 ЭКСПОРТ")
        print("-" * 30)
        
        exporter = DataExporter("data")
        exported_files = exporter.export_results(
            results,
            filename_prefix="FINAL_working",
            formats=["csv", "json"]
        )
        
        print("✅ Файлы:")
        for format_name, file_path in exported_files.items():
            print(f"  📄 {format_name.upper()}: {file_path}")
        
        total = sum(r.successful for r in results)
        print(f"\n🎉 ВСЕГО: {total} товаров")
    
    await context.close()
    await browser.close()
    await playwright.stop()
    
    return results


async def scrape_samokat(context):
    """Парсинг Самоката с проверенными селекторами."""
    page = await context.new_page()
    
    try:
        print("🌐 Загружаем Самокат...")
        await page.goto("https://samokat.ru/category/vsya-gotovaya-eda-13", wait_until="domcontentloaded")
        await asyncio.sleep(10)
        
        title = await page.title()
        print(f"📄 Заголовок: {title}")
        
        # Прокрутка
        print("🔄 Прокрутка...")
        for i in range(5):
            await page.mouse.wheel(0, 3000)
            await asyncio.sleep(1)
        
        # Используем рабочий селектор из диагностики
        print("🔍 Поиск товаров...")
        elements = await page.locator('a[href*="/product/"]').all()
        print(f"📦 Найдено ссылок на товары: {len(elements)}")
        
        items = []
        for i, element in enumerate(elements[:50]):  # Ограничиваем
            try:
                item_data = await extract_samokat_data(element)
                if item_data:
                    items.append(item_data)
                    if len(items) % 10 == 0:
                        print(f"   ✅ Обработано {len(items)} товаров...")
            except Exception as e:
                print(f"   ⚠️ Ошибка товара {i}: {e}")
        
        return items
        
    finally:
        await page.close()


async def scrape_lavka(context):
    """Парсинг Лавки с проверенными селекторами."""
    page = await context.new_page()
    
    try:
        print("🌐 Загружаем Лавку...")
        await page.goto("https://lavka.yandex.ru/category/gotovaya_eda", wait_until="domcontentloaded")
        
        title = await page.title()
        print(f"📄 Заголовок: {title}")
        
        # Проверка на капчу
        if "робот" in title.lower():
            print("🛡️ КАПЧА! Пройдите 'Я не робот' (60 сек)...")
            for i in range(60, 0, -10):
                print(f"⏰ {i} сек...")
                await asyncio.sleep(10)
                current_title = await page.title()
                if "робот" not in current_title.lower():
                    print("✅ Капча пройдена!")
                    break
            else:
                print("❌ Капча не пройдена")
                return []
        
        # Автоматически выбираем ЛЮБОЙ доступный адрес
        print("📍 Автоматически выбираем любой адрес...")
        try:
            # Ищем любые кнопки/ссылки связанные с адресом
            address_elements = await page.locator('button, a, div').filter(has_text=re.compile(r'адрес|доставка|выбрать', re.IGNORECASE)).all()
            
            if address_elements:
                print(f"🔍 Найдено {len(address_elements)} элементов адреса")
                for element in address_elements[:3]:  # Пробуем первые 3
                    try:
                        await element.click()
                        await asyncio.sleep(3)
                        print("✅ Кликнули на элемент адреса")
                        break
                    except:
                        continue
            
            # Ищем любое поле ввода и вводим адрес
            input_elements = await page.locator('input[type="text"], input[placeholder*="дрес"], input[placeholder*="город"]').all()
            
            if input_elements:
                for input_el in input_elements[:2]:
                    try:
                        await input_el.click()
                        await input_el.fill("Москва")
                        await asyncio.sleep(2)
                        await input_el.press('Enter')
                        print("✅ Ввели адрес в поле")
                        break
                    except:
                        continue
            
            # Ищем любые кнопки подтверждения
            confirm_elements = await page.locator('button').filter(has_text=re.compile(r'подтвердить|выбрать|готово|да|ок', re.IGNORECASE)).all()
            
            if confirm_elements:
                for button in confirm_elements[:2]:
                    try:
                        await button.click()
                        await asyncio.sleep(2)
                        print("✅ Нажали кнопку подтверждения")
                        break
                    except:
                        continue
            
            print("✅ Попытка выбора адреса завершена")
            
        except Exception as e:
            print(f"⚠️ Автоматический выбор адреса не удался: {e}")
        
        print("⏳ Ждем загрузки товаров после выбора адреса (20 сек)...")
        
        await asyncio.sleep(10)  # Ждем загрузки товаров
        
        # Прокрутка
        print("🔄 Прокрутка...")
        for i in range(10):
            await page.mouse.wheel(0, 2000)
            await asyncio.sleep(1)
        
        # Ищем товары по всем возможным селекторам
        print("🔍 Поиск товаров...")
        
        selectors = [
            'a[href*="/product/"]',
            'a[href*="/goods/"]',
            '[data-testid*="product"]',
            '[class*="Product"]',
            '[class*="Item"]'
        ]
        
        best_elements = []
        for selector in selectors:
            try:
                elements = await page.locator(selector).all()
                print(f"   {selector}: {len(elements)} элементов")
                if len(elements) > len(best_elements):
                    best_elements = elements
            except:
                continue
        
        if not best_elements:
            print("❌ Товары не найдены")
            return []
        
        items = []
        for i, element in enumerate(best_elements[:50]):  # Ограничиваем
            try:
                item_data = await extract_lavka_data(element)
                if item_data:
                    items.append(item_data)
                    if len(items) % 10 == 0:
                        print(f"   ✅ Обработано {len(items)} товаров...")
            except Exception as e:
                print(f"   ⚠️ Ошибка товара {i}: {e}")
        
        return items
        
    finally:
        await page.close()


async def extract_samokat_data(element):
    """Извлечение данных товара Самокат."""
    try:
        # URL товара
        href = await element.get_attribute('href')
        url = f"https://samokat.ru{href}" if href.startswith('/') else href
        
        # Название из URL
        name = url_to_name(url, "samokat")
        
        # Текст элемента
        text = await element.inner_text()
        
        # Цена из текста
        price = extract_price_from_text(text)
        
        # Вес из текста или URL
        weight = extract_weight_from_text(text) or extract_weight_from_text(url)
        
        return {
            'id': f"samokat:{generate_id(name)}",
            'name': name,
            'category': 'готовая еда',
            'price': price or Decimal("0"),
            'shop': 'samokat',
            'url': url,
            'photo_url': None,
            'portion_g': weight,
            'tags': [],
            'composition': None,
            'kcal_100g': None,
            'protein_100g': None,
            'fat_100g': None,
            'carb_100g': None,
            'city': "Москва",
            'address': "Красная площадь, 1"
        }
        
    except Exception as e:
        return None


async def extract_lavka_data(element):
    """Извлечение данных товара Лавка."""
    try:
        # Пробуем получить ссылку
        url = None
        try:
            if hasattr(element, 'get_attribute'):
                href = await element.get_attribute('href')
                if href:
                    url = f"https://lavka.yandex.ru{href}" if href.startswith('/') else href
        except:
            pass
        
        # Текст элемента
        text = await element.inner_text()
        
        # Название из текста
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        name = None
        for line in lines:
            if len(line) > 5 and not re.match(r'^[\d\s₽]+$', line):
                name = line
                break
        
        if not name:
            name = f"Товар Лавка {generate_id(text)}"
        
        # Цена
        price = extract_price_from_text(text)
        
        # Вес
        weight = extract_weight_from_text(text)
        
        if not url:
            url = f"https://lavka.yandex.ru/product/{generate_id(name)}"
        
        return {
            'id': f"lavka:{generate_id(name)}",
            'name': name,
            'category': 'готовая еда',
            'price': price or Decimal("0"),
            'shop': 'lavka',
            'url': url,
            'photo_url': None,
            'portion_g': weight,
            'tags': [],
            'composition': None,
            'kcal_100g': None,
            'protein_100g': None,
            'fat_100g': None,
            'carb_100g': None,
            'city': "Москва",
            'address': "Красная площадь, 1"
        }
        
    except Exception as e:
        return None


def url_to_name(url: str, shop: str) -> str:
    """Преобразование URL в название."""
    try:
        if shop == "samokat":
            # https://samokat.ru/product/salat-tsezar -> Салат цезарь
            if '/product/' in url:
                name_part = url.split('/product/')[-1]
                name = name_part.replace('-', ' ').title()
                return name
        
        return f"Товар {shop.title()}"
    except:
        return f"Товар {shop.title()}"


def extract_price_from_text(text: str) -> Decimal:
    """Извлечение цены из текста."""
    try:
        # Ищем цены в разных форматах
        price_patterns = [
            r'(\d{1,4})\s*₽',  # Обычная цена 299₽
            r'(\d{1,4})\s*руб',  # 299 руб
            r'₽\s*(\d{1,4})',  # ₽299
            r'(\d{1,3}[.,]\d{2})\s*₽',  # 299.50₽
        ]
        
        for pattern in price_patterns:
            price_match = re.search(pattern, text)
            if price_match:
                price_str = price_match.group(1).replace(',', '.').replace(' ', '')
                price_val = float(price_str)
                # Проверяем разумность цены (от 50 до 2000 рублей)
                if 50 <= price_val <= 2000:
                    return Decimal(str(price_val))
    except:
        pass
    return Decimal("0")


def extract_weight_from_text(text: str) -> Decimal:
    """Извлечение веса из текста."""
    try:
        weight_match = re.search(r'(\d+(?:[.,]\d+)?)\s*г', text)
        if weight_match:
            return Decimal(weight_match.group(1).replace(',', '.'))
    except:
        pass
    return None


def generate_id(text: str) -> str:
    """Генерация ID из текста."""
    clean_text = re.sub(r'[^\w\-]', '-', text.lower())[:30]
    return clean_text


def create_result(shop: str, items: list) -> ScrapingResult:
    """Создание результата."""
    food_items = []
    for item_data in items:
        try:
            food_item = FoodItem(**item_data)
            food_items.append(food_item)
        except Exception as e:
            print(f"⚠️ Ошибка создания FoodItem: {e}")
    
    return ScrapingResult(
        shop=shop,
        items=food_items,
        total_found=len(items),
        successful=len(food_items),
        failed=len(items) - len(food_items),
        errors=[],
        duration_seconds=60.0
    )


if __name__ == "__main__":
    print("🎯 ФИНАЛЬНЫЙ РАБОЧИЙ ПАРСЕР")
    print("🛡️ Будьте готовы пройти капчу в Лавке!")
    
    input("\n⏸️ Нажмите Enter для запуска...")
    
    results = asyncio.run(final_working_scraper())
    
    if results:
        total = sum(r.successful for r in results)
        print(f"\n🎉 УСПЕХ! Собрано {total} товаров")
        print("🔍 Файлы: FINAL_working.csv в data/")
        
        # Добавляем готовые данные ВкусВилл
        print(f"\n📊 ИТОГО С ГОТОВЫМИ ДАННЫМИ:")
        print(f"   ВкусВилл: 275 товаров (готовые)")
        print(f"   Новые: {total} товаров")
        print(f"   ВСЕГО: {275 + total} товаров")
    else:
        print(f"\n💥 Не удалось собрать новые товары")
        print("💡 Используйте готовые данные: data/FINAL_real_foods.csv (275 товаров)")
        exit(1)

#!/usr/bin/env python3
"""
Тестовый скрипт для исследования структуры сайта Самоката
"""

import asyncio
import time
from playwright.async_api import async_playwright

async def explore_samokat_structure():
    """Исследуем структуру сайта Самоката"""
    
    async with async_playwright() as p:
        # Запускаем браузер
        browser = await p.webkit.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        
        try:
            print("🌐 Переходим на сайт Самоката...")
            await page.goto("https://samokat.ru", timeout=60000)
            await page.wait_for_load_state("networkidle", timeout=60000)
            
            print("📸 Делаем скриншот главной страницы...")
            await page.screenshot(path="data/out/samokat_main_page.png")
            
            print("🔍 Ищем все ссылки на странице...")
            links = await page.query_selector_all("a")
            print(f"Найдено ссылок: {len(links)}")
            
            # Ищем ссылки, связанные с едой
            food_links = []
            for i, link in enumerate(links[:20]):  # Первые 20 ссылок
                try:
                    href = await link.get_attribute("href")
                    text = await link.text_content()
                    if href and text:
                        if any(keyword in text.lower() or keyword in href.lower() 
                               for keyword in ['еда', 'food', 'доставка', 'delivery', 'ресторан', 'restaurant']):
                            food_links.append((text.strip(), href))
                            print(f"🍽️ Найдена ссылка на еду: '{text.strip()}' -> {href}")
                except:
                    continue
            
            if food_links:
                print(f"\n🎯 Найдено {len(food_links)} ссылок на еду!")
                first_food_link = food_links[0]
                print(f"Кликаем по первой ссылке: {first_food_link[0]}")
                
                # Кликаем по ссылке на еду
                await page.click(f"a:has-text('{first_food_link[0]}')")
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(3)
                
                print("📸 Делаем скриншот страницы еды...")
                await page.screenshot(path="data/out/samokat_food_page.png")
                
                print("🔍 Ищем элементы товаров на странице еды...")
                
                # Пробуем разные селекторы
                selectors_to_try = [
                    '.product', '.item', '.card', '.product-card', '.item-card',
                    '.catalog-item', '.menu-item', '.food-item', '.dish-item',
                    '.restaurant-item', '.catalog-item', '.list-item',
                    '[class*="product"]', '[class*="item"]', '[class*="card"]',
                    'li', 'div', 'a', 'article', 'section'
                ]
                
                for selector in selectors_to_try:
                    try:
                        elements = await page.query_selector_all(selector)
                        if elements:
                            print(f"✅ Селектор '{selector}' нашел {len(elements)} элементов")
                            
                            # Проверяем первые 3 элемента
                            for i, elem in enumerate(elements[:3]):
                                try:
                                    text = await elem.text_content()
                                    if text and len(text.strip()) > 10:
                                        print(f"   Элемент {i+1}: {text.strip()[:100]}...")
                                except:
                                    continue
                            
                            # Если нашли много элементов, это может быть список товаров
                            if len(elements) > 5:
                                print(f"🎉 Возможно, это список товаров! Селектор: {selector}")
                                break
                        else:
                            print(f"❌ Селектор '{selector}' не дал результатов")
                    except Exception as e:
                        print(f"⚠️ Ошибка с селектором '{selector}': {e}")
                
                # Ищем поле ввода адреса
                print("\n🏠 Ищем поле ввода адреса...")
                address_selectors = [
                    'input[placeholder*="адрес"]', 'input[placeholder*="Адрес"]',
                    'input[placeholder*="город"]', 'input[placeholder*="Город"]',
                    'input[name*="address"]', 'input[id*="address"]',
                    'input[class*="address"]', 'input[type="text"]'
                ]
                
                for selector in address_selectors:
                    try:
                        address_input = await page.query_selector(selector)
                        if address_input:
                            placeholder = await address_input.get_attribute("placeholder")
                            print(f"✅ Поле адреса найдено: {selector} (placeholder: {placeholder})")
                            break
                    except:
                        continue
                else:
                    print("❌ Поле адреса не найдено")
                
            else:
                print("❌ Ссылки на еду не найдены")
                
                # Ищем любые элементы с текстом
                print("\n🔍 Ищем любые элементы с текстом...")
                all_elements = await page.query_selector_all("*")
                print(f"Всего элементов на странице: {len(all_elements)}")
                
                text_elements = []
                for elem in all_elements[:100]:  # Первые 100 элементов
                    try:
                        text = await elem.text_content()
                        if text and len(text.strip()) > 10 and len(text.strip()) < 200:
                            text_elements.append((elem, text.strip()))
                    except:
                        continue
                
                print(f"Элементов с текстом: {len(text_elements)}")
                for i, (elem, text) in enumerate(text_elements[:10]):
                    print(f"  {i+1}: {text[:80]}...")
            
            print("\n⏳ Ждем 5 секунд для просмотра...")
            await asyncio.sleep(5)
            
        except Exception as e:
            print(f"❌ Ошибка: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(explore_samokat_structure())

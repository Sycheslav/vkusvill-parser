"""Скрейпер для Самокат."""

import asyncio
import re
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
from decimal import Decimal
from playwright.async_api import Page
from loguru import logger
from bs4 import BeautifulSoup

from .base import BaseScraper


class SamokratScraper(BaseScraper):
    """Скрейпер для сайта Самокат."""
    
    @property
    def shop_name(self) -> str:
        return "samokat"
    
    @property
    def base_url(self) -> str:
        return "https://samokat.ru"
    
    async def _set_location(self) -> None:
        """Установка города и адреса в Самокат по рабочей логике."""
        page = await self.context.new_page()
        
        try:
            logger.info(f"Setting location to {self.config.city}, {self.config.address}")
            
            # Переходим на главную страницу
            await page.goto(self.base_url, wait_until="networkidle")
            logger.info("✅ Main page loaded")
            
            # Ждем 25 секунд как в оригинальном коде
            await asyncio.sleep(25)
            
            # Обновляем страницу как в оригинале
            await page.reload()
            await asyncio.sleep(2)
            logger.info("✅ Page refreshed")
            
            # Используем точный XPath из рабочего кода
            try:
                # Кликаем на адрес (XPath из оригинала)
                address_click = await page.wait_for_selector("xpath=/html/body/div/section/aside[2]/div/div/div[1]/div/span[1]", timeout=10000)
                await address_click.click()
                logger.info("✅ Address button clicked")
                await asyncio.sleep(3)
                
                # Кликаем на кнопку изменения адреса
                change_button = await page.wait_for_selector("xpath=/html/body/div[1]/div[2]/div[2]/nav/div/div[2]/div[82]/div", timeout=10000)
                change_button_el = await change_button.query_selector("button")
                await change_button_el.click()
                logger.info("✅ Change address button clicked")
                await asyncio.sleep(4)
            
                # Выбираем город из списка (логика из оригинала)
                towns_container = await page.wait_for_selector(".AddressCreation_root__rnHIH", timeout=10000)
                suggest_container = await towns_container.query_selector(".Suggest_suggestItems__wOpnt")
                town_items = await suggest_container.query_selector_all(".Suggest_suggestItem__ZDojM")
                
                logger.info(f"Found {len(town_items)} towns")
                
                # Ищем нужный город
                for town_item in town_items:
                    town_text = await town_item.inner_text()
                    logger.debug(f"Town option: {town_text}")
                    if self.config.city in town_text:
                        await town_item.click()
                        logger.info(f"✅ Selected city: {town_text}")
                        break
                
                await asyncio.sleep(5)
                
                # Вводим адрес (логика из оригинала)
                address_suggest = await page.wait_for_selector(".AddressSuggest_addressSuggest__6Y9nV", timeout=10000)
                address_input = await address_suggest.query_selector("input")
                await address_input.type(self.config.address, delay=100)
                logger.info(f"✅ Address typed: {self.config.address}")
                await asyncio.sleep(3)
                
                # Выбираем первый вариант из подсказок
                suggest_item = await address_suggest.wait_for_selector(".Suggest_suggestItem__ZDojM", timeout=10000)
                await suggest_item.click()
                logger.info("✅ Address suggestion selected")
                await asyncio.sleep(3)
                
                # Подтверждаем адрес кнопкой "Да, всё верно"
                buttons = await page.query_selector_all("button")
                for button in buttons:
                    button_text = await button.inner_text()
                    if "Да, всё верно" in button_text:
                        await button.click()
                        logger.info("✅ Address confirmed")
                        break
                
            except Exception as e:
                logger.warning(f"Could not set location using exact XPath: {e}")
                logger.warning("Address selection failed - continuing anyway")
            
            await asyncio.sleep(3)
            logger.info("Location setup completed")
            
            # Сохраняем состояние сессии для повторного использования
            try:
                await page.context.storage_state(path="samokat_session.json")
                logger.info("✅ Session state saved to samokat_session.json")
            except Exception as e:
                logger.warning(f"Could not save session state: {e}")
            
        except Exception as e:
            logger.warning(f"Could not set location automatically: {e}")
        finally:
            await page.close()
    
    async def _scrape_items(self) -> List[Dict[str, Any]]:
        """Получение списка товаров из категорий готовой еды."""
        
        # ПРЕДВАРИТЕЛЬНАЯ ПРОВЕРКА ДОСТУПНОСТИ САМОКАТА
        logger.info("🔍 Проверяем доступность Самоката...")
        test_page = await self.context.new_page()
        
        try:
            await test_page.goto("https://samokat.ru", wait_until="domcontentloaded", timeout=30000)
            title = await test_page.title()
            content = await test_page.content()
            
            # Логируем статус но продолжаем парсинг
            if any(phrase in content.lower() for phrase in [
                "мы сломались", "сломались", "ошибка", "недоступен",
                "технические работы", "попробуйте позже", "временно недоступен"
            ]) or "сломались" in title.lower():
                
                logger.warning("⚠️ САМОКАТ: Обнаружена техническая ошибка, но продолжаем парсинг")
                logger.info("📄 Заголовок страницы: " + title)
            
            logger.info("✅ Самокат доступен, продолжаем парсинг")
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки доступности Самоката: {e}")
            await test_page.close()
            return []
        finally:
            await test_page.close()
        
        all_items = []
        
        # Прямые ссылки на категории готовой еды Самокат
        category_urls = [
            "https://samokat.ru/category/vsya-gotovaya-eda-13",
            "https://samokat.ru/category/gotovaya-eda-25", 
            "https://samokat.ru/category/supy",
            "https://samokat.ru/category/salaty-i-zakuski",
            "https://samokat.ru/category/chto-na-zavtrak",
            "https://samokat.ru/category/gotovaya-eda-i-vypechka-6",
            "https://samokat.ru/category/stritfud-1",
            "https://samokat.ru/category/pochti-gotovo",
            "https://samokat.ru/category/bolshie-portsii"
        ]
        
        for category_url in category_urls:
            try:
                category_name = category_url.split('/')[-1]
                logger.info(f"Scraping category URL: {category_url}")
                category_items = await self._scrape_category_url(category_url, category_name)
                all_items.extend(category_items)
                
                # Задержка между категориями
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Failed to scrape category {category_url}: {e}")
        
        # Удаляем дубликаты по URL
        seen_urls = set()
        unique_items = []
        for item in all_items:
            if item.get('url') not in seen_urls:
                seen_urls.add(item.get('url'))
                unique_items.append(item)
        
        logger.info(f"Found {len(unique_items)} unique items from {len(all_items)} total")
        return unique_items
    
    async def _scrape_category_url(self, category_url: str, category_name: str) -> List[Dict[str, Any]]:
        """Скрейпинг товаров по прямому URL категории."""
        page = await self.context.new_page()
        items = []
        
        try:
            logger.info(f"🌐 Loading category URL: {category_url}")
            
            await page.goto(category_url, wait_until="networkidle", timeout=60000)
            logger.info(f"✅ Page loaded successfully")
            
            # Проверяем заголовок страницы
            title = await page.title()
            logger.info(f"📄 Page title: {title}")
            
            # ПРОВЕРКА НА ТЕХНИЧЕСКУЮ ОШИБКУ САМОКАТА
            page_content = await page.content()
            
            if any(phrase in page_content.lower() for phrase in [
                "мы сломались", "сломались", "ошибка", "недоступен", 
                "технические работы", "попробуйте позже"
            ]):
                logger.warning("⚠️ САМОКАТ: Техническая ошибка на сайте, но продолжаем парсинг")
            
            if "сломались" in title.lower() or "ошибка" in title.lower():
                logger.warning("⚠️ САМОКАТ: Техническая ошибка в заголовке, но продолжаем парсинг")
            
            # Ждем загрузки JavaScript и появления контента
            await asyncio.sleep(10)
            logger.info(f"⏱️ Waited 10 seconds for JavaScript to load")
            
            # Ждем появления карточек товаров с контентом
            logger.info(f"🔍 Waiting for product cards with content to appear...")
            
            # Ждем не просто элементы, а элементы с текстом
            content_loaded = False
            for attempt in range(10):
                try:
                    # Ищем элементы с текстом
                    # Ищем карточки с любым текстом
                    all_cards = await page.locator("[class*='Card']").all()
                    cards_with_text = []
                    for card in all_cards:
                        try:
                            text = await card.inner_text()
                            if text and len(text.strip()) > 3:
                                cards_with_text.append(card)
                        except:
                            continue
                    if cards_with_text:
                        logger.info(f"✅ Found {len(cards_with_text)} cards with content")
                        content_loaded = True
                        break
                    else:
                        logger.debug(f"   Attempt {attempt + 1}: waiting for content...")
                        await asyncio.sleep(2)
                except Exception as e:
                    logger.debug(f"   Attempt {attempt + 1}: error - {e}")
                    await asyncio.sleep(2)
            
            if not content_loaded:
                logger.warning(f"⚠️ No cards with content appeared after 20s")
            
            # Прокручиваем страницу для загрузки всех товаров
            await self._smart_scroll_to_load_all(page)
            
            # Извлекаем товары с рабочими селекторами для Самокат
            product_selectors = [
                # Рабочие селекторы из диагностики
                '[class*="CatalogTreeSectionCard"]',
                '[class*="Card"]',
                '[class*="Product"]',
                '[class*="product"]',
                'a[href*="/product/"]',
                'a[href*="/goods/"]',
                '[data-testid="product-card"]',
                '.product-card',
                '.catalog-item',
                '.goods-tile'
            ]
            
            products = []
            logger.info(f"🔍 Testing product selectors for Samokat...")
            
            for selector in product_selectors:
                try:
                    locator = page.locator(selector)
                    count = await locator.count()
                    logger.info(f"   {selector}: {count} elements")
                    if count > len(products):
                        products = await locator.all()
                        logger.info(f"✅ Best selector so far: {selector} with {len(products)} products")
                except Exception as e:
                    logger.warning(f"   {selector}: ERROR - {e}")
            
            if not products:
                logger.warning(f"❌ No products found with standard selectors")
                
                # Диагностика: проверяем содержимое страницы
                page_content = await page.content()
                logger.info(f"📄 Page content length: {len(page_content)} characters")
                
                if 'товар' in page_content.lower():
                    logger.info(f"✅ Page contains word 'товар'")
                else:
                    logger.warning(f"❌ Page does not contain word 'товар'")
                
                if 'product' in page_content.lower():
                    logger.info(f"✅ Page contains word 'product'")
                else:
                    logger.warning(f"❌ Page does not contain word 'product'")
                
                # Попробуем найти любые ссылки на товары
                logger.info(f"🔍 Looking for any product links...")
                link_selectors = ['a[href*="/product/"]', 'a[href*="/goods/"]', 'a[href*="/item/"]', 'a[href*="/catalog/"]']
                
                for link_selector in link_selectors:
                    try:
                        links = await page.query_selector_all(link_selector)
                        logger.info(f"   {link_selector}: {len(links)} links")
                        if links:
                            products = links[:20]  # Берем первые 20 для теста
                            break
                    except Exception as e:
                        logger.warning(f"   {link_selector}: ERROR - {e}")
                
                if not products:
                    logger.error(f"❌ No product links found at all!")
                    return items
            
            logger.info(f"Processing {len(products)} products from category {category_name}")
            
            for product in products:
                try:
                    item_data = await self._extract_product_data(product, page, category_name)
                    if item_data:
                        items.append(item_data)
                except Exception as e:
                    logger.warning(f"Failed to extract product data: {e}")
            
        except Exception as e:
            logger.error(f"Failed to scrape category {category_url}: {e}")
        finally:
            await page.close()
        
        return items
    
    async def _scrape_category(self, category: str) -> List[Dict[str, Any]]:
        """Скрейпинг товаров из одной категории."""
        page = await self.context.new_page()
        items = []
        
        try:
            # Формируем URL категории
            category_url = f"{self.base_url}/catalog/{category}"
            logger.debug(f"Loading category URL: {category_url}")
            
            await page.goto(category_url, wait_until="networkidle")
            await asyncio.sleep(3)
            
            # Прокручиваем страницу для загрузки всех товаров
            await self._scroll_to_load_all(page)
            
            # Извлекаем товары
            product_selectors = [
                # Рабочие селекторы из диагностики
                '[class*="CatalogTreeSectionCard"]',
                '[class*="Card"]',
                '[data-testid="product-card"]',
                '.product-card',
                '.catalog-item',
                '[class*="product"]',
                '.goods-tile'
            ]
            
            products = []
            for selector in product_selectors:
                try:
                    products = await page.query_selector_all(selector)
                    if products:
                        logger.debug(f"Found {len(products)} products with selector: {selector}")
                        break
                except:
                    continue
            
            if not products:
                logger.warning(f"No products found in category {category}")
                return items
            
            logger.info(f"Processing {len(products)} products from category {category}")
            
            for product in products:
                try:
                    item_data = await self._extract_product_data(product, page, category)
                    if item_data:
                        items.append(item_data)
                except Exception as e:
                    logger.warning(f"Failed to extract product data: {e}")
            
        except Exception as e:
            logger.error(f"Failed to scrape category {category}: {e}")
        finally:
            await page.close()
        
        return items
    
    async def _extract_product_data(self, product_element, page: Page, category: str) -> Optional[Dict[str, Any]]:
        """Извлечение данных товара из карточки."""
        try:
            # Название товара - расширенный поиск для Самокат
            name_selectors = [
                '[data-testid="product-name"]',
                '.product-name',
                '.product-title', 
                '.goods-tile__title',
                '[class*="title"]',
                '[class*="Title"]',
                '[class*="name"]',
                '[class*="Name"]',
                'h1', 'h2', 'h3', 'h4', 'h5',
                'a', # Иногда название в ссылке
                'span'
            ]
            
            name = None
            logger.debug(f"🏷️ Looking for product name...")
            
            # Сначала попробуем получить весь текст элемента
            try:
                all_text = await product_element.inner_text()
                logger.debug(f"📝 Full element text: {all_text[:100]}...")
            except:
                all_text = ""
            
            for selector in name_selectors:
                try:
                    locator = product_element.locator(selector)
                    if await locator.count() > 0:
                        name = await locator.first.inner_text()
                        if name and name.strip():
                            logger.debug(f"✅ Found name with {selector}: {name[:30]}...")
                            break
                except Exception as e:
                    logger.debug(f"   {selector}: failed - {e}")
            
            # Если название не найдено, попробуем использовать весь текст элемента
            if not name and all_text:
                # Очищаем текст и используем как название
                clean_text = all_text.strip().split('\n')[0]  # Берем первую строку
                if len(clean_text) > 3 and not clean_text.isdigit():
                    name = clean_text
                    logger.debug(f"✅ Using full element text as name: {name[:30]}...")
            
            if not name:
                logger.warning(f"❌ No name found for product")
                logger.debug(f"   Full element text was: {all_text[:200]}...")
                return None
            
            # Цена - используем классы из рабочего кода
            price_selectors = [
                '.ProductCardActions_text__3Uohy',  # Основной класс из оригинала
                '.ProductCardActions_oldPrice__d7vDY',  # Старая цена
                '[data-testid="product-price"]',
                '.product-price',
                '.price',
                '[class*="price"]',
                '[class*="Price"]',
                '[class*="ProductCard"]'
            ]
            
            price = None
            logger.debug(f"💰 Looking for product price...")
            
            for selector in price_selectors:
                try:
                    locator = product_element.locator(selector)
                    if await locator.count() > 0:
                        price_text = await locator.first.inner_text()
                        price = self._extract_price(price_text)
                        if price:
                            logger.debug(f"✅ Found price with {selector}: {price}")
                            break
                        else:
                            logger.debug(f"   {selector}: found element but no price in text: {price_text}")
                except Exception as e:
                    logger.debug(f"   {selector}: failed - {e}")
            
            # Если цена не найдена, ищем в общем тексте элемента
            if not price and all_text:
                price = self._extract_price(all_text)
                if price:
                    logger.debug(f"✅ Found price in full text: {price}")
            
            if not price:
                logger.warning(f"⚠️ No price found for product: {name} (will use 0)")
                logger.debug(f"   Full element text was: {all_text[:200]}...")
                price = Decimal("0")  # Не отбрасываем товар, ставим 0
            
            # Ссылка на товар
            link_selectors = [
                'a[href]',
                '[data-testid="product-link"] a',
                '.product-link'
            ]
            
            url = None
            for selector in link_selectors:
                try:
                    locator = product_element.locator(selector)
                    if await locator.count() > 0:
                        href = await locator.first.get_attribute('href')
                        if href:
                            url = urljoin(self.base_url, href)
                            logger.debug(f"✅ Found URL with {selector}: {url[:50]}...")
                            break
                except Exception as e:
                    logger.debug(f"   {selector}: failed - {e}")
            
            if not url:
                logger.warning(f"⚠️ No URL found for product: {name} (will generate)")
                url = f"{self.base_url}/product/unknown_{hash(name)}"
            
            # Изображение
            image_selectors = [
                'img[src]',
                '[data-testid="product-image"] img',
                '.product-image img'
            ]
            
            photo_url = None
            for selector in image_selectors:
                try:
                    img_element = await product_element.query_selector(selector)
                    if img_element:
                        src = await img_element.get_attribute('src')
                        if src:
                            photo_url = urljoin(self.base_url, src)
                            break
                except:
                    continue
            
            # Пытаемся извлечь вес из названия
            portion_g = self._extract_weight_from_text(name)
            if not portion_g:
                # Ищем вес в других элементах карточки
                weight_text = await self._extract_text_from_element(product_element, [
                    '[class*="weight"]', '[class*="Weight"]', 
                    '[class*="gram"]', '[class*="Gram"]',
                    '.weight', '.portion'
                ])
                if weight_text:
                    portion_g = self._extract_weight_from_text(weight_text)
            
            # Пытаемся извлечь дополнительные данные из видимого текста
            all_text = await product_element.inner_text() if product_element else ""
            
            # Ищем теги/особенности в тексте
            tags = self._extract_tags_from_text(all_text)
            
            # Генерируем ID из URL
            native_id = self._extract_id_from_url(url)
            
            return {
                'native_id': native_id,
                'name': name.strip(),
                'category': category,
                'price': price,
                'url': url,
                'photo_url': photo_url,
                'portion_g': portion_g,
                'tags': tags,
                'composition': None,
                'kcal_100g': None,
                'protein_100g': None,
                'fat_100g': None,
                'carb_100g': None
            }
            
        except Exception as e:
            logger.warning(f"Failed to extract product data: {e}")
            return None
    
    async def _enrich_item_details(self, item_data: Dict[str, Any]) -> Dict[str, Any]:
        """Обогащение данных товара переходом в карточку."""
        page = await self.context.new_page()
        
        try:
            logger.debug(f"Enriching item: {item_data.get('name')}")
            
            await page.goto(item_data['url'], wait_until="networkidle")
            await asyncio.sleep(2)
            
            # Извлекаем детальную информацию
            
            # Состав/ингредиенты
            composition_selectors = [
                '[data-testid="composition"]',
                '.composition',
                '.ingredients',
                '.product-composition',
                '*:has-text("Состав")',
                '*:has-text("Ингредиенты")'
            ]
            
            composition = await self._extract_text_by_selectors(page, composition_selectors)
            
            # Пищевая ценность
            nutrition_data = await self._extract_nutrition_info(page)
            
            # Вес порции
            portion_g = await self._extract_portion_weight(page)
            
            # Теги/особенности
            tags = await self._extract_tags(page)
            
            # Бренд
            brand_selectors = [
                '[data-testid="brand"]',
                '.brand',
                '.manufacturer',
                '*:has-text("Производитель")'
            ]
            brand = await self._extract_text_by_selectors(page, brand_selectors)
            
            # Штрихкод
            barcode_selectors = [
                '[data-testid="barcode"]',
                '.barcode',
                '*:has-text("Штрихкод")'
            ]
            barcode = await self._extract_text_by_selectors(page, barcode_selectors)
            
            # Обновляем данные
            item_data.update({
                'composition': composition,
                'portion_g': portion_g,
                'tags': tags,
                'brand': brand,
                'barcode': barcode,
                **nutrition_data
            })
            
        except Exception as e:
            logger.warning(f"Failed to enrich item {item_data.get('url')}: {e}")
        finally:
            await page.close()
        
        return item_data
    
    async def _extract_nutrition_info(self, page: Page) -> Dict[str, Any]:
        """Извлечение информации о пищевой ценности."""
        nutrition_data = {}
        
        # Ищем блок с пищевой ценностью
        nutrition_selectors = [
            '[data-testid="nutrition"]',
            '.nutrition',
            '.nutritional-value',
            '.energy-value',
            '*:has-text("Пищевая ценность")',
            '*:has-text("Энергетическая ценность")'
        ]
        
        nutrition_block = None
        for selector in nutrition_selectors:
            try:
                nutrition_block = await page.query_selector(selector)
                if nutrition_block:
                    break
            except:
                continue
        
        if nutrition_block:
            # Извлекаем текст из блока
            nutrition_text = await nutrition_block.inner_text()
            
            # Парсим калории
            kcal_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:ккал|kcal)', nutrition_text, re.IGNORECASE)
            if kcal_match:
                nutrition_data['kcal_100g'] = Decimal(kcal_match.group(1).replace(',', '.'))
            
            # Парсим белки
            protein_match = re.search(r'белк[и|а].*?(\d+(?:[.,]\d+)?)', nutrition_text, re.IGNORECASE)
            if protein_match:
                nutrition_data['protein_100g'] = Decimal(protein_match.group(1).replace(',', '.'))
            
            # Парсим жиры
            fat_match = re.search(r'жир[ы|а].*?(\d+(?:[.,]\d+)?)', nutrition_text, re.IGNORECASE)
            if fat_match:
                nutrition_data['fat_100g'] = Decimal(fat_match.group(1).replace(',', '.'))
            
            # Парсим углеводы
            carb_match = re.search(r'углевод[ы|а].*?(\d+(?:[.,]\d+)?)', nutrition_text, re.IGNORECASE)
            if carb_match:
                nutrition_data['carb_100g'] = Decimal(carb_match.group(1).replace(',', '.'))
        
        # Дополнительно ищем отдельные элементы
        nutrient_patterns = {
            'kcal_100g': [r'(\d+(?:[.,]\d+)?)\s*(?:ккал|kcal)', 'калории', 'энергетическая ценность'],
            'protein_100g': [r'(\d+(?:[.,]\d+)?)\s*г.*белк', 'белки', 'protein'],
            'fat_100g': [r'(\d+(?:[.,]\d+)?)\s*г.*жир', 'жиры', 'fat'],
            'carb_100g': [r'(\d+(?:[.,]\d+)?)\s*г.*углевод', 'углеводы', 'carbohydrates']
        }
        
        page_content = await page.content()
        
        for nutrient, patterns in nutrient_patterns.items():
            if nutrient not in nutrition_data:
                for pattern in patterns:
                    if isinstance(pattern, str):
                        # Ищем по тексту
                        elements = await page.query_selector_all(f'*:has-text("{pattern}")')
                        for element in elements:
                            try:
                                text = await element.inner_text()
                                number_match = re.search(r'(\d+(?:[.,]\d+)?)', text)
                                if number_match:
                                    nutrition_data[nutrient] = Decimal(number_match.group(1).replace(',', '.'))
                                    break
                            except:
                                continue
                    else:
                        # Регулярное выражение
                        match = re.search(pattern, page_content, re.IGNORECASE)
                        if match:
                            nutrition_data[nutrient] = Decimal(match.group(1).replace(',', '.'))
                            break
        
        return nutrition_data
    
    async def _extract_portion_weight(self, page: Page) -> Optional[Decimal]:
        """Извлечение веса порции."""
        weight_selectors = [
            '[data-testid="weight"]',
            '.weight',
            '.portion-weight',
            '*:has-text("Вес")',
            '*:has-text("Масса")'
        ]
        
        weight_text = await self._extract_text_by_selectors(page, weight_selectors)
        
        if weight_text:
            # Ищем вес в граммах
            weight_match = re.search(r'(\d+(?:[.,]\d+)?)\s*г', weight_text)
            if weight_match:
                return Decimal(weight_match.group(1).replace(',', '.'))
        
        # Ищем в названии товара
        try:
            title = await page.title()
            weight_match = re.search(r'(\d+)\s*г', title)
            if weight_match:
                return Decimal(weight_match.group(1))
        except:
            pass
        
        return None
    
    async def _extract_tags(self, page: Page) -> List[str]:
        """Извлечение тегов товара."""
        tags = []
        
        tag_selectors = [
            '[data-testid="tags"]',
            '.tags',
            '.labels',
            '.badges',
            '.product-labels'
        ]
        
        for selector in tag_selectors:
            try:
                tag_elements = await page.query_selector_all(f'{selector} *')
                for element in tag_elements:
                    tag_text = await element.inner_text()
                    if tag_text and len(tag_text.strip()) > 1:
                        tags.append(tag_text.strip().lower())
            except:
                continue
        
        # Ищем ключевые слова в описании
        try:
            page_content = await page.content()
            keywords = ['острое', 'острый', 'вегетарианский', 'веган', 'пп', 'диетический', 'без глютена', 'без лактозы']
            for keyword in keywords:
                if keyword in page_content.lower():
                    tags.append(keyword)
        except:
            pass
        
        return list(set(tags))  # Удаляем дубликаты
    
    async def _smart_scroll_to_load_all(self, page: Page) -> None:
        """Умная прокрутка с ожиданием появления новых карточек."""
        logger.info("🔄 Smart scrolling to load all products...")
        
        prev_count = 0
        stable_rounds = 0
        max_rounds = 20
        
        for round_num in range(max_rounds):
            # Прокручиваем вниз
            await page.mouse.wheel(0, 4000)
            await asyncio.sleep(1)
            
            # Считаем карточки
            try:
                grid = page.locator("[class*='Card'], [class*='Product'], .product-card, .goods-tile")
                current_count = await grid.count()
                
                logger.debug(f"   Round {round_num + 1}: {current_count} cards")
                
                if current_count > prev_count:
                    prev_count = current_count
                    stable_rounds = 0
                    logger.debug(f"   ✅ New cards loaded: {current_count}")
                else:
                    stable_rounds += 1
                    logger.debug(f"   ⏸️ No new cards: stable round {stable_rounds}")
                
                # Если 3 раунда без новых карточек - останавливаемся
                if stable_rounds >= 3:
                    logger.info(f"✅ Scrolling completed: {current_count} cards loaded")
                    break
                    
            except Exception as e:
                logger.warning(f"Error during scroll round {round_num + 1}: {e}")
                break
        
        logger.info(f"🏁 Smart scrolling finished after {round_num + 1} rounds")
    
    async def _extract_text_by_selectors(self, page: Page, selectors: List[str]) -> Optional[str]:
        """Извлечение текста по списку селекторов."""
        for selector in selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    text = await element.inner_text()
                    if text and text.strip():
                        return text.strip()
            except:
                continue
        return None
    
    def _extract_price(self, price_text: str) -> Optional[Decimal]:
        """Извлечение цены из текста."""
        if not price_text:
            return None
        
        # Убираем все кроме цифр, точек и запятых
        clean_price = re.sub(r'[^\d.,]', '', price_text)
        
        # Ищем число
        price_match = re.search(r'(\d+(?:[.,]\d+)?)', clean_price)
        if price_match:
            price_str = price_match.group(1).replace(',', '.')
            try:
                return Decimal(price_str)
            except:
                pass
        
        return None
    
    def _extract_weight_from_text(self, text: str) -> Optional[Decimal]:
        """Извлечение веса из текста."""
        if not text:
            return None
        
        # Ищем вес в граммах
        weight_patterns = [
            r'(\d+(?:[.,]\d+)?)\s*г\b',
            r'(\d+(?:[.,]\d+)?)\s*гр\b',
            r'(\d+(?:[.,]\d+)?)\s*gram\b',
            r'(\d+(?:[.,]\d+)?)\s*g\b'
        ]
        
        for pattern in weight_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    return Decimal(match.group(1).replace(',', '.'))
                except:
                    continue
        
        return None
    
    def _extract_tags_from_text(self, text: str) -> List[str]:
        """Извлечение тегов из текста."""
        if not text:
            return []
        
        tags = []
        text_lower = text.lower()
        
        # Ключевые слова для тегов
        tag_keywords = {
            'острое': ['острый', 'острая', 'острое', 'перец', 'чили'],
            'вегетарианское': ['вегетарианский', 'вегетарианская', 'веган'],
            'диетическое': ['диетический', 'диетическая', 'пп', 'фитнес'],
            'без глютена': ['без глютена', 'безглютеновый'],
            'без лактозы': ['без лактозы', 'безлактозный'],
            'органическое': ['органический', 'эко', 'био'],
            'домашнее': ['домашний', 'домашняя', 'фермерский']
        }
        
        for tag, keywords in tag_keywords.items():
            if any(keyword in text_lower for keyword in keywords):
                tags.append(tag)
        
        return tags
    
    async def _extract_text_from_element(self, element, selectors: List[str]) -> Optional[str]:
        """Извлечение текста из элемента по селекторам."""
        for selector in selectors:
            try:
                sub_element = await element.query_selector(selector)
                if sub_element:
                    text = await sub_element.inner_text()
                    if text and text.strip():
                        return text.strip()
            except:
                continue
        return None
    
    def _extract_id_from_url(self, url: str) -> str:
        """Извлечение ID товара из URL."""
        # Ищем ID в URL
        id_match = re.search(r'/product/([^/]+)', url)
        if id_match:
            return id_match.group(1)
        
        # Если не найден, используем последний сегмент пути
        parsed = urlparse(url)
        path_parts = [p for p in parsed.path.split('/') if p]
        if path_parts:
            return path_parts[-1]
        
        # В крайнем случае используем хеш от URL
        return super()._generate_id_from_url(url)

"""Скрейпер для Яндекс Лавка."""

import asyncio
import re
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
from decimal import Decimal
from playwright.async_api import Page
from loguru import logger

from .base import BaseScraper


class LavkaScraper(BaseScraper):
    """Скрейпер для сайта Яндекс Лавка."""
    
    @property
    def shop_name(self) -> str:
        return "lavka"
    
    @property
    def base_url(self) -> str:
        return "https://lavka.yandex.ru"
    
    async def _set_location(self) -> None:
        """Установка города и адреса в Яндекс Лавка."""
        page = await self.context.new_page()
        
        try:
            logger.info(f"Setting location to {self.config.city}, {self.config.address}")
            
            # Переходим на главную страницу
            await page.goto(self.base_url, wait_until="networkidle")
            await asyncio.sleep(3)
            
            # Ищем модальное окно выбора адреса
            address_modal_selectors = [
                '[data-testid="address-modal"]',
                '.address-modal',
                '.location-modal',
                '[class*="modal"]',
                '[role="dialog"]'
            ]
            
            modal_found = False
            for selector in address_modal_selectors:
                try:
                    modal = await page.wait_for_selector(selector, timeout=5000)
                    if modal:
                        modal_found = True
                        break
                except:
                    continue
            
            if not modal_found:
                # Ищем кнопку выбора адреса
                address_button_selectors = [
                    '[data-testid="address-button"]',
                    '.address-button',
                    'button[class*="address"]',
                    'button:has-text("Выберите адрес")',
                    'button:has-text("Адрес доставки")'
                ]
                
                for selector in address_button_selectors:
                    try:
                        button = await page.wait_for_selector(selector, timeout=3000)
                        if button:
                            await button.click()
                            await asyncio.sleep(2)
                            break
                    except:
                        continue
            
            # Ищем поле ввода адреса
            address_input_selectors = [
                'input[placeholder*="адрес"]',
                'input[placeholder*="Адрес"]',
                'input[name="address"]',
                'input[data-testid="address-input"]',
                '.address-input input',
                'input[type="text"]'
            ]
            
            address_input = None
            for selector in address_input_selectors:
                try:
                    address_input = await page.wait_for_selector(selector, timeout=5000)
                    if address_input:
                        break
                except:
                    continue
            
            if address_input:
                # Очищаем поле и вводим адрес
                await address_input.click()
                await address_input.fill("")
                full_address = f"{self.config.city}, {self.config.address}"
                await address_input.type(full_address, delay=100)
                await asyncio.sleep(3)
                
                # Ждем появления подсказок и кликаем на первую
                suggestion_selectors = [
                    '[data-testid="address-suggestion"]',
                    '.address-suggestion',
                    '.suggestion',
                    '.dropdown-item',
                    '.autocomplete-item',
                    '[role="option"]'
                ]
                
                suggestion_clicked = False
                for selector in suggestion_selectors:
                    try:
                        suggestions = await page.wait_for_selector(selector, timeout=5000)
                        if suggestions:
                            await suggestions.click()
                            suggestion_clicked = True
                            break
                    except:
                        continue
                
                if not suggestion_clicked:
                    # Если подсказки не найдены, просто нажимаем Enter
                    await address_input.press('Enter')
                
                await asyncio.sleep(2)
                
                # Ищем кнопку подтверждения
                confirm_selectors = [
                    'button:has-text("Подтвердить")',
                    'button:has-text("Выбрать")',
                    'button:has-text("Готово")',
                    'button:has-text("Сохранить")',
                    '[data-testid="confirm-button"]',
                    '.confirm-button'
                ]
                
                for selector in confirm_selectors:
                    try:
                        confirm_btn = await page.wait_for_selector(selector, timeout=3000)
                        if confirm_btn:
                            await confirm_btn.click()
                            break
                    except:
                        continue
            
            await asyncio.sleep(5)
            logger.info("Location set successfully")
            
        except Exception as e:
            logger.warning(f"Could not set location automatically: {e}")
        finally:
            await page.close()
    
    async def _scrape_items(self) -> List[Dict[str, Any]]:
        """Получение списка товаров из категорий готовой еды."""
        all_items = []
        
        # Прямые ссылки на категории готовой еды Яндекс Лавка
        category_urls = [
            "https://lavka.yandex.ru/category/gotovaya_eda",
            "https://lavka.yandex.ru/category/hot_streetfood", 
            "https://lavka.yandex.ru/category/gotovaya_eda/ostroe-1"
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
            
            await page.goto(category_url, wait_until="domcontentloaded", timeout=60000)
            logger.info(f"✅ Page loaded successfully")
            
            # Проверяем заголовок страницы
            title = await page.title()
            logger.info(f"📄 Page title: {title}")
            
            await asyncio.sleep(5)
            logger.info(f"⏱️ Waited 5 seconds for page to settle")
            
            # Прокручиваем страницу для загрузки всех товаров
            await self._scroll_to_load_all(page)
            
            # Извлекаем товары с рабочими селекторами для Лавка
            product_selectors = [
                '[class*="Product"]',
                '[class*="Card"]',
                '[class*="product"]',
                'a[href*="/product/"]',
                'a[href*="/goods/"]',
                '[data-testid="product-card"]',
                '[data-testid="product"]',
                '.product-card',
                '.product',
                '.catalog-item'
            ]
            
            products = []
            logger.info(f"🔍 Testing product selectors for Lavka...")
            
            for selector in product_selectors:
                try:
                    test_products = await page.query_selector_all(selector)
                    logger.info(f"   {selector}: {len(test_products)} elements")
                    if test_products and len(test_products) > len(products):
                        products = test_products
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
                
                # Fallback: ищем любые ссылки на товары
                logger.info(f"🔍 Looking for any product links...")
                link_selectors = ['a[href*="/product/"]', 'a[href*="/goods/"]', 'a[href*="/item/"]']
                
                for link_selector in link_selectors:
                    try:
                        links = await page.query_selector_all(link_selector)
                        logger.info(f"   {link_selector}: {len(links)} links")
                        if links:
                            products = links[:20]
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
                '[data-testid="product-card"]',
                '[data-testid="product"]',
                '.product-card',
                '.product',
                '[class*="ProductCard"]',
                '[class*="product"]'
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
            # Название товара - расширенный поиск для Лавка
            name_selectors = [
                '[data-testid="product-name"]',
                '[data-testid="product-title"]',
                '.product-name',
                '.product-title',
                '[class*="title"]',
                '[class*="Title"]',
                '[class*="name"]',
                '[class*="Name"]',
                'h1', 'h2', 'h3', 'h4', 'h5',
                'a',
                'span'
            ]
            
            name = None
            for selector in name_selectors:
                try:
                    name_element = await product_element.query_selector(selector)
                    if name_element:
                        name = await name_element.inner_text()
                        if name and name.strip():
                            break
                except:
                    continue
            
            if not name:
                return None
            
            # Цена - расширенный поиск для Лавка
            price_selectors = [
                '[data-testid="product-price"]',
                '[data-testid="price"]',
                '.product-price',
                '.price',
                '[class*="Price"]',
                '[class*="price"]',
                '[class*="cost"]',
                '[class*="Cost"]',
                'span:contains("₽")',
                'div:contains("₽")'
            ]
            
            price = None
            for selector in price_selectors:
                try:
                    price_element = await product_element.query_selector(selector)
                    if price_element:
                        price_text = await price_element.inner_text()
                        price = self._extract_price(price_text)
                        if price:
                            break
                except:
                    continue
            
            if not price:
                return None
            
            # Ссылка на товар
            link_selectors = [
                'a[href]',
                '[data-testid="product-link"]'
            ]
            
            url = None
            for selector in link_selectors:
                try:
                    link_element = await product_element.query_selector(selector)
                    if link_element:
                        href = await link_element.get_attribute('href')
                        if href:
                            url = urljoin(self.base_url, href)
                            break
                except:
                    continue
            
            # Если ссылка не найдена, возможно нужно кликнуть на карточку
            if not url:
                try:
                    # Получаем текущий URL
                    current_url = page.url
                    
                    # Кликаем на карточку
                    await product_element.click()
                    await asyncio.sleep(1)
                    
                    # Проверяем, изменился ли URL
                    new_url = page.url
                    if new_url != current_url:
                        url = new_url
                        # Возвращаемся назад
                        await page.go_back()
                        await asyncio.sleep(1)
                except:
                    pass
            
            if not url:
                return None
            
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
                        if src and 'data:' not in src:  # Исключаем base64 изображения
                            if src.startswith('//'):
                                photo_url = f"https:{src}"
                            elif src.startswith('/'):
                                photo_url = urljoin(self.base_url, src)
                            else:
                                photo_url = src
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
            await asyncio.sleep(3)
            
            # Извлекаем детальную информацию
            
            # Состав/ингредиенты
            composition = await self._extract_composition(page)
            
            # Пищевая ценность
            nutrition_data = await self._extract_nutrition_info(page)
            
            # Вес порции
            portion_g = await self._extract_portion_weight(page)
            
            # Теги/особенности
            tags = await self._extract_tags(page)
            
            # Дополнительная информация
            brand = await self._extract_brand(page)
            barcode = await self._extract_barcode(page)
            shelf_life = await self._extract_shelf_life(page)
            
            # Обновляем данные
            item_data.update({
                'composition': composition,
                'portion_g': portion_g,
                'tags': tags,
                'brand': brand,
                'barcode': barcode,
                'shelf_life': shelf_life,
                **nutrition_data
            })
            
        except Exception as e:
            logger.warning(f"Failed to enrich item {item_data.get('url')}: {e}")
        finally:
            await page.close()
        
        return item_data
    
    async def _extract_composition(self, page: Page) -> Optional[str]:
        """Извлечение состава."""
        composition_selectors = [
            '[data-testid="composition"]',
            '[data-testid="ingredients"]',
            '.composition',
            '.ingredients',
            '*:has-text("Состав")',
            '*:has-text("Ингредиенты")'
        ]
        
        for selector in composition_selectors:
            try:
                if ':has-text(' in selector:
                    # Для селекторов с текстом ищем родительский элемент
                    elements = await page.query_selector_all(selector)
                    for element in elements:
                        parent = await element.query_selector('xpath=..')
                        if parent:
                            text = await parent.inner_text()
                            if 'состав' in text.lower() or 'ингредиент' in text.lower():
                                # Извлекаем только часть с составом
                                lines = text.split('\n')
                                for i, line in enumerate(lines):
                                    if 'состав' in line.lower() or 'ингредиент' in line.lower():
                                        composition_lines = lines[i+1:]
                                        composition = '\n'.join(composition_lines).strip()
                                        if composition:
                                            return composition
                else:
                    element = await page.query_selector(selector)
                    if element:
                        text = await element.inner_text()
                        if text and text.strip():
                            return text.strip()
            except:
                continue
        
        return None
    
    async def _extract_nutrition_info(self, page: Page) -> Dict[str, Any]:
        """Извлечение информации о пищевой ценности."""
        nutrition_data = {}
        
        # Ищем блок с пищевой ценностью
        nutrition_selectors = [
            '[data-testid="nutrition"]',
            '[data-testid="nutritional-value"]',
            '.nutrition',
            '.nutritional-value',
            '.energy-value',
            '*:has-text("Пищевая ценность")',
            '*:has-text("Энергетическая ценность")',
            '*:has-text("на 100")'
        ]
        
        nutrition_text = ""
        
        for selector in nutrition_selectors:
            try:
                if ':has-text(' in selector:
                    elements = await page.query_selector_all(selector)
                    for element in elements:
                        parent = await element.query_selector('xpath=..')
                        if parent:
                            text = await parent.inner_text()
                            if any(keyword in text.lower() for keyword in ['пищевая', 'энергетическая', 'калор', 'белк', 'жир', 'углевод']):
                                nutrition_text += text + "\n"
                else:
                    element = await page.query_selector(selector)
                    if element:
                        text = await element.inner_text()
                        if text:
                            nutrition_text += text + "\n"
            except:
                continue
        
        if not nutrition_text:
            # Ищем в общем контенте страницы
            try:
                page_content = await page.content()
                nutrition_text = page_content
            except:
                pass
        
        if nutrition_text:
            # Парсим нутриенты
            
            # Калории
            kcal_patterns = [
                r'(\d+(?:[.,]\d+)?)\s*(?:ккал|kcal)',
                r'калор.*?(\d+(?:[.,]\d+)?)',
                r'энергет.*?(\d+(?:[.,]\d+)?)'
            ]
            
            for pattern in kcal_patterns:
                match = re.search(pattern, nutrition_text, re.IGNORECASE)
                if match:
                    nutrition_data['kcal_100g'] = Decimal(match.group(1).replace(',', '.'))
                    break
            
            # Белки
            protein_patterns = [
                r'белк[и|а].*?(\d+(?:[.,]\d+)?)',
                r'(\d+(?:[.,]\d+)?)\s*г.*белк',
                r'protein.*?(\d+(?:[.,]\d+)?)'
            ]
            
            for pattern in protein_patterns:
                match = re.search(pattern, nutrition_text, re.IGNORECASE)
                if match:
                    nutrition_data['protein_100g'] = Decimal(match.group(1).replace(',', '.'))
                    break
            
            # Жиры
            fat_patterns = [
                r'жир[ы|а].*?(\d+(?:[.,]\d+)?)',
                r'(\d+(?:[.,]\d+)?)\s*г.*жир',
                r'fat.*?(\d+(?:[.,]\d+)?)'
            ]
            
            for pattern in fat_patterns:
                match = re.search(pattern, nutrition_text, re.IGNORECASE)
                if match:
                    nutrition_data['fat_100g'] = Decimal(match.group(1).replace(',', '.'))
                    break
            
            # Углеводы
            carb_patterns = [
                r'углевод[ы|а].*?(\d+(?:[.,]\d+)?)',
                r'(\d+(?:[.,]\d+)?)\s*г.*углевод',
                r'carb.*?(\d+(?:[.,]\d+)?)'
            ]
            
            for pattern in carb_patterns:
                match = re.search(pattern, nutrition_text, re.IGNORECASE)
                if match:
                    nutrition_data['carb_100g'] = Decimal(match.group(1).replace(',', '.'))
                    break
        
        return nutrition_data
    
    async def _extract_portion_weight(self, page: Page) -> Optional[Decimal]:
        """Извлечение веса порции."""
        # Ищем вес в различных местах
        weight_selectors = [
            '[data-testid="weight"]',
            '.weight',
            '.portion-weight',
            '*:has-text("Вес")',
            '*:has-text("Масса")',
            '*:has-text(" г")'
        ]
        
        for selector in weight_selectors:
            try:
                if ':has-text(' in selector:
                    elements = await page.query_selector_all(selector)
                    for element in elements:
                        text = await element.inner_text()
                        weight_match = re.search(r'(\d+(?:[.,]\d+)?)\s*г', text)
                        if weight_match:
                            return Decimal(weight_match.group(1).replace(',', '.'))
                else:
                    element = await page.query_selector(selector)
                    if element:
                        text = await element.inner_text()
                        weight_match = re.search(r'(\d+(?:[.,]\d+)?)\s*г', text)
                        if weight_match:
                            return Decimal(weight_match.group(1).replace(',', '.'))
            except:
                continue
        
        # Ищем в заголовке страницы
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
        
        # Ищем теги в специальных элементах
        tag_selectors = [
            '[data-testid="tags"]',
            '[data-testid="labels"]',
            '.tags',
            '.labels',
            '.badges'
        ]
        
        for selector in tag_selectors:
            try:
                elements = await page.query_selector_all(f'{selector} *')
                for element in elements:
                    text = await element.inner_text()
                    if text and len(text.strip()) > 1:
                        tags.append(text.strip().lower())
            except:
                continue
        
        # Ищем ключевые слова в содержимом
        try:
            page_content = await page.content()
            keywords = ['острое', 'острый', 'вегетарианский', 'веган', 'пп', 'диетический', 
                       'без глютена', 'без лактозы', 'органический', 'эко', 'bio']
            for keyword in keywords:
                if keyword in page_content.lower():
                    tags.append(keyword)
        except:
            pass
        
        return list(set(tags))
    
    async def _extract_brand(self, page: Page) -> Optional[str]:
        """Извлечение бренда."""
        brand_selectors = [
            '[data-testid="brand"]',
            '.brand',
            '.manufacturer',
            '*:has-text("Производитель")',
            '*:has-text("Бренд")'
        ]
        
        return await self._extract_text_by_selectors(page, brand_selectors)
    
    async def _extract_barcode(self, page: Page) -> Optional[str]:
        """Извлечение штрихкода."""
        barcode_selectors = [
            '[data-testid="barcode"]',
            '.barcode',
            '*:has-text("Штрихкод")'
        ]
        
        return await self._extract_text_by_selectors(page, barcode_selectors)
    
    async def _extract_shelf_life(self, page: Page) -> Optional[str]:
        """Извлечение срока годности."""
        shelf_life_selectors = [
            '[data-testid="shelf-life"]',
            '.shelf-life',
            '*:has-text("Срок годности")',
            '*:has-text("Годен до")'
        ]
        
        return await self._extract_text_by_selectors(page, shelf_life_selectors)
    
    async def _extract_text_by_selectors(self, page: Page, selectors: List[str]) -> Optional[str]:
        """Извлечение текста по списку селекторов."""
        for selector in selectors:
            try:
                if ':has-text(' in selector:
                    elements = await page.query_selector_all(selector)
                    for element in elements:
                        parent = await element.query_selector('xpath=..')
                        if parent:
                            text = await parent.inner_text()
                            if text and text.strip():
                                # Извлекаем релевантную часть
                                lines = text.split('\n')
                                for line in lines:
                                    if len(line.strip()) > 2 and not any(word in line.lower() for word in ['производитель', 'бренд', 'штрихкод', 'срок']):
                                        return line.strip()
                else:
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
        # Ищем ID в URL Лавки
        id_patterns = [
            r'/product/([^/?]+)',
            r'/([^/?]+)/?$',
            r'id=([^&]+)'
        ]
        
        for pattern in id_patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        # В крайнем случае используем хеш от URL
        return super()._generate_id_from_url(url)
    
    async def _smart_scroll_to_load_all(self, page) -> None:
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
                grid = page.locator("[class*='Card'], [class*='Product'], .product-card")
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

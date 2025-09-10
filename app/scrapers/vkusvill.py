"""Скрейпер для ВкусВилл."""

import asyncio
import re
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
from decimal import Decimal
from playwright.async_api import Page
from loguru import logger

from .base import BaseScraper


class VkusvillScraper(BaseScraper):
    """Скрейпер для сайта ВкусВилл."""
    
    @property
    def shop_name(self) -> str:
        return "vkusvill"
    
    @property
    def base_url(self) -> str:
        return "https://vkusvill.ru"
    
    async def _set_location(self) -> None:
        """Установка города и адреса в ВкусВилл."""
        page = await self.context.new_page()
        
        try:
            logger.info(f"Setting location to {self.config.city}, {self.config.address}")
            
            # Переходим на главную страницу
            await page.goto(self.base_url, wait_until="networkidle")
            await asyncio.sleep(3)
            
            # Ищем кнопку выбора города
            city_button_selectors = [
                '[data-testid="city-selector"]',
                '.city-selector',
                '.city-button',
                'button[class*="city"]',
                'button:has-text("Москва")',
                'button:has-text("Выберите город")',
                '.header__city'
            ]
            
            city_button = None
            for selector in city_button_selectors:
                try:
                    city_button = await page.wait_for_selector(selector, timeout=3000)
                    if city_button:
                        break
                except:
                    continue
            
            if city_button:
                await city_button.click()
                await asyncio.sleep(2)
                
                # Ищем список городов или поле поиска
                city_search_selectors = [
                    'input[placeholder*="город"]',
                    'input[placeholder*="Город"]',
                    '.city-search input',
                    'input[type="text"]'
                ]
                
                city_input = None
                for selector in city_search_selectors:
                    try:
                        city_input = await page.wait_for_selector(selector, timeout=3000)
                        if city_input:
                            break
                    except:
                        continue
                
                if city_input:
                    # Вводим название города
                    await city_input.click()
                    await city_input.fill("")
                    await city_input.type(self.config.city, delay=100)
                    await asyncio.sleep(2)
                    
                    # Ищем город в списке
                    city_option_selectors = [
                        f'*:has-text("{self.config.city}")',
                        '.city-option',
                        '.dropdown-item',
                        '[role="option"]'
                    ]
                    
                    for selector in city_option_selectors:
                        try:
                            city_option = await page.wait_for_selector(selector, timeout=3000)
                            if city_option:
                                await city_option.click()
                                break
                        except:
                            continue
                else:
                    # Ищем город в статическом списке
                    city_list_selectors = [
                        f'button:has-text("{self.config.city}")',
                        f'a:has-text("{self.config.city}")',
                        f'*:has-text("{self.config.city}")'
                    ]
                    
                    for selector in city_list_selectors:
                        try:
                            city_option = await page.wait_for_selector(selector, timeout=3000)
                            if city_option:
                                await city_option.click()
                                break
                        except:
                            continue
                
                await asyncio.sleep(2)
            
            # Теперь ищем выбор магазина/адреса
            store_button_selectors = [
                '[data-testid="store-selector"]',
                '.store-selector',
                '.store-button',
                'button[class*="store"]',
                'button:has-text("Выберите магазин")',
                'button:has-text("Адрес")'
            ]
            
            store_button = None
            for selector in store_button_selectors:
                try:
                    store_button = await page.wait_for_selector(selector, timeout=3000)
                    if store_button:
                        break
                except:
                    continue
            
            if store_button:
                await store_button.click()
                await asyncio.sleep(2)
                
                # Ищем поле поиска адреса или первый магазин в списке
                address_input_selectors = [
                    'input[placeholder*="адрес"]',
                    'input[placeholder*="Адрес"]',
                    '.address-search input',
                    'input[type="text"]'
                ]
                
                address_input = None
                for selector in address_input_selectors:
                    try:
                        address_input = await page.wait_for_selector(selector, timeout=3000)
                        if address_input:
                            break
                    except:
                        continue
                
                if address_input:
                    # Вводим адрес
                    await address_input.click()
                    await address_input.fill("")
                    await address_input.type(self.config.address, delay=100)
                    await asyncio.sleep(2)
                    
                    # Выбираем первый вариант
                    suggestion_selectors = [
                        '.suggestion',
                        '.address-suggestion',
                        '.dropdown-item',
                        '[role="option"]'
                    ]
                    
                    for selector in suggestion_selectors:
                        try:
                            suggestion = await page.wait_for_selector(selector, timeout=3000)
                            if suggestion:
                                await suggestion.click()
                                break
                        except:
                            continue
                else:
                    # Выбираем первый магазин из списка
                    store_selectors = [
                        '.store-item:first-child',
                        '.shop-item:first-child',
                        '.address-item:first-child',
                        'button[class*="store"]:first-child'
                    ]
                    
                    for selector in store_selectors:
                        try:
                            store = await page.wait_for_selector(selector, timeout=3000)
                            if store:
                                await store.click()
                                break
                        except:
                            continue
                
                await asyncio.sleep(2)
            
            await asyncio.sleep(3)
            logger.info("Location set successfully")
            
        except Exception as e:
            logger.warning(f"Could not set location automatically: {e}")
        finally:
            await page.close()
    
    async def _scrape_items(self) -> List[Dict[str, Any]]:
        """Получение списка товаров из категорий готовой еды."""
        all_items = []
        
        # Прямые ссылки на категории готовой еды ВкусВилл
        category_urls = [
            "https://vkusvill.ru/goods/gotovaya-eda/",
            "https://vkusvill.ru/goods/gotovaya-eda/supy/",
            "https://vkusvill.ru/goods/gotovaya-eda/salaty/",
            "https://vkusvill.ru/goods/gotovaya-eda/zavtraki/",
            "https://vkusvill.ru/goods/gotovaya-eda/zakuski/",
            "https://vkusvill.ru/goods/gotovaya-eda/vtorye-blyuda/",
            "https://vkusvill.ru/goods/gotovaya-eda/pirogi-pirozhki-i-lepyeshki/",
            "https://vkusvill.ru/goods/gotovaya-eda/semeynyy-format/"
        ]
        
        for category_url in category_urls:
            try:
                category_name = category_url.split('/')[-2] if category_url.endswith('/') else category_url.split('/')[-1]
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
            logger.debug(f"Loading category URL: {category_url}")
            
            await page.goto(category_url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(5)
            
            # Прокручиваем страницу для загрузки всех товаров
            await self._scroll_to_load_all(page)
            
            # Извлекаем товары с рабочими селекторами для ВкусВилл
            product_selectors = [
                '[class*="Product"]',  # Найдено 445 элементов
                '[class*="Card"]',     # Найдено 444 элемента  
                'a[href*="/goods/"]',  # Найдено 87 ссылок на товары
                '[class*="product"]',  # Найдено 106 элементов
                '.product-card',
                '.catalog-item',
                '.goods-item'
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
                logger.warning(f"No products found in category {category_url}")
                
                # Fallback: ищем любые ссылки на товары
                all_links = await page.query_selector_all('a[href*="/goods/"], a[href*="/product/"], a[href*="/item/"]')
                if all_links:
                    logger.info(f"Found {len(all_links)} product links as fallback")
                    products = all_links[:20]
                else:
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
            category_url = f"{self.base_url}/goods/{category}"
            logger.debug(f"Loading category URL: {category_url}")
            
            await page.goto(category_url, wait_until="networkidle")
            await asyncio.sleep(3)
            
            # Прокручиваем страницу для загрузки всех товаров
            await self._scroll_to_load_all(page)
            
            # Извлекаем товары
            product_selectors = [
                '[data-testid="product-card"]',
                '.product-card',
                '.catalog-item',
                '.goods-item',
                '[class*="ProductCard"]',
                '[class*="product"]',
                '.product'
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
            # Название товара - более гибкий поиск
            name_selectors = [
                '[data-testid="product-name"]',
                '[data-testid="product-title"]',
                '.product-name',
                '.product-title',
                '.catalog-item__title',
                '.goods-item__title',
                'h1', 'h2', 'h3', 'h4', 'h5',
                '[class*="title"]',
                '[class*="Title"]',
                '[class*="name"]',
                '[class*="Name"]'
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
            
            # Цена - более широкий поиск
            price_selectors = [
                '[data-testid="product-price"]',
                '[data-testid="price"]',
                '.product-price',
                '.catalog-item__price',
                '.goods-item__price',
                '.price',
                '[class*="Price"]',
                '[class*="price"]',
                '[class*="cost"]',
                '[class*="Cost"]'
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
            
            if not url:
                return None
            
            # Изображение
            image_selectors = [
                'img[src]',
                '[data-testid="product-image"] img',
                '.product-image img',
                '.catalog-item__image img'
            ]
            
            photo_url = None
            for selector in image_selectors:
                try:
                    img_element = await product_element.query_selector(selector)
                    if img_element:
                        src = await img_element.get_attribute('src')
                        if src and 'data:' not in src:
                            if src.startswith('//'):
                                photo_url = f"https:{src}"
                            elif src.startswith('/'):
                                photo_url = urljoin(self.base_url, src)
                            else:
                                photo_url = src
                            break
                except:
                    continue
            
            # Пытаемся извлечь вес из названия или дополнительной информации
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
                'composition': None,  # Из каталога не получаем
                'kcal_100g': None,    # Из каталога не получаем
                'protein_100g': None, # Из каталога не получаем
                'fat_100g': None,     # Из каталога не получаем
                'carb_100g': None     # Из каталога не получаем
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
            storage = await self._extract_storage(page)
            
            # Обновляем данные
            item_data.update({
                'composition': composition,
                'portion_g': portion_g,
                'tags': tags,
                'brand': brand,
                'barcode': barcode,
                'shelf_life': shelf_life,
                'storage': storage,
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
            '.product-composition',
            '*:has-text("Состав")',
            '*:has-text("Ингредиенты")'
        ]
        
        # Ищем в табах или аккордеонах
        tab_selectors = [
            'button:has-text("Состав")',
            'button:has-text("Ингредиенты")',
            '.tab:has-text("Состав")',
            '[role="tab"]:has-text("Состав")'
        ]
        
        for selector in tab_selectors:
            try:
                tab = await page.query_selector(selector)
                if tab:
                    await tab.click()
                    await asyncio.sleep(1)
                    break
            except:
                continue
        
        for selector in composition_selectors:
            try:
                if ':has-text(' in selector:
                    elements = await page.query_selector_all(selector)
                    for element in elements:
                        # Ищем следующий элемент или родительский контейнер
                        parent = await element.query_selector('xpath=..')
                        if parent:
                            text = await parent.inner_text()
                            if 'состав' in text.lower() or 'ингредиент' in text.lower():
                                lines = text.split('\n')
                                composition_text = ""
                                found_composition = False
                                
                                for line in lines:
                                    if 'состав' in line.lower() or 'ингредиент' in line.lower():
                                        found_composition = True
                                        continue
                                    if found_composition and line.strip():
                                        composition_text += line.strip() + " "
                                
                                if composition_text.strip():
                                    return composition_text.strip()
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
        
        # Ищем таб с пищевой ценностью
        nutrition_tabs = [
            'button:has-text("Пищевая ценность")',
            'button:has-text("Энергетическая ценность")',
            'button:has-text("КБЖУ")',
            '.tab:has-text("Пищевая")',
            '[role="tab"]:has-text("Пищевая")'
        ]
        
        for selector in nutrition_tabs:
            try:
                tab = await page.query_selector(selector)
                if tab:
                    await tab.click()
                    await asyncio.sleep(1)
                    break
            except:
                continue
        
        # Ищем блок с пищевой ценностью
        nutrition_selectors = [
            '[data-testid="nutrition"]',
            '[data-testid="nutritional-value"]',
            '.nutrition',
            '.nutritional-value',
            '.energy-value',
            '.product-nutrition',
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
            # Парсим нутриенты с учетом особенностей ВкусВилл
            
            # Калории
            kcal_patterns = [
                r'(\d+(?:[.,]\d+)?)\s*(?:ккал|kcal)',
                r'энергетическая.*?(\d+(?:[.,]\d+)?)',
                r'калорийность.*?(\d+(?:[.,]\d+)?)'
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
                r'carbohydrates.*?(\d+(?:[.,]\d+)?)'
            ]
            
            for pattern in carb_patterns:
                match = re.search(pattern, nutrition_text, re.IGNORECASE)
                if match:
                    nutrition_data['carb_100g'] = Decimal(match.group(1).replace(',', '.'))
                    break
        
        return nutrition_data
    
    async def _extract_portion_weight(self, page: Page) -> Optional[Decimal]:
        """Извлечение веса порции."""
        weight_selectors = [
            '[data-testid="weight"]',
            '.weight',
            '.product-weight',
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
        
        # Ищем в заголовке или описании товара
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
        
        # Ищем теги/лейблы
        tag_selectors = [
            '[data-testid="tags"]',
            '[data-testid="labels"]',
            '.tags',
            '.labels',
            '.badges',
            '.product-labels'
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
        
        # Ищем специфичные для ВкусВилл теги
        try:
            page_content = await page.content()
            vkusvill_keywords = [
                'без консервантов', 'натуральный', 'домашний', 'фермерский',
                'органический', 'эко', 'био', 'здоровое питание', 'пп',
                'диетический', 'без глютена', 'без лактозы', 'веган', 'вегетарианский'
            ]
            
            for keyword in vkusvill_keywords:
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
            '.producer',
            '*:has-text("Производитель")',
            '*:has-text("Бренд")',
            '*:has-text("Торговая марка")'
        ]
        
        return await self._extract_text_by_selectors(page, brand_selectors)
    
    async def _extract_barcode(self, page: Page) -> Optional[str]:
        """Извлечение штрихкода."""
        barcode_selectors = [
            '[data-testid="barcode"]',
            '.barcode',
            '*:has-text("Штрихкод")',
            '*:has-text("EAN")'
        ]
        
        return await self._extract_text_by_selectors(page, barcode_selectors)
    
    async def _extract_shelf_life(self, page: Page) -> Optional[str]:
        """Извлечение срока годности."""
        shelf_life_selectors = [
            '[data-testid="shelf-life"]',
            '.shelf-life',
            '*:has-text("Срок годности")',
            '*:has-text("Годен до")',
            '*:has-text("Срок хранения")'
        ]
        
        return await self._extract_text_by_selectors(page, shelf_life_selectors)
    
    async def _extract_storage(self, page: Page) -> Optional[str]:
        """Извлечение условий хранения."""
        storage_selectors = [
            '[data-testid="storage"]',
            '.storage',
            '*:has-text("Условия хранения")',
            '*:has-text("Хранить при")',
            '*:has-text("Температура хранения")'
        ]
        
        return await self._extract_text_by_selectors(page, storage_selectors)
    
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
                                    clean_line = line.strip()
                                    if (len(clean_line) > 2 and 
                                        not any(word in clean_line.lower() for word in 
                                               ['производитель', 'бренд', 'штрихкод', 'срок', 'условия'])):
                                        return clean_line
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
        # Ищем ID в URL ВкусВилл
        id_patterns = [
            r'/goods/([^/?]+)',
            r'/product/([^/?]+)',
            r'id=([^&]+)',
            r'/([^/?]+)/?$'
        ]
        
        for pattern in id_patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        # В крайнем случае используем хеш от URL
        return super()._generate_id_from_url(url)

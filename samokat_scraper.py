"""
Парсер Самоката для сбора готовой еды.
Поддерживает геопривязку и работает с SPA-интерфейсом.
"""

import asyncio
import logging
import re
from typing import AsyncIterator, Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs

import httpx
try:
    from selectolax.parser import HTMLParser
except ImportError:
    # Fallback для случаев, когда selectolax не установлен
    HTMLParser = None


class SamokatScraper:
    """Скрейпер готовой еды с Самоката."""
    
    BASE_URL = "https://samokat.ru"
    READY_FOOD_CATEGORIES = [
        # Основные категории готовой еды
        "/category/vsya-gotovaya-eda-13",  # Вся готовая еда
        "/category/gotovaya-eda-25",  # Готовая еда (альтернативный раздел)
        "/category/supy",  # Супы
        "/category/salaty-i-zakuski",  # Салаты и закуски
        "/category/chto-na-zavtrak",  # Что на завтрак?
        "/category/gotovaya-eda-i-vypechka-6",  # Готовая еда и выпечка
        "/category/stritfud-1",  # Стритфуд
        "/category/pochti-gotovo",  # Почти готово (полуфабрикаты/быстрые блюда)
        "/category/bolshie-portsii",  # Большие порции (подборка)
        
        # Дополнительные категории
        "/category/pizza",  # Пицца
        "/category/rolly-i-sushi",  # Роллы и суши
        "/category/burgery",  # Бургеры
        "/category/sendvichi",  # Сэндвичи
        "/category/shaurma",  # Шаурма
        "/category/salaty",  # Салаты
        "/category/zavtraki",  # Завтраки
        "/category/deserty",  # Десерты
        "/category/napitki",  # Напитки
        "/category/kofe",  # Кофе
        "/category/chai",  # Чай
        "/category/soki",  # Соки
        "/category/smoothie",  # Смузи
        "/category/kokteyli",  # Коктейли
        "/category/energetiki",  # Энергетики
        "/category/voda",  # Вода
        "/category/molochnye-napitki",  # Молочные напитки
        "/category/bezalkogolnye-napitki",  # Безалкогольные напитки
    ]
    
    def __init__(self, antibot_client):
        self.client = antibot_client
        self.session = None
        self.city_set = False
    
    async def scrape(self, city: str = "Москва", coords: str = "55.75,37.61", 
                    limit: int = -1, **kwargs) -> AsyncIterator[Dict]:
        """Основной метод скрейпинга."""
        logging.info(f"Starting Samokat scrape for {city} at {coords}")
        
        try:
            # Получаем сессию для домена
            self.session = await self.client.get_client(self.BASE_URL)
            
            # Устанавливаем город и координаты
            await self._set_location(city, coords)
            
            # Собираем товары из всех категорий готовой еды
            count = 0
            for category_url in self.READY_FOOD_CATEGORIES:
                category_full_url = urljoin(self.BASE_URL, category_url)
                logging.info(f"Scraping Samokat category: {category_full_url}")
                
                try:
                    async for product_data in self._scrape_category_products(category_full_url):
                        if limit > 0 and count >= limit:
                            break
                        
                        if product_data:
                            yield product_data
                            count += 1
                            
                            if count % 20 == 0:
                                logging.info(f"Scraped {count} products from Samokat")
                        
                        # Пауза между товарами
                        await asyncio.sleep(0.5)
                    
                    # Пауза между категориями
                    await asyncio.sleep(1.0)
                    
                except Exception as e:
                    logging.error(f"Error scraping category {category_url}: {e}")
                    continue
            
            logging.info(f"Samokat scrape completed. Total products: {count}")
            
        except Exception as e:
            logging.error(f"Samokat scrape failed: {e}")
            raise
    
    async def _set_location(self, city: str, coords: str) -> None:
        """Установить город и координаты для Самоката."""
        try:
            # Сначала загружаем главную страницу для получения сессии
            main_response = await self.client.request(method="GET", url=self.BASE_URL)
            if main_response.status_code != 200:
                logging.warning(f"Failed to load main page: {main_response.status_code}")
                return

            # Устанавливаем cookies для геолокации
            location_cookies = {
                'city': city,
                'coordinates': coords,
                'user_city': city,
                'user_coords': coords,
                'location_set': '1',
                'region': 'moscow' if 'moscow' in city.lower() else 'spb',
                'delivery_area': city,
                'geo_location': coords
            }

            # Обновляем сессию с cookies
            if self.session:
                for name, value in location_cookies.items():
                    self.session.cookies.set(name, value)

            # Попробуем установить город через различные API endpoints
            api_endpoints = [
                f"{self.BASE_URL}/api/location",
                f"{self.BASE_URL}/api/set-location",
                f"{self.BASE_URL}/ajax/set_city.php",
                f"{self.BASE_URL}/api/city",
                f"{self.BASE_URL}/location/set"
            ]

            location_data = {
                "city": city,
                "coordinates": coords.split(',') if ',' in coords else [coords, "0"],
                "address": city,
                "lat": coords.split(',')[0] if ',' in coords else coords,
                "lng": coords.split(',')[1] if ',' in coords else "0"
            }

            for api_url in api_endpoints:
                try:
                    response = await self.client.request(
                        method="POST",
                        url=api_url,
                        json=location_data
                    )
                    if response.status_code == 200:
                        logging.info(f"Location API call successful for {city} via {api_url}")
                        self.city_set = True
                        break
                except Exception:
                    continue

            # Проверяем, что геолокация установлена
            try:
                test_response = await self.client.request(method="GET", url=f"{self.BASE_URL}/category/vsya-gotovaya-eda-13")
                if test_response.status_code == 200:
                    logging.info(f"Location verification successful - catalog accessible")
                    self.city_set = True
                else:
                    logging.warning(f"Location verification failed - catalog returned {test_response.status_code}")
            except Exception as test_error:
                logging.warning(f"Location verification failed: {test_error}")

            logging.info(f"Location set to {city} ({coords})")

        except Exception as e:
            logging.warning(f"Failed to set location: {e}")
            self.city_set = False
    
    async def _scrape_category_products(self, category_url: str) -> AsyncIterator[Dict]:
        """Собрать товары из категории готовой еды."""
        if HTMLParser is None:
            logging.error("selectolax not installed, cannot parse HTML")
            return
            
        # category_url уже полный URL, используем его как есть
        
        try:
            # Загружаем основную страницу категории
            response = await self.client.request(method="GET", url=category_url)
            
            if response.status_code != 200:
                logging.error(f"Failed to load category page: {response.status_code}")
                return
            
            html = response.text
            parser = HTMLParser(html)
            
            # Проверяем, не перенаправили ли нас на главную страницу
            if "vsya-gotovaya-eda" not in html and "готовой еды" not in html.lower():
                logging.warning("Category page might be geo-restricted or redirected")
                # Попробуем альтернативный подход
                async for product_data in self._scrape_alternative_approach():
                    yield product_data
                return
            
            # Собираем ссылки на товары
            product_urls = self._extract_product_urls(parser)
            
            if not product_urls:
                logging.warning("No product URLs found, trying alternative approach")
                async for product_data in self._scrape_alternative_approach():
                    yield product_data
                return
            
            logging.info(f"Found {len(product_urls)} product URLs")
            
            # Собираем данные по каждому товару
            for product_url in product_urls:
                try:
                    product_data = await self._scrape_product(product_url)
                    if product_data:
                        yield product_data
                    
                    # Пауза между товарами
                    await asyncio.sleep(0.3)
                    
                except Exception as e:
                    logging.error(f"Error scraping product {product_url}: {e}")
                    continue
            
            # Проверяем пагинацию
            async for product_data in self._scrape_pagination(parser, category_url):
                yield product_data
            
        except Exception as e:
            logging.error(f"Error scraping ready food category: {e}")
    
    async def _scrape_alternative_approach(self) -> AsyncIterator[Dict]:
        """Альтернативный подход для сбора товаров."""
        logging.info("Trying alternative scraping approach")
        
        # Попробуем найти товары через поиск
        search_terms = [
            "готовая еда",
            "готовые блюда", 
            "салаты",
            "супы",
            "вторые блюда",
            "диетические блюда"
        ]
        
        for search_term in search_terms:
            try:
                search_url = f"{self.BASE_URL}/search?q={search_term}"
                response = await self.client.request(method="GET", url=search_url)
                
                if response.status_code == 200:
                    if HTMLParser is None:
                        logging.error("selectolax not installed, cannot parse HTML")
                        continue
                    parser = HTMLParser(response.text)
                    product_urls = self._extract_product_urls(parser)
                    
                    for product_url in product_urls:  # Убираем ограничение
                        try:
                            product_data = await self._scrape_product(product_url)
                            if product_data:
                                yield product_data
                            await asyncio.sleep(0.5)
                        except Exception as e:
                            logging.error(f"Error in alternative scraping: {e}")
                            continue
                
                await asyncio.sleep(1.0)  # Пауза между поисковыми запросами
                
            except Exception as e:
                logging.error(f"Error in alternative search for '{search_term}': {e}")
                continue
    
    def _extract_product_urls(self, parser: HTMLParser) -> List[str]:
        """Извлечь ссылки на товары из страницы."""
        product_urls = []
        
        # Расширенные селекторы для ссылок на товары Самоката
        link_selectors = [
            # Основные селекторы товаров
            'a[href*="/product/"]',
            'a[href*="/item/"]',
            'a[href*="/goods/"]',
            'a[href*="/catalog/"]',
            
            # Селекторы карточек товаров
            '.product-card a',
            '.item-card a',
            '.product-item a',
            '.catalog-item a',
            '.goods-item a',
            '.product-link',
            '.item-link',
            '.product-title a',
            '.item-title a',
            
            # Селекторы для списков товаров
            '.product-list a',
            '.item-list a',
            '.catalog-list a',
            '.goods-list a',
            '.products-grid a',
            '.items-grid a',
            
            # Селекторы для карточек в сетке
            '.grid-item a',
            '.card-item a',
            '.product-grid-item a',
            '.catalog-grid-item a',
            
            # Дополнительные селекторы
            '[data-product-id] a',
            '[data-item-id] a',
            '.product a',
            '.item a',
            '.goods a',
            '.catalog a'
        ]
        
        for selector in link_selectors:
            links = parser.css(selector)
            for link in links:
                href = link.attributes.get('href')
                if href and any(pattern in href for pattern in ['/product/', '/item/', '/goods/', '/catalog/']):
                    full_url = urljoin(self.BASE_URL, href)
                    if full_url not in product_urls:
                        product_urls.append(full_url)
        
        # Дополнительный поиск в data-атрибутах
        data_selectors = [
            '[data-product-url]',
            '[data-item-url]',
            '[data-goods-url]',
            '[data-catalog-url]'
        ]
        
        for selector in data_selectors:
            elements = parser.css(selector)
            for element in elements:
                url_attr = element.attributes.get('data-product-url') or \
                          element.attributes.get('data-item-url') or \
                          element.attributes.get('data-goods-url') or \
                          element.attributes.get('data-catalog-url')
                if url_attr:
                    full_url = urljoin(self.BASE_URL, url_attr)
                    if full_url not in product_urls:
                        product_urls.append(full_url)
        
        return product_urls
    
    async def _scrape_pagination(self, parser: HTMLParser, base_url: str) -> AsyncIterator[Dict]:
        """Обработать пагинацию."""
        # Ищем кнопки пагинации
        pagination_selectors = [
            '.pagination a',
            '.pager a',
            '.load-more',
            '.show-more',
            '.next-page'
        ]
        
        for selector in pagination_selectors:
            elements = parser.css(selector)
            for element in elements:
                href = element.attributes.get('href')
                if href and 'page=' in href:
                    # Загружаем следующую страницу
                    next_url = urljoin(self.BASE_URL, href)
                    try:
                        response = await self.client.request(method="GET", url=next_url)
                        if response.status_code == 200:
                            if HTMLParser is None:
                                logging.error("selectolax not installed, cannot parse HTML")
                                break
                            next_parser = HTMLParser(response.text)
                            product_urls = self._extract_product_urls(next_parser)
                            
                            for product_url in product_urls:
                                product_data = await self._scrape_product(product_url)
                                if product_data:
                                    yield product_data
                                await asyncio.sleep(0.3)
                    except Exception as e:
                        logging.error(f"Error scraping pagination: {e}")
                        break
    
    async def _scrape_product(self, product_url: str) -> Optional[Dict]:
        """Собрать данные одного товара."""
        try:
            response = await self.client.request(method="GET", url=product_url)
            if response.status_code != 200:
                logging.warning(f"Failed to load product {product_url}: {response.status_code}")
                return None
            
            html = response.text
            if HTMLParser is None:
                logging.error("selectolax not installed, cannot parse HTML")
                return None
            parser = HTMLParser(html)
            
            # Извлекаем ID из URL
            product_id = self._extract_product_id(product_url)
            if not product_id:
                logging.warning(f"Could not extract ID from {product_url}")
                return None
            
            # Название товара
            name = self._extract_name(parser)
            if not name:
                logging.warning(f"No name found for {product_url}")
                return None
            
            # Категория
            category = self._extract_category(parser)
            
            # Цена
            price = self._extract_price(parser)
            
            # Вес/объем порции
            portion_g = self._extract_portion(parser)
            
            # БЖУ (если есть на 100г)
            nutritional_info = self._extract_nutritional_info(parser)
            
            # Состав
            composition = self._extract_composition(parser)
            
            # Фото
            photo_url = self._extract_photo(parser)
            
            # Теги
            tags = self._extract_tags(parser)
            
            # Формируем результат
            product_data = {
                'id': product_id,
                'name': name,
                'category': category,
                'kcal_100g': nutritional_info.get('kcal'),
                'protein_100g': nutritional_info.get('protein'),
                'fat_100g': nutritional_info.get('fat'),
                'carb_100g': nutritional_info.get('carb'),
                'portion_g': portion_g,
                'price': price,
                'shop': 'samokat',
                'tags': tags,
                'composition': composition,
                'url': product_url,
                'photo': photo_url
            }
            
            return product_data
            
        except Exception as e:
            logging.error(f"Error parsing product {product_url}: {e}")
            return None
    
    def _extract_product_id(self, url: str) -> Optional[str]:
        """Извлечь ID товара из URL."""
        # Ищем числовой ID в URL
        match = re.search(r'/product/(\d+)', url)
        if match:
            return match.group(1)
        
        match = re.search(r'/item/(\d+)', url)
        if match:
            return match.group(1)
        
        # Альтернативно - последний сегмент URL
        path_parts = urlparse(url).path.split('/')
        if path_parts:
            return path_parts[-1] or path_parts[-2]
        
        return None
    
    def _extract_name(self, parser: HTMLParser) -> Optional[str]:
        """Извлечь название товара."""
        name_selectors = [
            'h1.product-title',
            'h1[data-testid="product-title"]',
            '.product-info h1',
            '.product-header h1',
            '.item-title h1',
            'h1'
        ]
        
        for selector in name_selectors:
            element = parser.css_first(selector)
            if element and element.text(strip=True):
                return element.text(strip=True)
        
        return None
    
    def _extract_category(self, parser: HTMLParser) -> str:
        """Извлечь категорию."""
        # Хлебные крошки
        breadcrumb_selectors = [
            '.breadcrumbs a',
            '.breadcrumb a',
            '.navigation-path a',
            '.breadcrumb-nav a'
        ]
        
        categories = []
        for selector in breadcrumb_selectors:
            elements = parser.css(selector)
            for element in elements:
                text = element.text(strip=True)
                if text and text not in ['Главная', 'Каталог', 'Самокат']:
                    categories.append(text)
        
        if categories:
            return ' > '.join(categories)
        
        # Альтернативно - из мета-тегов или заголовков
        meta_category = parser.css_first('meta[property="product:category"]')
        if meta_category:
            return meta_category.attributes.get('content', 'Готовая еда')
        
        return 'Готовая еда'
    
    def _extract_price(self, parser: HTMLParser) -> Optional[float]:
        """Извлечь цену товара."""
        price_selectors = [
            '.price-current',
            '.product-price .current',
            '.price .value',
            '.product-price',
            '[data-testid="price"]',
            '.item-price'
        ]
        
        for selector in price_selectors:
            element = parser.css_first(selector)
            if element:
                price_text = element.text(strip=True)
                # Убираем валютные символы и пробелы
                price_text = re.sub(r'[^\d,.]', '', price_text)
                if price_text:
                    try:
                        return float(price_text.replace(',', '.'))
                    except ValueError:
                        continue
        
        return None
    
    def _extract_portion(self, parser: HTMLParser) -> Optional[float]:
        """Извлечь вес/объем порции."""
        # Ищем в характеристиках товара
        characteristics_selectors = [
            '.product-characteristics',
            '.product-specs',
            '.product-details',
            '.specifications',
            '.item-specs'
        ]
        
        for selector in characteristics_selectors:
            container = parser.css_first(selector)
            if container:
                text = container.text()
                
                # Ищем вес в граммах
                weight_match = re.search(r'(\d+(?:[.,]\d+)?)\s*г', text, re.IGNORECASE)
                if weight_match:
                    try:
                        return float(weight_match.group(1).replace(',', '.'))
                    except ValueError:
                        continue
                
                # Ищем объем в мл
                volume_match = re.search(r'(\d+(?:[.,]\d+)?)\s*мл', text, re.IGNORECASE)
                if volume_match:
                    try:
                        return float(volume_match.group(1).replace(',', '.'))
                    except ValueError:
                        continue
        
        return None
    
    def _extract_nutritional_info(self, parser: HTMLParser) -> Dict[str, Optional[float]]:
        """Извлечь БЖУ (только если указано на 100г)."""
        nutritional_info = {
            'kcal': None,
            'protein': None,
            'fat': None,
            'carb': None
        }
        
        # Ищем таблицу пищевой ценности
        nutrition_selectors = [
            '.nutrition-table',
            '.nutrition-info',
            '.product-nutrition',
            '.food-value-table',
            'table[data-testid="nutrition"]'
        ]
        
        for selector in nutrition_selectors:
            table = parser.css_first(selector)
            if table:
                # Проверяем, что это БЖУ на 100г
                table_text = table.text()
                if '100 г' in table_text or '100г' in table_text:
                    # Парсим таблицу
                    rows = table.css('tr')
                    for row in rows:
                        cells = row.css('td, th')
                        if len(cells) >= 2:
                            label = cells[0].text(strip=True).lower()
                            value_text = cells[1].text(strip=True)
                            
                            # Извлекаем числовое значение
                            value_match = re.search(r'(\d+(?:[.,]\d+)?)', value_text)
                            if value_match:
                                try:
                                    value = float(value_match.group(1).replace(',', '.'))
                                    
                                    if 'калори' in label or 'энерг' in label:
                                        nutritional_info['kcal'] = value
                                    elif 'белк' in label or 'протеин' in label:
                                        nutritional_info['protein'] = value
                                    elif 'жир' in label:
                                        nutritional_info['fat'] = value
                                    elif 'углевод' in label or 'карб' in label:
                                        nutritional_info['carb'] = value
                                except ValueError:
                                    continue
        
        return nutritional_info
    
    def _extract_composition(self, parser: HTMLParser) -> str:
        """Извлечь состав товара."""
        composition_selectors = [
            '.product-composition',
            '.composition',
            '.ingredients',
            '[data-testid="composition"]',
            '.product-ingredients',
            '.item-composition'
        ]
        
        for selector in composition_selectors:
            element = parser.css_first(selector)
            if element:
                composition_text = element.text(strip=True)
                if composition_text and 'состав' in composition_text.lower():
                    return composition_text
        
        # Альтернативный поиск в тексте страницы
        page_text = parser.text()
        composition_match = re.search(
            r'Состав[:\s]*([^.]*)',
            page_text,
            re.IGNORECASE | re.DOTALL
        )
        
        if composition_match:
            return composition_match.group(1).strip()
        
        return ''
    
    def _extract_photo(self, parser: HTMLParser) -> str:
        """Извлечь URL главного фото."""
        photo_selectors = [
            '.product-image img',
            '.product-photo img',
            '.product-gallery img',
            '[data-testid="product-image"] img',
            '.main-image img',
            '.item-image img'
        ]
        
        for selector in photo_selectors:
            element = parser.css_first(selector)
            if element:
                src = element.attributes.get('src')
                if src:
                    return urljoin(self.BASE_URL, src)
        
        return ''
    
    def _extract_tags(self, parser: HTMLParser) -> str:
        """Извлечь теги товара."""
        tags = []
        
        # Ищем теги в различных местах
        tag_selectors = [
            '.product-tags .tag',
            '.product-badges .badge',
            '.product-labels .label',
            '.product-features .feature',
            '.product-attributes .attribute',
            '.item-tags .tag'
        ]
        
        for selector in tag_selectors:
            elements = parser.css(selector)
            for element in elements:
                tag_text = element.text(strip=True)
                if tag_text and len(tag_text) < 50:
                    tags.append(tag_text)
        
        return '; '.join(tags) if tags else ''

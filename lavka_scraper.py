"""
Парсер Яндекс Лавки для сбора готовой еды.
Работает в бережном режиме из-за агрессивной антибот-защиты (SmartCaptcha).
"""

import asyncio
import logging
import random
import re
from typing import AsyncIterator, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser


class LavkaScraper:
    """Скрейпер готовой еды с Яндекс Лавки (бережный режим)."""
    
    BASE_URL = "https://lavka.yandex.ru"
    MOBILE_BASE_URL = "https://m.lavka.yandex.ru"  # Мобильная версия
    READY_FOOD_CATEGORIES = [
        # Основные категории готовой еды
        "/category/gotovaya_eda",  # Готовая еда (основной раздел)
        "/category/hot_streetfood",  # Горячий стритфуд («Есть горячее!»)
        "/category/gotovaya_eda/ostroe-1",  # Острое (подраздел внутри «Готовой еды»)
        
        # Дополнительные категории
        "/category/salaty",  # Салаты
        "/category/supy",  # Супы
        "/category/zavtraki",  # Завтраки
        "/category/burgery",  # Бургеры
        "/category/pizza",  # Пицца
        "/category/rolly",  # Роллы
        "/category/sushi",  # Суши
        "/category/sendvichi",  # Сэндвичи
        "/category/shaurma",  # Шаурма
        "/category/deserty",  # Десерты
        "/category/vypechka",  # Выпечка
        "/category/snacks",  # Снеки
        "/category/napitki",  # Напитки
        "/category/kofe",  # Кофе
        "/category/chai",  # Чай
        "/category/soki",  # Соки
        "/category/smoothie",  # Смузи
        "/category/voda",  # Вода
        "/category/molochnye_napitki",  # Молочные напитки
        "/category/energetiki",  # Энергетики
        "/category/bezalkogolnye_napitki",  # Безалкогольные напитки
    ]
    
    def __init__(self, antibot_client):
        self.client = antibot_client
        self.session = None
        self.request_count = 0
        self.max_requests_per_session = 50  # Увеличиваем лимит для большего охвата
    
    async def scrape(self, city: str = "Москва", coords: str = "55.75,37.61", 
                    limit: int = -1, **kwargs) -> AsyncIterator[Dict]:
        """Основной метод скрейпинга в бережном режиме."""
        logging.info(f"Starting Lavka scrape for {city} (gentle mode)")
        
        try:
            # Получаем сессию для домена
            self.session = await self.client.get_client(self.BASE_URL)
            
            # Устанавливаем город
            await self._set_city(city, coords)
            
            # Собираем товары из категорий готовой еды
            count = 0
            async for product_data in self._scrape_ready_food_categories():
                if limit > 0 and count >= limit:
                    break
                
                if product_data:
                    yield product_data
                    count += 1
                    
                    if count % 10 == 0:
                        logging.info(f"Scraped {count} products from Lavka")
                
                # Увеличенная пауза для бережного режима
                await asyncio.sleep(2.0)
            
            logging.info(f"Lavka scrape completed. Total products: {count}")
            
        except Exception as e:
            logging.error(f"Lavka scrape failed: {e}")
            raise
    
    async def _set_city(self, city: str, coords: str) -> None:
        """Установить город для Лавки."""
        try:
            # Попытка установить город через главную страницу с более реалистичными заголовками
            response = await self.client.request(
                method="GET",
                url=f"{self.BASE_URL}/",
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Cache-Control": "max-age=0",
                    "Sec-Ch-Ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                    "Sec-Ch-Ua-Mobile": "?0",
                    "Sec-Ch-Ua-Platform": '"macOS"',
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
                }
            )
            
            if response.status_code == 200:
                logging.info(f"Lavka homepage loaded for {city}")
            else:
                logging.warning(f"Failed to load Lavka homepage: {response.status_code}")
                
        except Exception as e:
            logging.warning(f"Could not set city for Lavka: {e}")
    
    async def _scrape_ready_food_categories(self) -> AsyncIterator[Dict]:
        """Собрать товары из категорий готовой еды."""
        # Попробуем сначала прямые ссылки на популярные товары (реальные ID)
        popular_products = [
            "/product/1001",   # Попробуем простые ID
            "/product/1002",
            "/product/1003",
            "/product/1004",
            "/product/1005",
            "/product/2001",
            "/product/2002",
            "/product/2003",
            "/product/2004",
            "/product/2005"
        ]
        
        for product_path in popular_products:
            try:
                product_url = urljoin(self.BASE_URL, product_path)
                logging.info(f"Trying direct product: {product_url}")
                
                # Проверяем лимит запросов
                if self.request_count >= self.max_requests_per_session:
                    logging.warning("Reached request limit, creating new session")
                    await self._refresh_session()
                
                # Большая пауза
                await asyncio.sleep(random.uniform(5.0, 8.0))
                
                # Загружаем страницу товара
                response = await self.client.request(method="GET", url=product_url)
                self.request_count += 1
                
                if response.status_code == 200:
                    html = response.text
                    
                    # Проверяем на капчу или блокировку
                    if not self._is_blocked(html):
                        product_data = await self._scrape_product(product_url)
                        if product_data:
                            yield product_data
                    else:
                        logging.warning(f"Blocked on product {product_url}")
                else:
                    logging.warning(f"Failed to load product {product_url}: {response.status_code}")
                
            except Exception as e:
                logging.error(f"Error with direct product {product_url}: {e}")
                continue
        
        # Попробуем поиск по более простым терминам
        simple_search_terms = [
            "салат",
            "суп", 
            "пицца",
            "бургер",
            "кофе"
        ]
        
        for term in simple_search_terms:
            try:
                logging.info(f"Trying simple search for: {term}")
                
                # Проверяем лимит запросов
                if self.request_count >= self.max_requests_per_session:
                    logging.warning("Reached request limit, creating new session")
                    await self._refresh_session()
                
                # Большая пауза для бережного режима
                await asyncio.sleep(random.uniform(8.0, 12.0))
                
                # Кодируем поисковый запрос для URL
                from urllib.parse import quote
                encoded_term = quote(term)
                search_url = f"{self.BASE_URL}/search/?q={encoded_term}"
                
                # Загружаем страницу поиска с реалистичными заголовками
                response = await self.client.request(
                    method="GET", 
                    url=search_url,
                    headers={
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Cache-Control": "max-age=0",
                        "Sec-Ch-Ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                        "Sec-Ch-Ua-Mobile": "?0",
                        "Sec-Ch-Ua-Platform": '"macOS"',
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "same-origin",
                        "Sec-Fetch-User": "?1",
                        "Upgrade-Insecure-Requests": "1",
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
                    }
                )
                self.request_count += 1
                
                if response.status_code != 200:
                    logging.warning(f"Failed to load search {search_url}: {response.status_code}")
                    continue
                
                html = response.text
                
                # Проверяем на капчу или блокировку
                if self._is_blocked(html):
                    logging.warning(f"Blocked on search {search_url}, skipping")
                    continue
                
                parser = HTMLParser(html)
                
                # Собираем ссылки на товары
                product_urls = self._extract_product_urls(parser)
                
                if not product_urls:
                    logging.warning(f"No products found in search for {term}")
                    continue
                
                logging.info(f"Found {len(product_urls)} products in search for {term}")
                
                # Собираем данные по каждому товару (ограничиваем количество)
                for product_url in product_urls[:5]:  # Берем только первые 5
                    try:
                        # Проверяем лимит запросов
                        if self.request_count >= self.max_requests_per_session:
                            logging.warning("Reached request limit, refreshing session")
                            await self._refresh_session()
                        
                        product_data = await self._scrape_product(product_url)
                        if product_data:
                            yield product_data
                        
                        self.request_count += 1
                        
                        # Большая пауза между товарами
                        await asyncio.sleep(random.uniform(3.0, 5.0))
                        
                    except Exception as e:
                        logging.error(f"Error scraping product {product_url}: {e}")
                        continue
                
                # Большая пауза между поисковыми запросами
                await asyncio.sleep(random.uniform(10.0, 15.0))
                
            except Exception as e:
                logging.error(f"Error in search for {term}: {e}")
                continue
        
        # Если поиск не дал результатов, пробуем только основные категории
        main_categories = [
            "/category/gotovaya_eda",
            "/category/salaty",
            "/category/supy"
        ]
        
        for category_path in main_categories:
            category_url = urljoin(self.BASE_URL, category_path)
            
            try:
                logging.info(f"Scraping Lavka category: {category_url}")
                
                # Проверяем лимит запросов
                if self.request_count >= self.max_requests_per_session:
                    logging.warning("Reached request limit, creating new session")
                    await self._refresh_session()
                
                # Очень большая пауза
                await asyncio.sleep(random.uniform(15.0, 20.0))
                
                # Загружаем страницу категории с реалистичными заголовками
                response = await self.client.request(
                    method="GET", 
                    url=category_url,
                    headers={
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Cache-Control": "max-age=0",
                        "Sec-Ch-Ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                        "Sec-Ch-Ua-Mobile": "?0",
                        "Sec-Ch-Ua-Platform": '"macOS"',
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "same-origin",
                        "Sec-Fetch-User": "?1",
                        "Upgrade-Insecure-Requests": "1",
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
                    }
                )
                self.request_count += 1
                
                if response.status_code != 200:
                    logging.warning(f"Failed to load category {category_url}: {response.status_code}")
                    continue
                
                html = response.text
                
                # Проверяем на капчу или блокировку
                if self._is_blocked(html):
                    logging.warning(f"Blocked on category {category_url}, skipping")
                    continue
                
                parser = HTMLParser(html)
                
                # Собираем ссылки на товары
                product_urls = self._extract_product_urls(parser)
                
                if not product_urls:
                    logging.warning(f"No products found in {category_url}")
                    continue
                
                logging.info(f"Found {len(product_urls)} products in {category_path}")
                
                # Собираем данные по каждому товару (ограничиваем количество)
                for product_url in product_urls[:3]:  # Берем только первые 3
                    try:
                        # Проверяем лимит запросов
                        if self.request_count >= self.max_requests_per_session:
                            logging.warning("Reached request limit, refreshing session")
                            await self._refresh_session()
                        
                        product_data = await self._scrape_product(product_url)
                        if product_data:
                            yield product_data
                        
                        self.request_count += 1
                        
                        # Очень большая пауза между товарами
                        await asyncio.sleep(random.uniform(5.0, 8.0))
                        
                    except Exception as e:
                        logging.error(f"Error scraping product {product_url}: {e}")
                        continue
                
                # Большая пауза между категориями
                await asyncio.sleep(random.uniform(15.0, 20.0))
                
            except Exception as e:
                logging.error(f"Error scraping category {category_url}: {e}")
                continue
        
        # Если ничего не получилось, попробуем мобильную версию
        logging.info("Trying mobile version as last resort")
        async for product_data in self._scrape_mobile_version():
            yield product_data
    
    async def _refresh_session(self) -> None:
        """Обновить сессию для избежания блокировки."""
        try:
            # Закрываем текущую сессию
            if self.session:
                await self.session.aclose()
            
            # Создаем новую сессию
            self.session = await self.client.get_client(self.BASE_URL)
            self.request_count = 0
            
            logging.info("Session refreshed")
            
        except Exception as e:
            logging.error(f"Error refreshing session: {e}")
    
    async def _scrape_mobile_version(self) -> AsyncIterator[Dict]:
        """Попробовать мобильную версию сайта."""
        try:
            # Попробуем мобильную версию с мобильными заголовками
            mobile_headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "Cache-Control": "max-age=0",
                "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1"
            }
            
            # Попробуем главную страницу мобильной версии
            response = await self.client.request(
                method="GET",
                url=self.MOBILE_BASE_URL,
                headers=mobile_headers
            )
            
            if response.status_code == 200:
                html = response.text
                
                if not self._is_blocked(html):
                    logging.info("Mobile version accessible, trying to find products")
                    
                    parser = HTMLParser(html)
                    product_urls = self._extract_product_urls(parser)
                    
                    if product_urls:
                        logging.info(f"Found {len(product_urls)} products in mobile version")
                        
                        # Собираем данные по каждому товару (ограничиваем количество)
                        for product_url in product_urls[:3]:  # Берем только первые 3
                            try:
                                # Конвертируем мобильную ссылку в обычную
                                if self.MOBILE_BASE_URL in product_url:
                                    product_url = product_url.replace(self.MOBILE_BASE_URL, self.BASE_URL)
                                
                                product_data = await self._scrape_product(product_url)
                                if product_data:
                                    yield product_data
                                
                                # Большая пауза между товарами
                                await asyncio.sleep(random.uniform(5.0, 8.0))
                                
                            except Exception as e:
                                logging.error(f"Error scraping mobile product {product_url}: {e}")
                                continue
                    else:
                        logging.warning("No products found in mobile version")
                else:
                    logging.warning("Mobile version also blocked")
            else:
                logging.warning(f"Failed to load mobile version: {response.status_code}")
                
        except Exception as e:
            logging.error(f"Error scraping mobile version: {e}")
    
    def _is_blocked(self, html: str) -> bool:
        """Проверить, заблокирован ли запрос."""
        block_indicators = [
            "captcha",
            "smartcaptcha",
            "robot",
            "blocked",
            "access denied",
            "доступ запрещен",
            "проверка безопасности",
            "cloudflare"
        ]
        
        html_lower = html.lower()
        for indicator in block_indicators:
            if indicator in html_lower:
                return True
        
        return False
    
    def _extract_product_urls(self, parser: HTMLParser) -> List[str]:
        """Извлечь ссылки на товары из страницы."""
        product_urls = []
        
        # Расширенные селекторы для ссылок на товары Яндекс Лавки
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
            '.catalog a',
            
            # Специфичные для Яндекс Лавки
            '.ProductCard a',
            '.ProductItem a',
            '.CatalogItem a',
            '.GoodsItem a',
            '.ProductCard-link',
            '.ProductItem-link'
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
    
    async def _scrape_product(self, product_url: str) -> Optional[Dict]:
        """Собрать данные одного товара."""
        try:
            response = await self.client.request(
                method="GET", 
                url=product_url,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Cache-Control": "max-age=0",
                    "Sec-Ch-Ua": '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
                    "Sec-Ch-Ua-Mobile": "?0",
                    "Sec-Ch-Ua-Platform": '"macOS"',
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "same-origin",
                    "Sec-Fetch-User": "?1",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
                }
            )
            
            if response.status_code != 200:
                logging.warning(f"Failed to load product {product_url}: {response.status_code}")
                return None
            
            html = response.text
            
            # Проверяем на блокировку
            if self._is_blocked(html):
                logging.warning(f"Blocked on product {product_url}")
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
            
            # БЖУ (только если указано на 100г)
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
                'shop': 'lavka',
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
                if text and text not in ['Главная', 'Каталог', 'Лавка', 'Яндекс']:
                    categories.append(text)
        
        if categories:
            return ' > '.join(categories)
        
        # Альтернативно - из мета-тегов
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
            '.item-price',
            '.price-value'
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
            '.item-specs',
            '.product-info'
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
            'table[data-testid="nutrition"]',
            '.nutritional-info'
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
            '.item-image img',
            '.product-picture img'
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

#!/usr/bin/env python3
"""
🏗️ ТЯЖЕЛЫЙ ПАРСЕР ВКУСВИЛЛА ДЛЯ МОСКВЫ
Полный парсинг с заходом в каждую карточку товара для сбора всех данных включая БЖУ.
"""

import asyncio
import csv
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin, quote

try:
    from selectolax.parser import HTMLParser
except ImportError:
    HTMLParser = None

# Встроенный AntiBotClient
import httpx


class AntiBotClient:
    """HTTP клиент с поддержкой cookies для обхода защиты."""

    def __init__(self, concurrency: int = 10, timeout: int = 30):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.timeout = timeout
        self.cookies = {}
        self.client = None  # Храним клиент

    async def _ensure_client(self):
        """Создание или переиспользование клиента."""
        if self.client is None:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            self.client = httpx.AsyncClient(
                timeout=self.timeout,
                headers=headers,
                cookies=self.cookies,
                follow_redirects=True,
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
            )
        return self.client

    async def request(self, method: str, url: str, **kwargs):
        """Выполнить HTTP запрос с сохранением cookies."""
        async with self.semaphore:
            client = await self._ensure_client()
            try:
                response = await client.request(method, url, **kwargs)
                # Обновляем cookies
                self.cookies.update(response.cookies)
                return response
            except httpx.TimeoutException:
                # При таймауте пересоздаем клиент
                await self.close()
                self.client = None
                raise

    async def close(self):
        """Закрытие клиента."""
        if self.client:
            await self.client.aclose()
            self.client = None


class VkusvillHeavyParser:
    """Тяжелый парсер с глубоким анализом каждой карточки."""
    
    def __init__(self, antibot_client):
        self.antibot_client = antibot_client
        self.BASE_URL = "https://vkusvill.ru"
        
    async def scrape_heavy(self, limit: int = 1500) -> List[Dict]:
        """Тяжелый парсинг с заходом в каждую карточку."""
        print(f"🏗️ Начинаем тяжелый парсинг на {limit} товаров...")
        
        # Установка локации для Москвы
        await self._set_location()
        
        # Сбор всех товаров готовой еды
        print("📋 Собираем ВСЕ товары готовой еды...")
        product_urls = set()
        
        ready_food_categories = [
            "/goods/gotovaya-eda/",
            "/goods/gotovaya-eda/novinki/",
            "/goods/gotovaya-eda/vtorye-blyuda/",
            "/goods/gotovaya-eda/vtorye-blyuda/vtorye-blyuda-s-myasom/",
            "/goods/gotovaya-eda/vtorye-blyuda/vtorye-blyuda-s-ptitsey/",
            "/goods/gotovaya-eda/vtorye-blyuda/vtorye-blyuda-s-ryboy-i-moreproduktami/",
            "/goods/gotovaya-eda/vtorye-blyuda/garniry-i-vtorye-blyuda-bez-myasa/",
            "/goods/gotovaya-eda/vtorye-blyuda/pasta-pitstsa/",
            "/goods/gotovaya-eda/salaty/",
            "/goods/gotovaya-eda/sendvichi-shaurma-i-burgery/",
            "/goods/gotovaya-eda/bolshe-belka-menshe-kaloriy/",
            "/goods/gotovaya-eda/bolshe-belka-menshe-kaloriy/malo-kaloriy/",
            "/goods/gotovaya-eda/bolshe-belka-menshe-kaloriy/bolshe-belka/",
            "/goods/gotovaya-eda/okroshki-i-letnie-supy/",
            "/goods/gotovaya-eda/supy/",
            "/goods/gotovaya-eda/zavtraki/",
            "/goods/gotovaya-eda/zavtraki/bliny-i-oladi/",
            "/goods/gotovaya-eda/zavtraki/syrniki-zapekanki-i-rikotniki/",
            "/goods/gotovaya-eda/zavtraki/omlety-i-zavtraki-s-yaytsom/",
            "/goods/gotovaya-eda/zavtraki/kashi/",
            "/goods/gotovaya-eda/zakuski/",
            "/goods/gotovaya-eda/rolly-i-sety/",
            "/goods/gotovaya-eda/onigiri/",
            "/goods/gotovaya-eda/pirogi-pirozhki-i-lepyeshki/",
            "/goods/gotovaya-eda/privezem-goryachim/",
            "/goods/gotovaya-eda/privezem-goryachim/goryachie-napitki/",
            "/goods/gotovaya-eda/tarelka-zdorovogo-pitaniya/",
            "/goods/gotovaya-eda/veganskie-i-postnye-blyuda/",
            "/goods/gotovaya-eda/semeynyy-format/",
            "/goods/gotovaya-eda/kombo-na-kazhdyy-den/",
            "/goods/gotovaya-eda/kukhni-mira/",
            "/goods/gotovaya-eda/kukhni-mira/aziatskaya-kukhnya/",
            "/goods/gotovaya-eda/kukhni-mira/russkaya-kukhnya/",
            "/goods/gotovaya-eda/kukhni-mira/kukhnya-kavkaza/",
            "/goods/gotovaya-eda/kukhni-mira/sredizemnomorskaya-kukhnya/",
            "/goods/gotovaya-eda/bliny-i-oladi/",
            "/goods/gotovaya-eda/khalyal/"
        ]
        
        for category in ready_food_categories:
            try:
                urls = await self._get_category_products(category, 500)
                product_urls.update(urls)
                print(f"   {category}: +{len(urls)} товаров")
            except Exception as e:
                print(f"   ❌ {category}: {e}")
        
        
        print(f"📦 Всего найдено {len(product_urls)} ссылок на товары")
        
        # Берем максимальное количество товаров
        product_list = list(product_urls)[:limit * 5]  # Увеличиваем запас
        products = []
        
        # Парсим товары батчами
        batch_size = 8
        semaphore = asyncio.Semaphore(6)
        
        for i in range(0, len(product_list), batch_size):
            batch = product_list[i:i + batch_size]
            print(f"🔍 Товары {i+1}-{min(i+batch_size, len(product_list))}/{len(product_list)}")
            
            async def process_product(url):
                async with semaphore:
                    return await self._extract_full_product(url)
            
            tasks = [process_product(url) for url in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, dict) and result:
                    if self._is_ready_food(result):
                        products.append(result)
                        
                        # Проверяем лимит
                        if len(products) >= limit:
                            print(f"🎯 Достигнут лимит {limit} товаров")
                            return products
            
            await asyncio.sleep(1)
        
        print(f"🏁 Тяжелый парсинг завершен: {len(products)} товаров с полными данными")
        return products
    
    async def _set_location(self):
        """Установка локации для Москвы."""
        try:
            location_url = f"{self.BASE_URL}/api/location?city=Москва&lat=55.7558&lon=37.6176"
            await self.antibot_client.request(method="GET", url=location_url)
            print("📍 Локация установлена: Москва (центр)")
        except Exception as e:
            print(f"⚠️ Ошибка установки локации: {e}")
    
    async def _get_category_products(self, category: str, max_products: int) -> List[str]:
        """Получить ВСЕ товары из категории через пагинацию."""
        product_urls = set()
        
        # Обычные страницы пагинации (page=1,2,3...)
        for page_num in range(1, 100):  # До 100 страниц
            try:
                url = f"{self.BASE_URL}{category}?page={page_num}"
                response = await self.antibot_client.request(method="GET", url=url)
                
                if response.status_code != 200:
                    break
                    
                parser = HTMLParser(response.text)
                links = parser.css('a[href*="/goods/"][href$=".html"]')
                
                if not links:
                    break

                page_count = 0
                for link in links:
                    href = link.attributes.get('href')
                    if href and '.html' in href and '/goods/' in href:
                        full_url = urljoin(self.BASE_URL, href)
                        if full_url not in product_urls:
                            product_urls.add(full_url)
                            page_count += 1

                if page_count == 0:  # Нет новых товаров - конец
                    break
                    
                if len(product_urls) >= max_products:
                    break
                    
                await asyncio.sleep(0.2)
                
            except Exception:
                break
        
        # Дополнительная загрузка
        if len(product_urls) < max_products:
            await self._load_more_products(category, product_urls, max_products)
        
        return list(product_urls)
    
    async def _load_more_products(self, category: str, product_urls: set, max_products: int):
        """Простая загрузка дополнительных товаров через пагинацию."""
        # Пробуем загрузить больше страниц через обычную пагинацию
        for page_num in range(2, 20):  # Страницы 2-19
            try:
                url = f"{self.BASE_URL}{category}?page={page_num}"
                response = await self.antibot_client.request(method="GET", url=url)
                
                if response.status_code != 200:
                    break
                    
                parser = HTMLParser(response.text)
                links = parser.css('a[href*="/goods/"][href$=".html"]')
                
                if not links:
                    break

                page_count = 0
                for link in links:
                    href = link.attributes.get('href')
                    if href and '.html' in href and '/goods/' in href:
                        full_url = urljoin(self.BASE_URL, href)
                        if full_url not in product_urls:
                            product_urls.add(full_url)
                            page_count += 1

                if page_count == 0:  # Нет новых товаров - конец
                    break
                    
                if len(product_urls) >= max_products:
                    break
                    
                await asyncio.sleep(0.2)
                
            except Exception:
                break
    
    async def _search_products(self, search_term: str, max_results: int) -> List[str]:
        """Поиск товаров через поисковую систему сайта."""
        product_urls = set()
        
        try:
            # URL поиска ВкусВилл
            search_url = f"{self.BASE_URL}/search/"
            
            # Пробуем разные варианты поиска
            search_variants = [
                {"q": search_term},
                {"search": search_term},
                {"query": search_term},
            ]
            
            for params in search_variants:
                try:
                    response = await self.antibot_client.request(
                        method="GET", 
                        url=search_url,
                        params=params
                    )
                    
                    if response.status_code == 200:
                        parser = HTMLParser(response.text)
                        links = parser.css('a[href*="/goods/"][href$=".html"]')
                        
                        for link in links:
                            href = link.attributes.get('href')
                            if href and '.html' in href and '/goods/' in href:
                                full_url = urljoin(self.BASE_URL, href)
                                product_urls.add(full_url)
                                
                                if len(product_urls) >= max_results:
                                    break
                        
                        if product_urls:  # Если нашли товары, прекращаем пробовать другие варианты
                            break
                            
                except Exception:
                    continue
                    
            # Дополнительно: поиск через POST запрос
            if not product_urls:
                try:
                    response = await self.antibot_client.request(
                        method="POST",
                        url=search_url,
                        data={"q": search_term},
                        headers={'Content-Type': 'application/x-www-form-urlencoded'}
                    )
                    
                    if response.status_code == 200:
                        parser = HTMLParser(response.text)
                        links = parser.css('a[href*="/goods/"][href$=".html"]')
                        
                        for link in links:
                            href = link.attributes.get('href')
                            if href and '.html' in href and '/goods/' in href:
                                full_url = urljoin(self.BASE_URL, href)
                                product_urls.add(full_url)
                                
                except Exception:
                    pass
                    
        except Exception as e:
            print(f"      Ошибка поиска: {e}")
        
        return list(product_urls)
    
    async def _get_sitemap_products(self) -> List[str]:
        """Получение товаров через sitemap.xml."""
        product_urls = set()
        
        try:
            # Пробуем разные варианты sitemap
            sitemap_urls = [
                f"{self.BASE_URL}/sitemap.xml",
                f"{self.BASE_URL}/sitemap_products.xml",
                f"{self.BASE_URL}/robots.txt"  # Может содержать ссылки на sitemap
            ]
            
            for sitemap_url in sitemap_urls:
                try:
                    response = await self.antibot_client.request(method="GET", url=sitemap_url)
                    if response.status_code == 200:
                        # Ищем ссылки на товары в XML
                        import re
                        urls = re.findall(r'https://vkusvill\.ru/goods/[^<]+\.html', response.text)
                        product_urls.update(urls)
                        
                        if len(product_urls) > 100:  # Если нашли много товаров, прекращаем
                            break
                except Exception:
                    continue
                    
        except Exception as e:
            print(f"      Ошибка sitemap: {e}")
        
        return list(product_urls)
    
    async def _get_products_by_id_range(self, start_id: int, end_id: int, max_products: int) -> List[str]:
        """Получение товаров через перебор ID."""
        product_urls = set()
        
        try:
            # Генерируем случайные ID в диапазоне
            import random
            test_ids = random.sample(range(start_id, end_id), min(1000, end_id - start_id))
            
            for product_id in test_ids:
                if len(product_urls) >= max_products:
                    break
                    
                try:
                    # Пробуем разные форматы URL
                    test_urls = [
                        f"{self.BASE_URL}/goods/product-{product_id}.html",
                        f"{self.BASE_URL}/goods/{product_id}.html",
                        f"{self.BASE_URL}/product/{product_id}",
                    ]
                    
                    for test_url in test_urls:
                        response = await self.antibot_client.request(method="HEAD", url=test_url)
                        if response.status_code == 200:
                            product_urls.add(test_url)
                            break
                            
                    # Небольшая пауза
                    if len(product_urls) % 50 == 0:
                        await asyncio.sleep(1)
                        
                except Exception:
                    continue
                    
        except Exception as e:
            print(f"      Ошибка ID перебора: {e}")
        
        return list(product_urls)
    
    def _is_ready_food(self, product: Dict) -> bool:
        """Проверяем что это товар готовой еды."""
        name = product.get('name', '').lower()
        url = product.get('url', '').lower()
        
        # Ключевые слова готовой еды
        ready_food_keywords = [
            'суп', 'салат', 'борщ', 'омлет', 'блины', 'каша', 'пицца',
            'паста', 'котлета', 'запеканка', 'сырники', 'плов', 'лазанья',
            'крем-суп', 'харчо', 'цезарь', 'винегрет', 'мимоза',
            'рагу', 'гуляш', 'жаркое', 'биточки', 'тефтели', 'фрикадельки',
            'голубцы', 'долма', 'манты', 'пельмени', 'вареники', 'хинкали',
            'шаурма', 'бургер', 'сэндвич', 'рулет', 'пирог', 'киш', 'тарт',
            'ризотто', 'паэлья', 'карри', 'рамен', 'фо', 'том-ям', 'мисо',
            'окрошка', 'солянка', 'щи', 'уха', 'рассольник', 'кулеш',
            'завтрак', 'обед', 'ужин'
        ]
        
        # Исключаем НЕ готовую еду
        exclude_keywords = [
            'крем для', 'гель для', 'средство для', 'прокладки', 'подгузники',
            'шампунь', 'бальзам', 'мыло', 'зубная', 'паста зубная',
            'чипсы', 'сухарики', 'орехи', 'семечки', 'конфеты', 'шоколад',
            'молоко', 'кефир', 'йогурт', 'творог', 'сыр', 'масло', 'яйца',
            'мясо', 'курица', 'говядина', 'свинина', 'рыба', 'филе',
            'овощи', 'фрукты', 'картофель', 'капуста', 'морковь',
            'хлеб', 'батон', 'булка', 'багет', 'лаваш'
        ]
        
        # Проверяем URL на готовую еду
        if 'gotovaya-eda' in url:
            return True
        
        # Проверяем название на ключевые слова готовой еды
        if any(keyword in name for keyword in ready_food_keywords):
            # Дополнительно проверяем что это не исключение
            if not any(exclude in name for exclude in exclude_keywords):
                return True
        
        return False
    
    async def _extract_full_product(self, url: str, retry_count: int = 0) -> Optional[Dict]:
        """Полное извлечение товара со всеми данными с retry механизмом."""
        max_retries = 2  # Максимум 2 повторных попытки
        
        try:
            response = await self.antibot_client.request(method="GET", url=url)
            if response.status_code != 200 or not HTMLParser:
                print(f"      ❌ HTTP {response.status_code} для {url}")
                return None
                
            parser = HTMLParser(response.text)
            page_text = response.text
            
            # Базовые данные
            product = {
                'id': self._extract_id(url),
                'name': self._extract_name(parser, page_text),
                'price': self._extract_price(parser, page_text),
                'category': 'Готовая еда',
                'url': url,
                'shop': 'vkusvill_heavy',
                'photo': self._extract_photo(parser),
                'composition': self._extract_composition(parser, page_text),
                'tags': '',
                'portion_g': self._extract_portion_weight(parser, page_text)
            }
            
            # Расширенное извлечение БЖУ
            nutrition = self._extract_bju_comprehensive(parser, page_text)
            product.update(nutrition)
            
            # Детальная статистика по полям
            filled_bju = sum(1 for field in ['kcal_100g', 'protein_100g', 'fat_100g', 'carb_100g'] if product.get(field))
            has_composition = bool(product.get('composition'))
            
            # Краткие логи
            print(f"      📦 {product['name'][:40]}... БЖУ:{filled_bju}/4 Состав:{'✓' if has_composition else '✗'} Цена:{product['price'] or '?'}")
            
            # Retry для состава
            if not has_composition and retry_count < max_retries:
                await asyncio.sleep(0.5)
                return await self._extract_full_product(url, retry_count + 1)
            
            # Проверка базовых данных
            if not product['name']:
                print(f"      ❌ Нет названия для {url}")
                return None
            
            return product
            
        except Exception as e:
            if retry_count < max_retries:
                await asyncio.sleep(1)
                return await self._extract_full_product(url, retry_count + 1)
            return None
    
    def _extract_id(self, url: str) -> str:
        """ID товара из URL."""
        match = re.search(r'/goods/([^/]+)\.html', url)
        return match.group(1) if match else str(hash(url))[-8:]
    
    def _extract_name(self, parser, page_text: str) -> str:
        """Название товара."""
        selectors = ['h1', '.product-title', '.goods-title']
        for selector in selectors:
            element = parser.css_first(selector)
            if element and element.text(strip=True):
                return element.text(strip=True)[:150]
        return ""
    
    def _extract_price(self, parser, page_text: str) -> str:
        """Цена товара - улучшенное извлечение."""
        # Расширенные селекторы для цены
        selectors = [
            '.price', '.product-price', '.cost', '.goods-price',
            '[data-testid*="price"]', '[class*="price"]', 
            '.js-product-price', '.current-price'
        ]
        
        for selector in selectors:
            elements = parser.css(selector)
            for element in elements:
                price_text = element.text(strip=True)
                # Ищем числа в тексте цены
                numbers = re.findall(r'(\d+(?:[.,]\d+)?)', price_text)
                for num in numbers:
                    try:
                        price_val = float(num.replace(',', '.'))
                        if 10 <= price_val <= 10000:  # Разумный диапазон цен
                            return num.replace(',', '.')
                    except ValueError:
                        continue
        
        # Поиск в JSON данных
        json_patterns = [
            r'"price"\s*:\s*"?(\d+(?:[.,]\d+)?)"?',
            r'"cost"\s*:\s*"?(\d+(?:[.,]\d+)?)"?',
            r'"currentPrice"\s*:\s*"?(\d+(?:[.,]\d+)?)"?'
        ]
        for pattern in json_patterns:
            match = re.search(pattern, page_text, re.I)
            if match:
                try:
                    price_val = float(match.group(1).replace(',', '.'))
                    if 10 <= price_val <= 10000:
                        return match.group(1).replace(',', '.')
                except ValueError:
                    continue
        
        # Поиск по тексту страницы
        text_patterns = [
            r'(\d+(?:[.,]\d+)?)\s*руб',
            r'(\d+(?:[.,]\d+)?)\s*₽',
            r'цена[:\s]*(\d+(?:[.,]\d+)?)',
            r'стоимость[:\s]*(\d+(?:[.,]\d+)?)'
        ]
        for pattern in text_patterns:
            matches = re.finditer(pattern, page_text, re.I)
            for match in matches:
                try:
                    price_val = float(match.group(1).replace(',', '.'))
                    if 10 <= price_val <= 10000:
                        return match.group(1).replace(',', '.')
                except ValueError:
                    continue
        
        return ""
    
    def _extract_photo(self, parser) -> str:
        """Фото товара - улучшенное извлечение."""
        # Расширенные селекторы для фото
        selectors = [
            'img',  # Все изображения для начала
            'img[src*="product"]', 
            '.product-image img',
            '[data-testid*="image"] img',
            '.gallery img',
            '.main-image img',
            'img[alt*="product"]',
            'img[src*="goods"]',
            'img[data-src*="product"]',
            'img[data-src*="goods"]'
        ]
        
        for selector in selectors:
            elements = parser.css(selector)
            for element in elements:
                # Проверяем src и data-src
                src = element.attributes.get('src') or element.attributes.get('data-src')
                if src:
                    # Проверяем что это изображение товара (не иконка, не логотип)
                    if any(keyword in src.lower() for keyword in ['product', 'goods', 'catalog', 'upload', 'resize']):
                        # Проверяем что это не маленькая иконка
                        width = element.attributes.get('width')
                        height = element.attributes.get('height')
                        if width and height:
                            try:
                                w, h = int(width), int(height)
                                if w < 50 or h < 50:  # Пропускаем маленькие изображения
                                    continue
                            except ValueError:
                                pass
                        
                        # Пропускаем системные изображения
                        if any(skip in src.lower() for skip in ['icon', 'logo', 'banner', 'button', 'svg']):
                            continue
                        
                        full_url = urljoin(self.BASE_URL, src)
                        # Проверяем что URL валидный
                        if full_url.startswith('http'):
                            return full_url
                    
                    # Если не нашли по ключевым словам, берем любое большое изображение
                    elif src.startswith('/') and any(ext in src.lower() for ext in ['.jpg', '.jpeg', '.png', '.webp']):
                        width = element.attributes.get('width')
                        height = element.attributes.get('height')
                        if width and height:
                            try:
                                w, h = int(width), int(height)
                                if w >= 100 and h >= 100:  # Берем только большие изображения
                                    full_url = urljoin(self.BASE_URL, src)
                                    return full_url
                            except ValueError:
                                pass
        
        return ""
    
    def _extract_composition(self, parser, page_text: str) -> str:
        """Состав товара - простое извлечение."""
        # Поиск по ключевому слову "Состав"
        elements = parser.css('div, p, span, td, li')
        for element in elements:
            text = element.text().strip()
            text_lower = text.lower()
            
            if 'состав' in text_lower and len(text) > 10:
                if not any(word in text_lower for word in ['меню', 'каталог', 'корзина', 'вкусвилл', 'доставки', 'выберите']):
                    if text_lower.startswith('состав'):
                        return text[:800]
                    elif len(text) < 800:
                        return text[:500]
        
        return ""
    
    def _extract_portion_weight(self, parser, page_text: str) -> str:
        """Вес порции."""
        patterns = [r'(\d+(?:[.,]\d+)?)\s*(?:г|гр|грам)']
        for pattern in patterns:
            matches = re.finditer(pattern, page_text, re.I)
            for match in matches:
                weight = float(match.group(1).replace(',', '.'))
                if 10 <= weight <= 2000:
                    return f"{weight}г"
        return ""
    
    def _extract_bju_comprehensive(self, parser, page_text: str) -> Dict[str, str]:
        """Максимально полное извлечение БЖУ с детальными логами."""
        nutrition = {'kcal_100g': '', 'protein_100g': '', 'fat_100g': '', 'carb_100g': ''}
        # Метод 1: Поиск в JSON-LD
        self._extract_nutrition_from_jsonld(page_text, nutrition)
        
        # Метод 2: Поиск в таблицах
        self._extract_nutrition_from_tables(parser, nutrition)
        
        # Метод 3: Поиск в элементах страницы (улучшенный)
        bju_elements_found = 0
        elements = parser.css('div, span, p, td, th, li')
        for element in elements:
            text = element.text().lower()
            original_text = element.text()
            
            # Пропускаем элементы с слишком большим количеством чисел (скорее всего мусор)
            numbers_count = len(re.findall(r'\d+', original_text))
            if numbers_count > 10:
                continue
                
            if any(word in text for word in ['ккал', 'белки', 'жиры', 'углеводы', 'энергетическая', 'калорийность', 'пищевая', 'ценность', 'состав']):
                bju_elements_found += 1
                
                # Ищем число РЯДОМ с ключевым словом
                if ('ккал' in text or 'калорийность' in text or 'энергетическая' in text) and not nutrition['kcal_100g']:
                    # Паттерны для поиска калорий (включая структуру ВкусВилл)
                    kcal_patterns = [
                        r'(\d+(?:[.,]\d+)?)\s*ккал',  # "189.6 Ккал"
                        r'ккал[:\s]*(\d+(?:[.,]\d+)?)',
                        r'калорийность[:\s]*(\d+(?:[.,]\d+)?)',
                        r'энергетическая\s+ценность[:\s]*(\d+(?:[.,]\d+)?)',
                        r'(\d+(?:[.,]\d+)?)\s+ккал'  # "189.6 Ккал" с пробелом
                    ]
                    for pattern in kcal_patterns:
                        match = re.search(pattern, text)
                        if match:
                            try:
                                val = float(match.group(1).replace(',', '.'))
                                if 10 <= val <= 900:
                                    nutrition['kcal_100g'] = match.group(1).replace(',', '.')
                                    break
                            except ValueError:
                                continue
                
                if 'белк' in text and not nutrition['protein_100g']:
                    protein_patterns = [
                        r'(\d+(?:[.,]\d+)?)\s+белки,\s*г',  # "11 Белки, г"
                        r'белк[иа][:\s]*(\d+(?:[.,]\d+)?)',
                        r'белок[:\s]*(\d+(?:[.,]\d+)?)',
                        r'(\d+(?:[.,]\d+)?)\s*г\s*белк'
                    ]
                    for pattern in protein_patterns:
                        match = re.search(pattern, text)
                        if match:
                            try:
                                val = float(match.group(1).replace(',', '.'))
                                if 0 <= val <= 100:
                                    nutrition['protein_100g'] = match.group(1).replace(',', '.')
                                    break
                            except ValueError:
                                continue
                
                if 'жир' in text and not nutrition['fat_100g']:
                    fat_patterns = [
                        r'(\d+(?:[.,]\d+)?)\s+жиры,\s*г',  # "7.6 Жиры, г"
                        r'жир[ыа][:\s]*(\d+(?:[.,]\d+)?)',
                        r'жир[:\s]*(\d+(?:[.,]\d+)?)',
                        r'(\d+(?:[.,]\d+)?)\s*г\s*жир'
                    ]
                    for pattern in fat_patterns:
                        match = re.search(pattern, text)
                        if match:
                            try:
                                val = float(match.group(1).replace(',', '.'))
                                if 0 <= val <= 100:
                                    nutrition['fat_100g'] = match.group(1).replace(',', '.')
                                    break
                            except ValueError:
                                continue
                
                if 'углевод' in text and not nutrition['carb_100g']:
                    carb_patterns = [
                        r'(\d+(?:[.,]\d+)?)\s+углеводы,\s*г',  # "19.3 Углеводы, г"
                        r'углевод[ыа][:\s]*(\d+(?:[.,]\d+)?)',
                        r'углевод[:\s]*(\d+(?:[.,]\d+)?)',
                        r'(\d+(?:[.,]\d+)?)\s*г\s*углевод'
                    ]
                    for pattern in carb_patterns:
                        match = re.search(pattern, text)
                        if match:
                            try:
                                val = float(match.group(1).replace(',', '.'))
                                if 0 <= val <= 100:
                                    nutrition['carb_100g'] = match.group(1).replace(',', '.')
                                    break
                            except ValueError:
                                continue
        
        # Метод 4: Поиск по регулярным выражениям в тексте
        patterns = {
            'kcal_100g': [
                r'(\d+(?:[.,]\d+)?)\s+ккал',  # "189.6 Ккал" - основной паттерн ВкусВилл
                r'(\d+(?:[.,]\d+)?)\s*ккал',
                r'калорийность[:\s]*(\d+(?:[.,]\d+)?)',
                r'энергетическая\s+ценность[:\s]*(\d+(?:[.,]\d+)?)',
                r'энергия[:\s]*(\d+(?:[.,]\d+)?)\s*ккал'
            ],
            'protein_100g': [
                r'(\d+(?:[.,]\d+)?)\s+белки,\s*г',  # "11 Белки, г" - основной паттерн ВкусВилл
                r'белки[:\s]*(\d+(?:[.,]\d+)?)',
                r'белок[:\s]*(\d+(?:[.,]\d+)?)',
                r'протеин[:\s]*(\d+(?:[.,]\d+)?)'
            ],
            'fat_100g': [
                r'(\d+(?:[.,]\d+)?)\s+жиры,\s*г',  # "7.6 Жиры, г" - основной паттерн ВкусВилл
                r'жиры[:\s]*(\d+(?:[.,]\d+)?)',
                r'жир[:\s]*(\d+(?:[.,]\d+)?)'
            ],
            'carb_100g': [
                r'(\d+(?:[.,]\d+)?)\s+углеводы,\s*г',  # "19.3 Углеводы, г" - основной паттерн ВкусВилл
                r'углеводы[:\s]*(\d+(?:[.,]\d+)?)',
                r'углевод[:\s]*(\d+(?:[.,]\d+)?)'
            ]
        }
        
        for field, field_patterns in patterns.items():
            if nutrition[field]:
                continue
            for pattern in field_patterns:
                matches = list(re.finditer(pattern, page_text, re.I))
                for match in matches:
                    try:
                        value = float(match.group(1).replace(',', '.'))
                        if field == 'kcal_100g' and 10 <= value <= 900:
                            nutrition[field] = str(value)
                            break
                        elif field != 'kcal_100g' and 0 <= value <= 100:
                            nutrition[field] = str(value)
                            break
                    except ValueError:
                        continue
                if nutrition[field]:
                    break
        
        # Простая статистика БЖУ
        filled_total = sum(1 for v in nutrition.values() if v)
        
        return nutrition
    
    def _extract_nutrition_from_jsonld(self, page_text: str, nutrition: Dict[str, str]):
        """Извлечение БЖУ из JSON-LD."""
        try:
            blocks = re.findall(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', page_text, re.S|re.I)
            for raw in blocks:
                try:
                    data = json.loads(raw)
                    print(f"         JSON-LD блок найден")
                    
                    def visit(obj):
                        if isinstance(obj, dict):
                            if obj.get('@type') in ('NutritionInformation', 'Nutrition'):
                                print(f"         Найден NutritionInformation блок!")
                                kcal = obj.get('calories') or obj.get('energy')
                                protein = obj.get('proteinContent')
                                fat = obj.get('fatContent')
                                carb = obj.get('carbohydrateContent')
                                
                                if kcal: nutrition['kcal_100g'] = str(kcal)
                                if protein: nutrition['protein_100g'] = str(protein)
                                if fat: nutrition['fat_100g'] = str(fat)
                                if carb: nutrition['carb_100g'] = str(carb)
                            
                            for v in obj.values():
                                visit(v)
                        elif isinstance(obj, list):
                            for v in obj:
                                visit(v)
                    
                    visit(data)
                except:
                    continue
        except:
            pass
    
    def _extract_nutrition_from_tables(self, parser, nutrition: Dict[str, str]):
        """Извлечение БЖУ из таблиц."""
        try:
            tables = parser.css('table')
            print(f"         Найдено таблиц: {len(tables)}")
            for i, table in enumerate(tables):
                print(f"         Анализируем таблицу {i+1}")
                rows = table.css('tr')
                for row in rows:
                    cells = row.css('td, th')
                    if len(cells) >= 2:
                        header = cells[0].text().lower()
                        value_text = cells[1].text()
                        
                        # Извлечение числа
                        num_match = re.search(r'(\d+(?:[.,]\d+)?)', value_text)
                        if num_match:
                            value = num_match.group(1).replace(',', '.')
                            print(f"         Строка таблицы: '{header}' = '{value}'")
                            
                            if ('ккал' in header or 'калорийность' in header) and not nutrition['kcal_100g']:
                                nutrition['kcal_100g'] = value
                                print(f"         ✅ Из таблицы ккал: {value}")
                            elif 'белк' in header and not nutrition['protein_100g']:
                                nutrition['protein_100g'] = value
                                print(f"         ✅ Из таблицы белки: {value}")
                            elif 'жир' in header and not nutrition['fat_100g']:
                                nutrition['fat_100g'] = value
                                print(f"         ✅ Из таблицы жиры: {value}")
                            elif 'углевод' in header and not nutrition['carb_100g']:
                                nutrition['carb_100g'] = value
                                print(f"         ✅ Из таблицы углеводы: {value}")
        except:
            pass


async def main():
    """Главная функция тяжелого парсера."""
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 1500
    
    print("🏗️ ТЯЖЕЛЫЙ ПАРСЕР ВКУСВИЛЛА - МОСКВА")
    print("=" * 50)
    print(f"🎯 Цель: {limit} товаров с полными данными")
    print("📍 Местоположение: Москва")
    print("⚡ Режим: Глубокий анализ каждой карточки")
    print()
    
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
    
    antibot_client = AntiBotClient(concurrency=8, timeout=60)
    
    try:
        parser = VkusvillHeavyParser(antibot_client)
        
        start_time = time.time()
        products = await parser.scrape_heavy(limit)
        end_time = time.time()
        
        if not products:
            print("❌ Тяжелый парсинг не дал результатов")
            return
        
        # Детальная статистика БЖУ и состава
        bju_stats = {'full_bju': 0, 'good_bju': 0, 'some_bju': 0, 'no_bju': 0}
        composition_stats = {'has_composition': 0, 'no_composition': 0}
        quality_stats = {'excellent': 0, 'good': 0, 'poor': 0}
        
        for product in products:
            bju_fields = ['kcal_100g', 'protein_100g', 'fat_100g', 'carb_100g']
            filled = sum(1 for field in bju_fields if product.get(field))
            has_composition = bool(product.get('composition'))
            
            # Статистика БЖУ
            if filled == 4:
                bju_stats['full_bju'] += 1
            elif filled == 3:
                bju_stats['good_bju'] += 1
            elif filled >= 1:
                bju_stats['some_bju'] += 1
            else:
                bju_stats['no_bju'] += 1
            
            # Статистика состава
            if has_composition:
                composition_stats['has_composition'] += 1
            else:
                composition_stats['no_composition'] += 1
            
            # Общая оценка качества
            if filled >= 3 and has_composition:
                quality_stats['excellent'] += 1
            elif filled >= 2 or has_composition:
                quality_stats['good'] += 1
            else:
                quality_stats['poor'] += 1
        
        # Сохранение результатов
        timestamp = int(time.time())
        csv_file = f"data/moscow_heavy_{timestamp}.csv"
        jsonl_file = f"data/moscow_heavy_{timestamp}.jsonl"
        
        Path("data").mkdir(exist_ok=True)
        
        if products:
            fieldnames = list(products[0].keys())
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(products)
        
        with open(jsonl_file, 'w', encoding='utf-8') as f:
            for product in products:
                f.write(json.dumps(product, ensure_ascii=False) + '\n')
        
        # Расширенная итоговая статистика
        duration = end_time - start_time
        print()
        print("🏁 ТЯЖЕЛЫЙ ПАРСИНГ ЗАВЕРШЕН")
        print("=" * 60)
        print(f"📊 ОБЩИЕ РЕЗУЛЬТАТЫ:")
        print(f"   • Всего товаров: {len(products)}")
        print(f"   • Скорость: {len(products)/(duration/60):.1f} товаров/мин")
        print()
        print(f"🍽️ СТАТИСТИКА БЖУ:")
        print(f"   • Полное БЖУ (4/4): {bju_stats['full_bju']} ({bju_stats['full_bju']/len(products)*100:.1f}%)")
        print(f"   • Хорошее БЖУ (3/4): {bju_stats['good_bju']} ({bju_stats['good_bju']/len(products)*100:.1f}%)")
        print(f"   • Частичное БЖУ (1-2/4): {bju_stats['some_bju']} ({bju_stats['some_bju']/len(products)*100:.1f}%)")
        print(f"   • Без БЖУ (0/4): {bju_stats['no_bju']} ({bju_stats['no_bju']/len(products)*100:.1f}%)")
        print()
        print(f"📝 СТАТИСТИКА СОСТАВА:")
        print(f"   • Есть состав: {composition_stats['has_composition']} ({composition_stats['has_composition']/len(products)*100:.1f}%)")
        print(f"   • Нет состава: {composition_stats['no_composition']} ({composition_stats['no_composition']/len(products)*100:.1f}%)")
        print()
        print(f"⭐ ОБЩЕЕ КАЧЕСТВО ДАННЫХ:")
        print(f"   • Отличное (БЖУ 3+ + состав): {quality_stats['excellent']} ({quality_stats['excellent']/len(products)*100:.1f}%)")
        print(f"   • Хорошее (БЖУ 2+ ИЛИ состав): {quality_stats['good']} ({quality_stats['good']/len(products)*100:.1f}%)")
        print(f"   • Плохое (БЖУ <2 И нет состава): {quality_stats['poor']} ({quality_stats['poor']/len(products)*100:.1f}%)")
        print()
        print(f"⏱️  Время выполнения: {duration/60:.1f} минут")
        print(f"💾 Файлы сохранены:")
        print(f"   • CSV: {csv_file}")
        print(f"   • JSONL: {jsonl_file}")
            
    except KeyboardInterrupt:
        print("\n⚠️ Парсинг прерван пользователем")
    except Exception as e:
        print(f"❌ Ошибка тяжелого парсинга: {e}")
    finally:
        await antibot_client.close()


if __name__ == "__main__":
    asyncio.run(main())

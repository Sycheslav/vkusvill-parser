#!/usr/bin/env python3
"""
⚡ БЫСТРЫЙ ПАРСЕР ВКУСВИЛЛА ПО ГЕОЛОКАЦИИ
Быстро собирает основные данные товаров без захода в карточки.

ОСОБЕННОСТИ:
- Парсинг только с каталожных страниц (без захода в карточки)
- Сбор только основных данных: ID, название, цена
- Очень быстрая работа (секунды вместо часов)
- Работает с любой геолокацией
- Использует базу тяжелого парсера для дополнения данных

ИСПОЛЬЗОВАНИЕ:
python3 address.py "Адрес" [количество_товаров]
python3 address.py  # Интерактивный режим

ПРИМЕРЫ:
python3 address.py "Москва, Красная площадь, 1" 200
python3 address.py "Санкт-Петербург, Невский проспект, 10" 300
python3 address.py "55.7558,37.6176" 100
python3 address.py  # Запуск в интерактивном режиме
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

# Встроенные классы
import httpx
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError


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

class LocationService:
    """Простой сервис геолокации."""
    
    def __init__(self):
        self.nominatim = Nominatim(user_agent="vkusvill-scraper/1.0")
        # Популярные адреса
        self.test_addresses = {
            "Москва, Красная площадь, 1": (55.7539, 37.6208),
            "Москва, Тверская улица, 1": (55.7558, 37.6176),
            "Санкт-Петербург, Невский проспект, 1": (59.9311, 30.3609),
        }
        
    async def geocode_address(self, address: str) -> Optional[tuple]:
        """Геокодировать адрес в координаты."""
        # Проверяем популярные адреса
        if address in self.test_addresses:
            return self.test_addresses[address]
            
        try:
            location = self.nominatim.geocode(address, timeout=10)
            if location:
                return (location.latitude, location.longitude)
        except (GeocoderTimedOut, GeocoderServiceError):
            pass
            
        # Возвращаем Москву по умолчанию
        return (55.7558, 37.6176)


class VkusvillFastParser:
    """Быстрый парсер без захода в карточки товаров."""
    
    def __init__(self, antibot_client):
        self.antibot_client = antibot_client
        self.BASE_URL = "https://vkusvill.ru"
        self.heavy_data = {}  # База данных тяжелого парсера

    def load_heavy_data(self, heavy_file_path: str = None):
        """Загрузка данных тяжелого парсера."""
        if not heavy_file_path:
            # Поиск последнего файла тяжелого парсера
            data_dir = Path(__file__).parent / "data"
            if data_dir.exists():
                heavy_files = list(data_dir.glob("moscow_improved_*.csv"))
                if not heavy_files:
                    heavy_files = list(data_dir.glob("moscow_heavy_*.csv"))

                if heavy_files:
                    # Берем последний по времени модификации
                    heavy_file_path = max(heavy_files, key=lambda p: p.stat().st_mtime)

        if heavy_file_path and Path(heavy_file_path).exists():
            print(f"📚 Загружаем базу тяжелого парсера: {heavy_file_path}")
            try:
                with open(heavy_file_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get('id'):  # Проверяем наличие ID
                            self.heavy_data[row['id']] = row
                print(f"   ✅ Загружено {len(self.heavy_data)} товаров из базы")
            except Exception as e:
                print(f"   ❌ Ошибка загрузки базы: {e}")
                import traceback
                traceback.print_exc()
        else:
            print("⚠️ База тяжелого парсера не найдена, работаем только с каталогом")
    
    async def scrape_fast(self, city: str, coords: str, address: str = None, limit: int = 100) -> List[Dict]:
        """Быстрый парсинг - сначала проверяем доступность по адресу, потом сопоставляем с базой."""
        print(f"⚡ Начинаем быстрый парсинг на {limit} товаров...")
        print(f"📍 Локация: {address or city}")
        
        # Установка локации
        await self._set_location(city, coords)
        
        # Сначала получаем список доступных товаров по адресу
        print(f"🔍 Проверяем доступность товаров по адресу...")
        available_product_ids = await self._get_available_products(coords)
        print(f"📦 По адресу доступно: {len(available_product_ids)} товаров")
        
        products = []
        
        # Если есть база тяжелого парсера - сопоставляем с доступными товарами
        if self.heavy_data and available_product_ids:
            print(f"📚 Сопоставляем с базой тяжелого парсера...")
            matched_count = 0
            
            for product_id in available_product_ids[:limit]:
                if product_id in self.heavy_data:
                    heavy_product = self.heavy_data[product_id]
                    # Определяем подкатегорию для товаров из базы
                    subcategory = self._determine_subcategory(
                        heavy_product.get('url', ''), 
                        heavy_product.get('name', '')
                    )
                    product = {
                        'id': heavy_product.get('id', product_id),
                        'name': heavy_product.get('name', ''),
                        'price': heavy_product.get('price', ''),
                        'category': subcategory,
                        'url': heavy_product.get('url', ''),
                        'shop': 'vkusvill_address',
                        'photo': heavy_product.get('photo', ''),
                        'composition': heavy_product.get('composition', ''),
                        'tags': heavy_product.get('tags', ''),
                        'portion_g': heavy_product.get('portion_g', ''),
                        'kcal_100g': heavy_product.get('kcal_100g', ''),
                        'protein_100g': heavy_product.get('protein_100g', ''),
                        'fat_100g': heavy_product.get('fat_100g', ''),
                        'carb_100g': heavy_product.get('carb_100g', '')
                    }
                    products.append(product)
                    matched_count += 1
            
            print(f"✅ Сопоставлено с базой: {matched_count} товаров")
            print(f"⚡ Быстрый парсинг завершен: {len(products)} товаров")
            return products
        
        # Если базы нет - пробуем парсить каталог
        print("⚠️ База тяжелого парсера пуста, пробуем парсить каталог...")
        return await self._fallback_catalog_parsing(limit)
    
    async def _get_available_products(self, coords: str) -> List[str]:
        """Получение списка доступных товаров по адресу."""
        available_ids = []
        
        # Расширенные категории готовой еды (как в moscow_improved.py)
        categories = [
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
        
        for category in categories:
            try:
                # Пагинация по страницам (как в moscow_improved.py)
                for page_num in range(1, 20):  # До 20 страниц на категорию
                    try:
                        url = f"{self.BASE_URL}{category}?page={page_num}"
                        response = await self.antibot_client.request(method="GET", url=url)
                        
                        if response.status_code != 200:
                            break
                            
                        if not HTMLParser:
                            break
                            
                        parser = HTMLParser(response.text)
                        product_links = parser.css('a[href*="/goods/"][href$=".html"]')
                        
                        if not product_links:
                            break

                        page_count = 0
                        for link in product_links:
                            href = link.attributes.get('href')
                            if href and '.html' in href and '/goods/' in href:
                                product_id = self._extract_id_from_url(urljoin(self.BASE_URL, href))
                                if product_id and product_id not in available_ids:
                                    available_ids.append(product_id)
                                    page_count += 1

                        if page_count == 0:  # Нет новых товаров - конец
                            break
                            
                        await asyncio.sleep(0.2)  # Пауза между страницами
                        
                    except Exception:
                        break
                
                await asyncio.sleep(0.5)  # Пауза между категориями
                
            except Exception as e:
                print(f"   ❌ Ошибка категории {category}: {e}")
        
        return available_ids
    
    async def _fallback_catalog_parsing(self, limit: int) -> List[Dict]:
        """Резервный парсинг каталога если нет базы."""
        categories = [
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
        
        products = []
        
        for category in categories:
            try:
                category_products = await self._parse_category_fast(category, limit - len(products))
                products.extend(category_products)
                print(f"   {category}: найдено {len(category_products)} товаров")
                
                if len(products) >= limit:
                    break
                    
            except Exception as e:
                print(f"   ❌ Ошибка категории {category}: {e}")
        
        print(f"⚡ Быстрый парсинг завершен: {len(products)} товаров")
        return products[:limit]

    async def _set_location(self, city: str, coords: str):
        """Установка локации с правильной обработкой cookies."""
        try:
            lat, lon = coords.split(',')

            # Получаем главную страницу для cookies
            main_response = await self.antibot_client.request(method="GET", url=self.BASE_URL)

            # Используем правильный API endpoint
            headers = {
                'X-Requested-With': 'XMLHttpRequest',
                'Content-Type': 'application/json',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Referer': self.BASE_URL
            }

            # Сначала пробуем новый API
            location_data = {
                "lat": float(lat.strip()),
                "lon": float(lon.strip()),
                "radius": 5000,
                "city": city
            }

            try:
                response = await self.antibot_client.request(
                    method="POST",
                    url=f"{self.BASE_URL}/ajax/user/setCoords/",
                    json=location_data,
                    headers=headers
                )

                if response.status_code == 200:
                    print(f"📍 Локация установлена: {city} ({coords})")
                    return
            except:
                pass

            # Альтернативный метод через GET
            location_url = f"{self.BASE_URL}/ajax/user/setCoords/?lat={lat.strip()}&lon={lon.strip()}"
            await self.antibot_client.request(
                method="GET",
                url=location_url,
                headers={'X-Requested-With': 'XMLHttpRequest'}
            )
            print(f"📍 Локация установлена (альтернативный метод): {city}")

        except Exception as e:
            print(f"⚠️ Ошибка установки локации: {e}")
            # Продолжаем даже если не удалось


    async def _parse_category_fast(self, category: str, max_products: int) -> List[Dict]:
        """Быстрый парсинг категории без захода в карточки с пагинацией."""
        products = []
        
        try:
            # Пагинация по страницам (как в moscow_improved.py)
            for page_num in range(1, 10):  # До 10 страниц на категорию
                try:
                    url = f"{self.BASE_URL}{category}?page={page_num}"
                    print(f"   🔍 Парсим страницу {page_num}: {url}")
                    response = await self.antibot_client.request(method="GET", url=url)
                    
                    if response.status_code != 200:
                        break
                        
                    if not HTMLParser:
                        print(f"   ❌ HTMLParser недоступен")
                        break
                        
                    parser = HTMLParser(response.text)
                    
                    # Ищем все ссылки на товары
                    product_links = parser.css('a[href*="/goods/"][href$=".html"]')
                    print(f"   📦 Страница {page_num}: найдено {len(product_links)} ссылок")
                    
                    if not product_links:
                        break
                    
                    page_products = 0
                    for link in product_links:
                        if len(products) >= max_products:
                            break
                            
                        product = self._extract_product_from_link(link)
                        if product:
                            # Дополняем данными из тяжелого парсера если есть
                            if product['id'] in self.heavy_data:
                                heavy_product = self.heavy_data[product['id']]
                                product.update({
                                    'kcal_100g': heavy_product.get('kcal_100g', ''),
                                    'protein_100g': heavy_product.get('protein_100g', ''),
                                    'fat_100g': heavy_product.get('fat_100g', ''),
                                    'carb_100g': heavy_product.get('carb_100g', ''),
                                    'composition': heavy_product.get('composition', ''),
                                    'photo': heavy_product.get('photo', ''),
                                    'portion_g': heavy_product.get('portion_g', '')
                                })
                            
                            products.append(product)
                            page_products += 1
                            print(f"   ✅ {product['name'][:50]}...")
                    
                    if page_products == 0:  # Нет новых товаров - конец
                        break
                        
                    await asyncio.sleep(0.2)  # Пауза между страницами
                    
                except Exception as e:
                    print(f"   ❌ Ошибка страницы {page_num}: {e}")
                    break
        
        except Exception as e:
            print(f"   ❌ Ошибка парсинга категории: {e}")
            import traceback
            traceback.print_exc()
        
        return products
    
    def _extract_product_from_link(self, link) -> Optional[Dict]:
        """Извлечение данных товара из ссылки на каталожной странице."""
        try:
            url = urljoin(self.BASE_URL, link.attributes.get('href'))
            product_id = self._extract_id_from_url(url)
            
            # Название товара из текста ссылки или title
            name = link.text(strip=True) or link.attributes.get('title', '')
            
            # Ищем цену в родительском элементе
            parent = link.parent
            price = ""
            
            # Поиск цены в разных местах
            for _ in range(3):  # Поднимаемся до 3 уровней вверх
                if parent:
                    price_elements = parent.css('.price, [class*="price"], [class*="cost"]')
                    for price_elem in price_elements:
                        price_text = price_elem.text(strip=True)
                        match = re.search(r'(\d+(?:[.,]\d+)?)', price_text)
                        if match:
                            price = match.group(1).replace(',', '.')
                            break
                    if price:
                        break
                    parent = parent.parent
            
            if not name:
                return None
            
            # Определяем подкатегорию
            subcategory = self._determine_subcategory(url, name)
            
            return {
                'id': product_id,
                'name': name[:150],
                'price': price,
                'category': subcategory,
                'url': url,
                'shop': 'vkusvill_fast',
                'photo': '',
                'composition': '',
                'tags': '',
                'portion_g': '',
                'kcal_100g': '',
                'protein_100g': '',
                'fat_100g': '',
                'carb_100g': ''
            }
            
        except Exception as e:
            return None
    
    def _extract_product_from_block(self, block) -> Optional[Dict]:
        """Извлечение данных товара из блока на каталожной странице."""
        try:
            # Поиск ссылки на товар
            link = block.css_first('a[href*="/goods/"][href$=".html"]')
            if not link:
                return None
            
            url = urljoin(self.BASE_URL, link.attributes.get('href'))
            product_id = self._extract_id_from_url(url)
            
            # Название товара
            name = ""
            name_selectors = ['h3', '.title', '.name', '[data-testid*="name"]']
            for selector in name_selectors:
                element = block.css_first(selector)
                if element and element.text(strip=True):
                    name = element.text(strip=True)
                    break
            
            if not name:
                # Название из ссылки
                name = link.text(strip=True)
            
            # Цена товара
            price = ""
            price_selectors = ['.price', '.cost', '[data-testid*="price"]']
            for selector in price_selectors:
                element = block.css_first(selector)
                if element:
                    price_text = element.text(strip=True)
                    match = re.search(r'(\d+(?:[.,]\d+)?)', price_text)
                    if match:
                        price = match.group(1).replace(',', '.')
                        break
            
            # Фото товара (если есть в блоке)
            photo = ""
            img = block.css_first('img')
            if img:
                src = img.attributes.get('src') or img.attributes.get('data-src')
                if src:
                    photo = urljoin(self.BASE_URL, src)
            
            if not name:
                return None
            
            # Определяем подкатегорию
            subcategory = self._determine_subcategory(url, name)
            
            return {
                'id': product_id,
                'name': name[:150],
                'price': price,
                'category': subcategory,
                'url': url,
                'shop': 'vkusvill_fast',
                'photo': photo,
                'composition': '',
                'tags': '',
                'portion_g': '',
                'kcal_100g': '',
                'protein_100g': '',
                'fat_100g': '',
                'carb_100g': ''
            }
            
        except Exception as e:
            return None
    
    def _extract_id_from_url(self, url: str) -> str:
        """Извлечение ID товара из URL."""
        match = re.search(r'/goods/([^/]+)\.html', url)
        return match.group(1) if match else str(hash(url))[-8:]
    
    def _determine_subcategory(self, url: str, name: str) -> str:
        """Определение подкатегории товара по URL и названию."""
        url_lower = url.lower()
        name_lower = name.lower()
        
        # Определяем по URL категории
        if '/salaty/' in url_lower:
            return 'Салаты'
        elif '/supy/' in url_lower:
            return 'Супы'
        elif '/sendvichi-shaurma-i-burgery/' in url_lower:
            if any(word in name_lower for word in ['сэндвич', 'сендвич', 'бургер', 'шаурма']):
                return 'Сэндвичи и бургеры'
            return 'Сэндвичи и бургеры'
        elif '/vtorye-blyuda/' in url_lower:
            if '/vtorye-blyuda-s-myasom/' in url_lower:
                return 'Вторые блюда с мясом'
            elif '/vtorye-blyuda-s-ptitsey/' in url_lower:
                return 'Вторые блюда с птицей'
            elif '/vtorye-blyuda-s-ryboy-i-moreproduktami/' in url_lower:
                return 'Вторые блюда с рыбой'
            elif '/garniry-i-vtorye-blyuda-bez-myasa/' in url_lower:
                return 'Гарниры и вегетарианские блюда'
            elif '/pasta-pitstsa/' in url_lower:
                if 'пицц' in name_lower:
                    return 'Пицца'
                elif any(word in name_lower for word in ['паста', 'макарон', 'спагетти', 'фетучини']):
                    return 'Паста'
                return 'Паста и пицца'
            return 'Вторые блюда'
        elif '/zavtraki/' in url_lower:
            if '/bliny-i-oladi/' in url_lower or any(word in name_lower for word in ['блины', 'оладьи', 'олади']):
                return 'Блины и оладьи'
            elif '/syrniki-zapekanki-i-rikotniki/' in url_lower:
                if any(word in name_lower for word in ['сырники', 'запеканк']):
                    return 'Сырники и запеканки'
                return 'Запеканки'
            elif '/omlety-i-zavtraki-s-yaytsom/' in url_lower:
                return 'Омлеты и яичные блюда'
            elif '/kashi/' in url_lower:
                return 'Каши'
            return 'Завтраки'
        elif '/okroshki-i-letnie-supy/' in url_lower:
            return 'Окрошки и летние супы'
        elif '/zakuski/' in url_lower:
            return 'Закуски'
        elif '/rolly-i-sety/' in url_lower:
            return 'Роллы и сеты'
        elif '/onigiri/' in url_lower:
            return 'Онигири'
        elif '/pirogi-pirozhki-i-lepyeshki/' in url_lower:
            if any(word in name_lower for word in ['пирог', 'пирожок', 'лепешка']):
                return 'Пироги и лепешки'
            return 'Пироги'
        elif '/privezem-goryachim/' in url_lower:
            if '/goryachie-napitki/' in url_lower:
                return 'Горячие напитки'
            return 'Горячие блюда'
        elif '/tarelka-zdorovogo-pitaniya/' in url_lower:
            return 'Здоровое питание'
        elif '/veganskie-i-postnye-blyuda/' in url_lower:
            return 'Веганские и постные блюда'
        elif '/semeynyy-format/' in url_lower:
            return 'Семейный формат'
        elif '/kombo-na-kazhdyy-den/' in url_lower:
            return 'Комбо'
        elif '/kukhni-mira/' in url_lower:
            if '/aziatskaya-kukhnya/' in url_lower:
                return 'Азиатская кухня'
            elif '/russkaya-kukhnya/' in url_lower:
                return 'Русская кухня'
            elif '/kukhnya-kavkaza/' in url_lower:
                return 'Кавказская кухня'
            elif '/sredizemnomorskaya-kukhnya/' in url_lower:
                return 'Средиземноморская кухня'
            return 'Кухни мира'
        elif '/bliny-i-oladi/' in url_lower:
            return 'Блины и оладьи'
        elif '/khalyal/' in url_lower:
            return 'Халяль'
        elif '/bolshe-belka-menshe-kaloriy/' in url_lower:
            if '/malo-kaloriy/' in url_lower:
                return 'Низкокалорийные блюда'
            elif '/bolshe-belka/' in url_lower:
                return 'Высокобелковые блюда'
            return 'Диетические блюда'
        
        # Определяем по названию товара
        if any(word in name_lower for word in ['салат', 'цезарь', 'винегрет', 'мимоза', 'оливье']):
            return 'Салаты'
        elif any(word in name_lower for word in ['суп', 'борщ', 'щи', 'харчо', 'солянка', 'окрошка']):
            return 'Супы'
        elif any(word in name_lower for word in ['сэндвич', 'сендвич', 'бургер', 'шаурма']):
            return 'Сэндвичи и бургеры'
        elif any(word in name_lower for word in ['пицц']):
            return 'Пицца'
        elif any(word in name_lower for word in ['паста', 'макарон', 'спагетти', 'фетучини', 'лазань']):
            return 'Паста'
        elif any(word in name_lower for word in ['блины', 'оладьи', 'олади']):
            return 'Блины и оладьи'
        elif any(word in name_lower for word in ['сырники']):
            return 'Сырники'
        elif any(word in name_lower for word in ['запеканк']):
            return 'Запеканки'
        elif any(word in name_lower for word in ['омлет', 'яичн']):
            return 'Омлеты и яичные блюда'
        elif any(word in name_lower for word in ['каша', 'овсянк', 'гречн']):
            return 'Каши'
        elif any(word in name_lower for word in ['котлет', 'биточк', 'тефтел', 'фрикадел']):
            return 'Котлеты и фрикадельки'
        elif any(word in name_lower for word in ['ролл', 'суши']):
            return 'Роллы и суши'
        elif any(word in name_lower for word in ['пирог', 'пирожок', 'лепешка']):
            return 'Пироги и лепешки'
        elif any(word in name_lower for word in ['завтрак']):
            return 'Завтраки'
        elif any(word in name_lower for word in ['обед', 'ужин']):
            return 'Основные блюда'
        
        # По умолчанию
        return 'Готовая еда'


async def get_location_from_address(address: str) -> tuple:
    """Получение координат из адреса."""
    try:
        location_service = LocationService()
        result = await location_service.geocode_address(address)
        if result:
            lat, lon = result
            # Извлекаем город из адреса
            city = address.split(',')[0].strip() if ',' in address else "Москва"
            return city, f"{lat},{lon}"
        else:
            print(f"❌ Не удалось определить координаты для адреса: {address}")
            return None, None
    except Exception as e:
        print(f"❌ Ошибка геокодирования: {e}")
        return None, None


async def main():
    """Главная функция быстрого парсера."""
    # Интерактивный режим если нет аргументов
    if len(sys.argv) < 2:
        print("⚡ БЫСТРЫЙ ПАРСЕР ВКУСВИЛЛА")
        print("=" * 40)
        print("🌍 Введите адрес для парсинга:")
        print("   Примеры:")
        print("   • Москва, Красная площадь, 1")
        print("   • Санкт-Петербург, Невский проспект, 10")  
        print("   • 55.7558,37.6176 (координаты)")
        print()
        
        try:
            address = input("Адрес: ").strip()
            if not address:
                print("❌ Адрес не указан")
                return
                
            limit_input = input("Количество товаров (по умолчанию 100): ").strip()
            limit = int(limit_input) if limit_input.isdigit() else 100
            
        except (KeyboardInterrupt, EOFError):
            print("\n❌ Отменено пользователем")
            return
    else:
        address = sys.argv[1]
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    
    print()
    print("⚡ БЫСТРЫЙ ПАРСЕР ВКУСВИЛЛА")
    print("=" * 40)
    print(f"🎯 Цель: {limit} товаров")
    print(f"📍 Адрес: {address}")
    print("⚡ Режим: Быстрый (только каталог)")
    print()
    
    # Определение координат
    if ',' in address and len(address.split(',')) == 2:
        # Координаты переданы напрямую
        try:
            lat, lon = address.split(',')
            float(lat.strip())
            float(lon.strip())
            city = "Москва"  # По умолчанию
            coords = address
        except ValueError:
            city, coords = await get_location_from_address(address)
            if not coords:
                return
    else:
        # Геокодирование адреса
        city, coords = await get_location_from_address(address)
        if not coords:
            return
    
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
    
    antibot_client = AntiBotClient(concurrency=20, timeout=30)  # Быстрые настройки
    
    try:
        parser = VkusvillFastParser(antibot_client)
        
        # Загрузка базы тяжелого парсера
        parser.load_heavy_data()
        
        start_time = time.time()
        products = await parser.scrape_fast(city, coords, address, limit)
        end_time = time.time()
        
        if not products:
            print("❌ Быстрый парсинг не дал результатов")
            return
        
        # Статистика БЖУ
        with_bju = sum(1 for p in products if any(p.get(f) for f in ['kcal_100g', 'protein_100g', 'fat_100g', 'carb_100g']))
        from_heavy_db = sum(1 for p in products if p['id'] in parser.heavy_data)
        
        # Сохранение результатов
        timestamp = int(time.time())
        csv_file = f"data/address_fast_{timestamp}.csv"
        jsonl_file = f"data/address_fast_{timestamp}.jsonl"
        
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
        
        # Итоговая статистика
        duration = end_time - start_time
        print()
        print("⚡ БЫСТРЫЙ ПАРСИНГ ЗАВЕРШЕН")
        print("=" * 40)
        print(f"📊 Результаты:")
        print(f"   • Всего товаров: {len(products)}")
        print(f"   • С БЖУ данными: {with_bju} ({with_bju/len(products)*100:.1f}%)")
        print(f"   • Из базы тяжелого парсера: {from_heavy_db}")
        print(f"⏱️  Время выполнения: {duration:.1f} секунд")
        print(f"💾 Файлы сохранены:")
        print(f"   • CSV: {csv_file}")
        print(f"   • JSONL: {jsonl_file}")
                
    except KeyboardInterrupt:
        print("\n⚠️ Парсинг прерван пользователем")
    except Exception as e:
        print(f"❌ Ошибка быстрого парсинга: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await antibot_client.close()


if __name__ == "__main__":
    asyncio.run(main())

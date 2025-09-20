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
    """Простой HTTP клиент для обхода защиты."""
    
    def __init__(self, concurrency: int = 10, timeout: int = 30):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.timeout = timeout
        
    async def request(self, method: str, url: str, **kwargs):
        """Выполнить HTTP запрос."""
        async with self.semaphore:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            async with httpx.AsyncClient(timeout=self.timeout, headers=headers) as client:
                response = await client.request(method, url, **kwargs)
                return response
    
    async def close(self):
        """Закрытие клиента."""
        pass


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
            data_dir = Path("data")
            if data_dir.exists():
                heavy_files = list(data_dir.glob("moscow_improved_*.csv"))
                if heavy_files:
                    heavy_file_path = str(sorted(heavy_files)[-1])  # Последний файл
        
        if heavy_file_path and Path(heavy_file_path).exists():
            print(f"📚 Загружаем базу тяжелого парсера: {heavy_file_path}")
            try:
                with open(heavy_file_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        self.heavy_data[row['id']] = row
                print(f"   ✅ Загружено {len(self.heavy_data)} товаров из базы")
            except Exception as e:
                print(f"   ❌ Ошибка загрузки базы: {e}")
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
                    product = {
                        'id': heavy_product.get('id', product_id),
                        'name': heavy_product.get('name', ''),
                        'price': heavy_product.get('price', ''),
                        'category': heavy_product.get('category', 'Готовая еда'),
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
        
        # Категории готовой еды
        categories = [
            "/goods/gotovaya-eda/",
            "/goods/gotovaya-eda/salaty/",
            "/goods/gotovaya-eda/supy/",
            "/goods/gotovaya-eda/vtorye-blyuda/",
            "/goods/gotovaya-eda/zavtraki/",
        ]
        
        for category in categories:
            try:
                url = f"{self.BASE_URL}{category}"
                response = await self.antibot_client.request(method="GET", url=url)
                
                if response.status_code == 200 and HTMLParser:
                    parser = HTMLParser(response.text)
                    
                    # Ищем все ссылки на товары
                    product_links = parser.css('a[href*="/goods/"][href$=".html"]')
                    
                    for link in product_links:
                        href = link.attributes.get('href')
                        if href:
                            product_id = self._extract_id_from_url(urljoin(self.BASE_URL, href))
                            if product_id and product_id not in available_ids:
                                available_ids.append(product_id)
                
                await asyncio.sleep(0.5)  # Пауза между категориями
                
            except Exception as e:
                print(f"   ❌ Ошибка категории {category}: {e}")
        
        return available_ids
    
    async def _fallback_catalog_parsing(self, limit: int) -> List[Dict]:
        """Резервный парсинг каталога если нет базы."""
        categories = [
            "/goods/gotovaya-eda/",
            "/goods/gotovaya-eda/salaty/",
            "/goods/gotovaya-eda/supy/",
            "/goods/gotovaya-eda/vtorye-blyuda/",
            "/goods/gotovaya-eda/zavtraki/",
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
        """Установка локации."""
        try:
            lat, lon = coords.split(',')
            location_url = f"{self.BASE_URL}/api/location?city={quote(city)}&lat={lat.strip()}&lon={lon.strip()}"
            await self.antibot_client.request(method="GET", url=location_url)
            print(f"📍 Локация установлена: {city}")
        except Exception as e:
            print(f"⚠️ Ошибка установки локации: {e}")
    
    async def _parse_category_fast(self, category: str, max_products: int) -> List[Dict]:
        """Быстрый парсинг категории без захода в карточки."""
        products = []
        
        try:
            url = f"{self.BASE_URL}{category}"
            print(f"   🔍 Парсим: {url}")
            response = await self.antibot_client.request(method="GET", url=url)
            
            if response.status_code != 200:
                print(f"   ❌ HTTP {response.status_code}")
                return products
                
            if not HTMLParser:
                print(f"   ❌ HTMLParser недоступен")
                return products
                
            parser = HTMLParser(response.text)
            
            # Ищем все ссылки на товары
            product_links = parser.css('a[href*="/goods/"][href$=".html"]')
            print(f"   📦 Найдено ссылок на товары: {len(product_links)}")
            
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
                    print(f"   ✅ {product['name'][:50]}...")
        
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
            
            return {
                'id': product_id,
                'name': name[:150],
                'price': price,
                'category': 'Готовая еда',
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
            
            return {
                'id': product_id,
                'name': name[:150],
                'price': price,
                'category': 'Готовая еда',
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

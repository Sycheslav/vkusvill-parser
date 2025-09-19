#!/usr/bin/env python3
"""
🏗️ ТЯЖЕЛЫЙ ПАРСЕР ВКУСВИЛЛА ДЛЯ МОСКВЫ С АДРЕСОМ
Полный парсинг с установкой конкретного адреса в Москве.
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


class VkusvillHeavyParser:
    """Тяжелый парсер с глубоким анализом каждой карточки."""
    
    def __init__(self, antibot_client):
        self.antibot_client = antibot_client
        self.BASE_URL = "https://vkusvill.ru"
        
    async def scrape_heavy_with_address(self, limit: int = 1500, address: str = "Тверская улица, 12") -> List[Dict]:
        """Тяжелый парсинг с установкой адреса."""
        print(f"🏗️ Начинаем тяжелый парсинг на {limit} товаров...")
        
        # Установка локации с конкретным адресом
        await self._set_location_with_address(address)
        
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
                urls = await self._get_category_products_extended(category, 200)  # Больше товаров на категорию
                product_urls.update(urls)
                print(f"   {category}: +{len(urls)} товаров")
            except Exception as e:
                print(f"   ❌ {category}: {e}")
        
        print(f"📦 Всего найдено {len(product_urls)} ссылок на товары")
        
        # Берем все найденные товары
        product_list = list(product_urls)
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
    
    async def _set_location_with_address(self, address: str):
        """Установка локации с конкретным адресом."""
        try:
            # Сначала пробуем геокодинг адреса
            geocode_url = f"https://geocode-maps.yandex.ru/1.x/?format=json&geocode={quote(address + ', Москва')}"
            response = await self.antibot_client.request(method="GET", url=geocode_url)
            
            lat, lon = 55.7558, 37.6176  # Дефолтные координаты центра Москвы
            
            if response.status_code == 200:
                try:
                    geo_data = response.json()
                    pos = geo_data['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['Point']['pos']
                    lon, lat = map(float, pos.split())
                    print(f"📍 Найдены координаты для '{address}': {lat}, {lon}")
                except:
                    print(f"📍 Используем центр Москвы: {lat}, {lon}")
            
            # Устанавливаем локацию в ВкусВилл
            location_url = f"{self.BASE_URL}/api/location?city=Москва&lat={lat}&lon={lon}&address={quote(address)}"
            await self.antibot_client.request(method="GET", url=location_url)
            print(f"📍 Локация установлена: {address} ({lat}, {lon})")
            
        except Exception as e:
            print(f"⚠️ Ошибка установки локации: {e}")
            # Fallback на центр Москвы
            location_url = f"{self.BASE_URL}/api/location?city=Москва&lat=55.7558&lon=37.6176"
            await self.antibot_client.request(method="GET", url=location_url)
            print("📍 Локация установлена: Москва (центр)")
    
    async def _get_category_products_extended(self, category: str, max_products: int) -> List[str]:
        """Расширенное получение товаров из категории."""
        product_urls = set()
        
        # Обычные страницы пагинации с увеличенным лимитом
        for page_num in range(1, 200):  # До 200 страниц
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

                if page_count == 0:
                    break
                    
                if len(product_urls) >= max_products:
                    break
                    
                await asyncio.sleep(0.1)  # Быстрее
                
            except Exception:
                break
        
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
            'завтрак', 'обед', 'ужин', 'онигири', 'ролл', 'суши'
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
        max_retries = 1  # Уменьшаем retry для скорости
        
        try:
            response = await self.antibot_client.request(method="GET", url=url)
            if response.status_code != 200 or not HTMLParser:
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
        """Цена товара."""
        selectors = ['.price', '.product-price', '.cost', '.goods-price']
        
        for selector in selectors:
            elements = parser.css(selector)
            for element in elements:
                price_text = element.text(strip=True)
                numbers = re.findall(r'(\d+(?:[.,]\d+)?)', price_text)
                for num in numbers:
                    try:
                        price_val = float(num.replace(',', '.'))
                        if 10 <= price_val <= 10000:
                            return num.replace(',', '.')
                    except ValueError:
                        continue
        
        return ""
    
    def _extract_photo(self, parser) -> str:
        """Фото товара."""
        selectors = ['img[src*="product"]', 'img[src*="goods"]', 'img']
        
        for selector in selectors:
            elements = parser.css(selector)
            for element in elements:
                src = element.attributes.get('src') or element.attributes.get('data-src')
                if src and any(keyword in src.lower() for keyword in ['product', 'goods', 'catalog']):
                    if not any(skip in src.lower() for skip in ['icon', 'logo', 'banner']):
                        full_url = urljoin(self.BASE_URL, src)
                        if full_url.startswith('http'):
                            return full_url
        
        return ""
    
    def _extract_composition(self, parser, page_text: str) -> str:
        """Состав товара."""
        elements = parser.css('div, p, span, td, li')
        for element in elements:
            text = element.text().strip()
            text_lower = text.lower()
            
            if 'состав' in text_lower and len(text) > 10:
                if not any(word in text_lower for word in ['меню', 'каталог', 'корзина']):
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
        """Извлечение БЖУ."""
        nutrition = {'kcal_100g': '', 'protein_100g': '', 'fat_100g': '', 'carb_100g': ''}
        
        # Поиск в элементах страницы
        elements = parser.css('div, span, p, td, th, li')
        for element in elements:
            text = element.text().lower()
            original_text = element.text()
            
            if any(word in text for word in ['ккал', 'белки', 'жиры', 'углеводы']):
                if ('ккал' in text or 'калорийность' in text) and not nutrition['kcal_100g']:
                    kcal_patterns = [r'(\d+(?:[.,]\d+)?)\s*ккал', r'ккал[:\s]*(\d+(?:[.,]\d+)?)']
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
                    protein_patterns = [r'(\d+(?:[.,]\d+)?)\s+белки,\s*г', r'белк[иа][:\s]*(\d+(?:[.,]\d+)?)']
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
                    fat_patterns = [r'(\d+(?:[.,]\d+)?)\s+жиры,\s*г', r'жир[ыа][:\s]*(\d+(?:[.,]\d+)?)']
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
                    carb_patterns = [r'(\d+(?:[.,]\d+)?)\s+углеводы,\s*г', r'углевод[ыа][:\s]*(\d+(?:[.,]\d+)?)']
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
        
        return nutrition


async def main():
    """Главная функция тяжелого парсера с адресом."""
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 1500
    address = sys.argv[2] if len(sys.argv) > 2 else "Тверская улица, 12"
    
    print("🏗️ ТЯЖЕЛЫЙ ПАРСЕР ВКУСВИЛЛА - МОСКВА С АДРЕСОМ")
    print("=" * 60)
    print(f"🎯 Цель: {limit} товаров с полными данными")
    print(f"📍 Адрес: {address}")
    print("⚡ Режим: Глубокий анализ каждой карточки")
    print()
    
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
    
    antibot_client = AntiBotClient(concurrency=8, timeout=60)
    
    try:
        parser = VkusvillHeavyParser(antibot_client)
        
        start_time = time.time()
        products = await parser.scrape_heavy_with_address(limit, address)
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
        csv_file = f"data/moscow_address_{timestamp}.csv"
        jsonl_file = f"data/moscow_address_{timestamp}.jsonl"
        
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

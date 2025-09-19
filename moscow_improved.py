#!/usr/bin/env python3
"""
🏗️ УЛУЧШЕННЫЙ ПАРСЕР ВКУСВИЛЛА ДЛЯ МОСКВЫ
Правильная работа с пагинацией для получения всех товаров.
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


class VkusvillImprovedParser:
    """Улучшенный парсер с правильной пагинацией."""
    
    def __init__(self, antibot_client):
        self.antibot_client = antibot_client
        self.BASE_URL = "https://vkusvill.ru"
        
    async def scrape_all_products(self, limit: int = 1500) -> List[Dict]:
        """Парсинг всех товаров с правильной пагинацией."""
        print(f"🏗️ Начинаем улучшенный парсинг на {limit} товаров...")
        
        # Установка локации для Москвы
        await self._set_location()
        
        # Сбор товаров из всех категорий с правильной пагинацией
        print("📋 Собираем ВСЕ товары готовой еды с правильной пагинацией...")
        product_urls = set()
        
        ready_food_categories = [
            "/goods/gotovaya-eda/",
            "/goods/gotovaya-eda/novinki/",
            "/goods/gotovaya-eda/vtorye-blyuda/",
            "/goods/gotovaya-eda/salaty/",
            "/goods/gotovaya-eda/sendvichi-shaurma-i-burgery/",
            "/goods/gotovaya-eda/supy/",
            "/goods/gotovaya-eda/zavtraki/",
            "/goods/gotovaya-eda/zakuski/",
            "/goods/gotovaya-eda/rolly-i-sety/",
            "/goods/gotovaya-eda/onigiri/",
            "/goods/gotovaya-eda/pirogi-pirozhki-i-lepyeshki/"
        ]
        
        for category in ready_food_categories:
            try:
                urls = await self._get_all_category_products(category)
                product_urls.update(urls)
                print(f"   {category}: +{len(urls)} товаров")
            except Exception as e:
                print(f"   ❌ {category}: {e}")
        
        print(f"📦 Всего найдено {len(product_urls)} ссылок на товары")
        
        # Парсим все найденные товары
        products = []
        product_list = list(product_urls)
        
        batch_size = 10
        semaphore = asyncio.Semaphore(8)
        
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
                        
                        if len(products) >= limit:
                            print(f"🎯 Достигнут лимит {limit} товаров")
                            return products
            
            await asyncio.sleep(0.5)
        
        print(f"🏁 Парсинг завершен: {len(products)} товаров")
        return products
    
    async def _set_location(self):
        """Установка локации для Москвы."""
        try:
            location_url = f"{self.BASE_URL}/api/location?city=Москва&lat=55.7558&lon=37.6176"
            await self.antibot_client.request(method="GET", url=location_url)
            print("📍 Локация установлена: Москва (центр)")
        except Exception as e:
            print(f"⚠️ Ошибка установки локации: {e}")
    
    async def _get_all_category_products(self, category: str) -> List[str]:
        """Получить ВСЕ товары из категории через все возможные методы."""
        product_urls = set()
        
        # Метод 1: Обычная пагинация с увеличенным лимитом страниц
        print(f"      🔍 Пагинация для {category}")
        page = 1
        consecutive_empty = 0
        
        while page <= 100 and consecutive_empty < 5:  # До 100 страниц, но останавливаемся после 5 пустых подряд
            try:
                # Пробуем разные варианты URL пагинации
                urls_to_try = [
                    f"{self.BASE_URL}{category}?page={page}",
                    f"{self.BASE_URL}{category}?PAGEN_1={page}",
                    f"{self.BASE_URL}{category}?p={page}",
                    f"{self.BASE_URL}{category}?offset={24*(page-1)}&limit=24",
                    f"{self.BASE_URL}{category}?start={24*(page-1)}&count=24"
                ]
                
                found_products = False
                
                for url in urls_to_try:
                    try:
                        response = await self.antibot_client.request(method="GET", url=url)
                        
                        if response.status_code != 200:
                            continue
                            
                        parser = HTMLParser(response.text)
                        links = parser.css('a[href*="/goods/"][href$=".html"]')
                        
                        if links:
                            page_count = 0
                            for link in links:
                                href = link.attributes.get('href')
                                if href and '.html' in href and '/goods/' in href:
                                    full_url = urljoin(self.BASE_URL, href)
                                    if full_url not in product_urls:
                                        product_urls.add(full_url)
                                        page_count += 1
                            
                            if page_count > 0:
                                print(f"         ✅ Страница {page}: +{page_count} товаров (всего: {len(product_urls)})")
                                found_products = True
                                consecutive_empty = 0
                                break
                    
                    except Exception:
                        continue
                
                if not found_products:
                    consecutive_empty += 1
                    print(f"         ❌ Страница {page}: пустая ({consecutive_empty}/5)")
                
                page += 1
                await asyncio.sleep(0.1)
                
            except Exception:
                consecutive_empty += 1
                page += 1
        
        # Метод 2: Попытка AJAX загрузки
        if len(product_urls) < 200:  # Если мало товаров, пробуем AJAX
            print(f"      🔄 AJAX загрузка для {category}")
            await self._try_ajax_load(category, product_urls)
        
        # Метод 3: Поиск через различные параметры сортировки
        if len(product_urls) < 200:
            print(f"      🔄 Поиск через сортировки для {category}")
            sort_params = ['price', 'name', 'popular', 'new', 'rating']
            for sort_param in sort_params:
                try:
                    url = f"{self.BASE_URL}{category}?sort={sort_param}"
                    response = await self.antibot_client.request(method="GET", url=url)
                    
                    if response.status_code == 200:
                        parser = HTMLParser(response.text)
                        links = parser.css('a[href*="/goods/"][href$=".html"]')
                        
                        sort_count = 0
                        for link in links:
                            href = link.attributes.get('href')
                            if href and '.html' in href and '/goods/' in href:
                                full_url = urljoin(self.BASE_URL, href)
                                if full_url not in product_urls:
                                    product_urls.add(full_url)
                                    sort_count += 1
                        
                        if sort_count > 0:
                            print(f"         ✅ Сортировка {sort_param}: +{sort_count} товаров")
                        
                        await asyncio.sleep(0.2)
                        
                except Exception:
                    continue
        
        return list(product_urls)
    
    async def _try_ajax_load(self, category: str, product_urls: set):
        """Попытка AJAX загрузки дополнительных товаров."""
        try:
            # Получаем базовую страницу для анализа
            base_url = f"{self.BASE_URL}{category}"
            response = await self.antibot_client.request(method="GET", url=base_url)
            
            if response.status_code != 200:
                return
            
            # Ищем AJAX endpoints в JavaScript коде
            ajax_patterns = [
                r'url\s*:\s*["\']([^"\']*load[^"\']*)["\']',
                r'["\']([^"\']*ajax[^"\']*load[^"\']*)["\']',
                r'["\']([^"\']*catalog[^"\']*load[^"\']*)["\']'
            ]
            
            ajax_urls = set()
            for pattern in ajax_patterns:
                matches = re.findall(pattern, response.text, re.I)
                for match in matches:
                    if match.startswith('/'):
                        ajax_urls.add(self.BASE_URL + match)
                    elif match.startswith('http'):
                        ajax_urls.add(match)
            
            # Пробуем стандартные AJAX endpoints
            standard_endpoints = [
                f"{self.BASE_URL}/ajax/catalog/",
                f"{self.BASE_URL}/ajax/goods/",
                f"{self.BASE_URL}/local/ajax/catalog.php",
                f"{self.BASE_URL}/bitrix/components/custom/catalog.section/ajax.php"
            ]
            ajax_urls.update(standard_endpoints)
            
            # Пробуем каждый endpoint
            for ajax_url in list(ajax_urls)[:5]:  # Ограничиваем количество попыток
                try:
                    # POST запрос с данными категории
                    data = {
                        'PAGEN_1': 2,
                        'SECTION_CODE': category.strip('/').split('/')[-1],
                        'action': 'load_more',
                        'page': 2
                    }
                    
                    response = await self.antibot_client.request(
                        method="POST",
                        url=ajax_url,
                        data=data,
                        headers={
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'X-Requested-With': 'XMLHttpRequest',
                            'Referer': base_url
                        }
                    )
                    
                    if response.status_code == 200 and len(response.text) > 100:
                        # Пробуем найти товары в ответе
                        if 'goods/' in response.text and '.html' in response.text:
                            parser = HTMLParser(response.text)
                            links = parser.css('a[href*="/goods/"][href$=".html"]')
                            
                            ajax_count = 0
                            for link in links:
                                href = link.attributes.get('href')
                                if href and '.html' in href and '/goods/' in href:
                                    full_url = urljoin(self.BASE_URL, href)
                                    if full_url not in product_urls:
                                        product_urls.add(full_url)
                                        ajax_count += 1
                            
                            if ajax_count > 0:
                                print(f"         ✅ AJAX {ajax_url}: +{ajax_count} товаров")
                                return  # Если нашли рабочий endpoint, используем его
                
                except Exception:
                    continue
                    
        except Exception:
            pass
    
    def _is_ready_food(self, product: Dict) -> bool:
        """Проверяем что это товар готовой еды."""
        name = product.get('name', '').lower()
        url = product.get('url', '').lower()
        
        # Если URL содержит готовую еду - точно подходит
        if 'gotovaya-eda' in url:
            return True
        
        # Ключевые слова готовой еды
        ready_food_keywords = [
            'суп', 'салат', 'борщ', 'омлет', 'блины', 'каша', 'пицца',
            'паста', 'котлета', 'запеканка', 'сырники', 'плов', 'лазанья',
            'шаурма', 'бургер', 'сэндвич', 'онигири', 'ролл', 'суши'
        ]
        
        return any(keyword in name for keyword in ready_food_keywords)
    
    async def _extract_full_product(self, url: str) -> Optional[Dict]:
        """Полное извлечение товара."""
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
                'shop': 'vkusvill_improved',
                'photo': self._extract_photo(parser),
                'composition': self._extract_composition(parser, page_text),
                'tags': '',
                'portion_g': self._extract_portion_weight(parser, page_text)
            }
            
            # БЖУ
            nutrition = self._extract_bju(parser, page_text)
            product.update(nutrition)
            
            # Краткие логи
            filled_bju = sum(1 for field in ['kcal_100g', 'protein_100g', 'fat_100g', 'carb_100g'] if product.get(field))
            has_composition = bool(product.get('composition'))
            
            print(f"      📦 {product['name'][:40]}... БЖУ:{filled_bju}/4 Состав:{'✓' if has_composition else '✗'} Цена:{product['price'] or '?'}")
            
            if not product['name']:
                return None
            
            return product
            
        except Exception:
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
                try:
                    weight = float(match.group(1).replace(',', '.'))
                    if 10 <= weight <= 2000:
                        return f"{weight}г"
                except ValueError:
                    continue
        return ""
    
    def _extract_bju(self, parser, page_text: str) -> Dict[str, str]:
        """Извлечение БЖУ."""
        nutrition = {'kcal_100g': '', 'protein_100g': '', 'fat_100g': '', 'carb_100g': ''}
        
        # Поиск в элементах страницы
        elements = parser.css('div, span, p, td, th, li')
        for element in elements:
            text = element.text().lower()
            
            if any(word in text for word in ['ккал', 'белки', 'жиры', 'углеводы']):
                if ('ккал' in text or 'калорийность' in text) and not nutrition['kcal_100g']:
                    match = re.search(r'(\d+(?:[.,]\d+)?)\s*ккал', text)
                    if match:
                        try:
                            val = float(match.group(1).replace(',', '.'))
                            if 10 <= val <= 900:
                                nutrition['kcal_100g'] = match.group(1).replace(',', '.')
                        except ValueError:
                            pass
                
                if 'белк' in text and not nutrition['protein_100g']:
                    match = re.search(r'(\d+(?:[.,]\d+)?)\s+белки,\s*г', text)
                    if match:
                        try:
                            val = float(match.group(1).replace(',', '.'))
                            if 0 <= val <= 100:
                                nutrition['protein_100g'] = match.group(1).replace(',', '.')
                        except ValueError:
                            pass
                
                if 'жир' in text and not nutrition['fat_100g']:
                    match = re.search(r'(\d+(?:[.,]\d+)?)\s+жиры,\s*г', text)
                    if match:
                        try:
                            val = float(match.group(1).replace(',', '.'))
                            if 0 <= val <= 100:
                                nutrition['fat_100g'] = match.group(1).replace(',', '.')
                        except ValueError:
                            pass
                
                if 'углевод' in text and not nutrition['carb_100g']:
                    match = re.search(r'(\d+(?:[.,]\d+)?)\s+углеводы,\s*г', text)
                    if match:
                        try:
                            val = float(match.group(1).replace(',', '.'))
                            if 0 <= val <= 100:
                                nutrition['carb_100g'] = match.group(1).replace(',', '.')
                        except ValueError:
                            pass
        
        return nutrition


async def main():
    """Главная функция улучшенного парсера."""
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 1500
    
    print("🏗️ УЛУЧШЕННЫЙ ПАРСЕР ВКУСВИЛЛА - МОСКВА")
    print("=" * 60)
    print(f"🎯 Цель: {limit} товаров с полными данными")
    print("📍 Местоположение: Москва")
    print("⚡ Режим: Правильная пагинация")
    print()
    
    logging.basicConfig(level=logging.WARNING)
    
    antibot_client = AntiBotClient(concurrency=10, timeout=60)
    
    try:
        parser = VkusvillImprovedParser(antibot_client)
        
        start_time = time.time()
        products = await parser.scrape_all_products(limit)
        end_time = time.time()
        
        if not products:
            print("❌ Парсинг не дал результатов")
            return
        
        # Статистика
        bju_stats = {'full_bju': 0, 'good_bju': 0, 'some_bju': 0, 'no_bju': 0}
        composition_stats = {'has_composition': 0, 'no_composition': 0}
        
        for product in products:
            bju_fields = ['kcal_100g', 'protein_100g', 'fat_100g', 'carb_100g']
            filled = sum(1 for field in bju_fields if product.get(field))
            has_composition = bool(product.get('composition'))
            
            if filled == 4:
                bju_stats['full_bju'] += 1
            elif filled == 3:
                bju_stats['good_bju'] += 1
            elif filled >= 1:
                bju_stats['some_bju'] += 1
            else:
                bju_stats['no_bju'] += 1
            
            if has_composition:
                composition_stats['has_composition'] += 1
            else:
                composition_stats['no_composition'] += 1
        
        # Сохранение
        timestamp = int(time.time())
        csv_file = f"data/moscow_improved_{timestamp}.csv"
        jsonl_file = f"data/moscow_improved_{timestamp}.jsonl"
        
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
        excellent_quality = bju_stats['full_bju'] + bju_stats['good_bju']
        
        print()
        print("🏁 УЛУЧШЕННЫЙ ПАРСИНГ ЗАВЕРШЕН")
        print("=" * 60)
        print(f"📊 РЕЗУЛЬТАТЫ:")
        print(f"   • Всего товаров: {len(products)}")
        print(f"   • Полное БЖУ (4/4): {bju_stats['full_bju']} ({bju_stats['full_bju']/len(products)*100:.1f}%)")
        print(f"   • Хорошее БЖУ (3/4): {bju_stats['good_bju']} ({bju_stats['good_bju']/len(products)*100:.1f}%)")
        print(f"   • Качественных товаров: {excellent_quality} ({excellent_quality/len(products)*100:.1f}%)")
        print(f"   • Есть состав: {composition_stats['has_composition']} ({composition_stats['has_composition']/len(products)*100:.1f}%)")
        print(f"⏱️  Время: {duration/60:.1f} минут")
        print(f"💾 Файлы: {csv_file}, {jsonl_file}")
        
        # Проверяем достигли ли цели
        if len(products) >= 1200 and excellent_quality/len(products) >= 0.95:
            print("🎉 ЦЕЛЬ ДОСТИГНУТА: >1200 товаров и >95% качественных!")
        else:
            print(f"⚠️  Цель не достигнута: нужно >1200 товаров (есть {len(products)}) и >95% качественных (есть {excellent_quality/len(products)*100:.1f}%)")
            
    except KeyboardInterrupt:
        print("\n⚠️ Парсинг прерван")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await antibot_client.close()


if __name__ == "__main__":
    asyncio.run(main())

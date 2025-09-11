"""
Парсер ВкусВилл для сбора готовой еды.
Основной источник для достижения цели 500+ позиций за проход.
"""

import asyncio
import logging
import re
from typing import AsyncIterator, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import httpx
from selectolax.parser import HTMLParser


class VkusvillScraper:
    """Скрейпер готовой еды с ВкусВилл."""
    
    BASE_URL = "https://vkusvill.ru"
    READY_FOOD_CATEGORIES = [
        # Основные категории готовой еды
        "/goods/gotovaya-eda/",  # Готовая еда (общий раздел)
        "/goods/gotovaya-eda/supy/",  # Супы
        "/goods/gotovaya-eda/salaty/",  # Салаты
        "/goods/gotovaya-eda/zavtraki/",  # Завтраки
        "/goods/gotovaya-eda/zakuski/",  # Закуски
        "/goods/gotovaya-eda/vtorye-blyuda/",  # Вторые блюда
        "/goods/gotovaya-eda/pirogi-pirozhki-i-lepyeshki/",  # Пироги, пирожки и лепёшки
        "/goods/gotovaya-eda/semeynyy-format/",  # Семейный формат (большие порции)
        
        # Дополнительные подкатегории готовой еды
        "/goods/gotovaya-eda/bliny-i-oladi/",  # Блины и оладьи
        "/goods/gotovaya-eda/bliny-s-kartoshkoy/",  # Блины с картошкой
        "/goods/gotovaya-eda/omlety-i-zavtraki-s-yaytsom/",  # Омлеты и завтраки с яйцом
        "/goods/gotovaya-eda/syrniki-zapekanki-i-rikotniki/",  # Сырники, запеканки и рикотники
        "/goods/gotovaya-eda/veganskie-i-postnye-blyuda/",  # Веганские и постные блюда
        "/goods/gotovaya-eda/novinki/",  # Новинки
        "/goods/gotovaya-eda/skidki-/",  # Скидки
        "/goods/gotovaya-eda/kombo-na-kazhdyy-den/",  # Комбо на каждый день
        "/goods/gotovaya-eda/okroshki-i-letnie-supy/",  # Окрошки и летние супы
        "/goods/gotovaya-eda/privezem-goryachim/",  # Привезем горячим
        "/goods/gotovaya-eda/tarelka-zdorovogo-pitaniya/",  # Тарелка здорового питания
        "/goods/gotovaya-eda/bolshe-belka-menshe-kaloriy/",  # Больше белка, меньше калорий
        "/goods/gotovaya-eda/kukhni-mira/",  # Кухни мира
        "/goods/gotovaya-eda/rolly-i-sety/",  # Роллы и сеты
        "/goods/gotovaya-eda/sendvichi-shaurma-i-burgery/",  # Сэндвичи, шаурма и бургеры
        "/goods/gotovaya-eda/onigiri/",  # Онигири
        "/goods/gotovaya-eda/kapusta-po-koreyski/",  # Капуста по-корейски
        "/goods/gotovaya-eda/salat-chuka/",  # Салат чука
        "/goods/gotovaya-eda/khalyal/",  # Халял
        
        # Подкатегории вторых блюд
        "/goods/gotovaya-eda/vtorye-blyuda/vtorye-blyuda-s-myasom/",  # Вторые блюда с мясом
        "/goods/gotovaya-eda/vtorye-blyuda/vtorye-blyuda-s-ryboy-i-moreproduktami/",  # Вторые блюда с рыбой и морепродуктами
        "/goods/gotovaya-eda/vtorye-blyuda/vtorye-blyuda-s-ptitsey/",  # Вторые блюда с птицей
        "/goods/gotovaya-eda/vtorye-blyuda/garniry-i-vtorye-blyuda-bez-myasa/",  # Гарниры и вторые блюда без мяса
        "/goods/gotovaya-eda/vtorye-blyuda/pasta-pitstsa/",  # Паста, пицца
        
        # Подкатегории завтраков
        "/goods/gotovaya-eda/zavtraki/omlety-i-zavtraki-s-yaytsom/",  # Омлеты и завтраки с яйцом
        "/goods/gotovaya-eda/zavtraki/syrniki-zapekanki-i-rikotniki/",  # Сырники, запеканки и рикотники
        "/goods/gotovaya-eda/zavtraki/bliny-i-oladi/",  # Блины и оладьи
    ]
    
    def __init__(self, antibot_client):
        self.client = antibot_client
        self.session = None
    
    async def scrape(self, city: str = "Moscow", coords: str = "55.75,37.61", 
                    limit: int = -1, **kwargs) -> AsyncIterator[Dict]:
        """Основной метод скрейпинга."""
        logging.info(f"Starting Vkusvill scrape for {city}")
        
        try:
            # Получаем сессию для домена
            self.session = await self.client.get_client(self.BASE_URL)
            
            # ВкусВилл требует геопривязку для показа каталога
            # Попробуем установить город через API или cookies
            await self._set_location(city, coords)
            
            # Собираем все ссылки на товары из категорий готовой еды
            product_urls = set()
            
            for category_url in self.READY_FOOD_CATEGORIES:
                category_full_url = urljoin(self.BASE_URL, category_url)
                logging.info(f"Scraping category: {category_full_url}")
                
                try:
                    category_products = await self._scrape_category_products(category_full_url)
                    product_urls.update(category_products)
                    logging.info(f"Found {len(category_products)} products in {category_url}")
                    
                    # Пауза между категориями
                    await asyncio.sleep(1.0)
                    
                except Exception as e:
                    logging.error(f"Error scraping category {category_url}: {e}")
                    continue
            
            logging.info(f"Total unique products found: {len(product_urls)}")
            
            # Если товары не найдены, попробуем альтернативный подход
            if not product_urls:
                logging.warning("No products found with standard approach, trying alternative method")
                product_urls = await self._scrape_alternative_approach()
            
            # Собираем данные по каждому товару
            count = 0
            for product_url in product_urls:
                if limit > 0 and count >= limit:
                    break
                
                try:
                    product_data = await self._scrape_product(product_url)
                    if product_data:
                        yield product_data
                        count += 1
                        
                        if count % 50 == 0:
                            logging.info(f"Scraped {count} products")
                    
                    # Пауза между товарами
                    await asyncio.sleep(0.3)
                    
                except Exception as e:
                    logging.error(f"Error scraping product {product_url}: {e}")
                    continue
            
            logging.info(f"Vkusvill scrape completed. Total products: {count}")
            
        except Exception as e:
            logging.error(f"Vkusvill scrape failed: {e}")
            raise
    
    async def _scrape_category_products(self, category_url: str) -> List[str]:
        """Собрать все ссылки на товары из категории."""
        product_urls = []
        page = 1
        
        while True:
            # Формируем URL с пагинацией
            if page == 1:
                url = category_url
            else:
                url = f"{category_url}?page={page}"
            
            try:
                response = await self.client.request(method="GET", url=url)
                if response.status_code != 200:
                    logging.warning(f"Failed to load category page {url}: {response.status_code}")
                    break
                
                html = response.text
                # Парсим HTML с selectolax
                parser = HTMLParser(html)
                
                # Ищем ссылки на товары - обновленные селекторы для ВкусВилл
                product_selectors = [
                    'a[href*="/product/"]',
                    'a[href*="/goods/"]',
                    '.product-item a',
                    '.catalog-item a',
                    '.goods-item a',
                    '.product-card a',
                    '.item-card a'
                ]
                
                product_links = []
                for selector in product_selectors:
                    links = parser.css(selector)
                    product_links.extend(links)
                
                if not product_links:
                    logging.info(f"No more products found on page {page}")
                    break
                
                for link in product_links:
                    href = link.attributes.get('href')
                    if href and ('/product/' in href or '/goods/' in href):
                        full_url = urljoin(self.BASE_URL, href)
                        product_urls.append(full_url)
                
                # Проверяем наличие кнопки "Показать еще" или пагинации
                show_more = parser.css('.show-more, .pagination-next, .load-more')
                if not show_more and page > 1:
                    break
                
                page += 1
                
                # Пауза между страницами
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logging.error(f"Error scraping category page {url}: {e}")
                break
        
        return product_urls
    
    async def _scrape_product(self, product_url: str) -> Optional[Dict]:
        """Собрать данные одного товара."""
        try:
            response = await self.client.request(method="GET", url=product_url)
            if response.status_code != 200:
                logging.warning(f"Failed to load product {product_url}: {response.status_code}")
                return None
            
            html = response.text
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
            
            # Категория (хлебные крошки)
            category = self._extract_category(parser)
            
            # Цена
            price = self._extract_price(parser)
            
            # Вес/объем порции
            portion_g = self._extract_portion(parser)
            
            # БЖУ на 100г
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
                'shop': 'vkusvill',
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
        
        # Альтернативно - последний сегмент URL
        path_parts = urlparse(url).path.split('/')
        if path_parts:
            return path_parts[-1] or path_parts[-2]
        
        return None
    
    def _extract_name(self, parser: HTMLParser) -> Optional[str]:
        """Извлечь название товара."""
        # Обновленные селекторы для ВкусВилл
        name_selectors = [
            'h1.product-title',
            'h1[data-testid="product-title"]',
            '.product-info h1',
            '.product-header h1',
            '.product-name',
            '.goods-title',
            '.item-title',
            'h1.title',
            'h1'
        ]
        
        for selector in name_selectors:
            element = parser.css_first(selector)
            if element and element.text(strip=True):
                return element.text(strip=True)
        
        return None
    
    def _extract_category(self, parser: HTMLParser) -> str:
        """Извлечь категорию из хлебных крошек."""
        breadcrumb_selectors = [
            '.breadcrumbs a',
            '.breadcrumb a',
            '[data-testid="breadcrumb"] a',
            '.navigation-path a'
        ]
        
        categories = []
        for selector in breadcrumb_selectors:
            elements = parser.css(selector)
            for element in elements:
                text = element.text(strip=True)
                if text and text not in ['Главная', 'Каталог', 'ВкусВилл']:
                    categories.append(text)
        
        return ' > '.join(categories) if categories else 'Готовая еда'
    
    def _extract_price(self, parser: HTMLParser) -> Optional[float]:
        """Извлечь цену товара."""
        # Обновленные селекторы для ВкусВилл (2024)
        price_selectors = [
            # Основные селекторы цен
            '.price-current',
            '.product-price .current',
            '.price .value',
            '.product-price',
            '.goods-price',
            '.item-price',
            '.price-value',
            '.current-price',
            '.price',
            
            # Селекторы для карточек товаров
            '.product-card-price',
            '.catalog-item-price',
            '.goods-item-price',
            '.price-block .price',
            '.product-info .price',
            '.product-header .price',
            '.price-container .price',
            '.price-wrapper .price',
            
            # Дополнительные селекторы
            '.cost',
            '.amount',
            '.value',
            '.price-text',
            '.price-display',
            '.product-cost',
            '.goods-cost',
            '.item-cost',
            
            # Селекторы с атрибутами
            '[data-price]',
            '[data-cost]',
            '[data-value]',
            '.price[data-value]',
            '.cost[data-price]'
        ]
        
        for selector in price_selectors:
            element = parser.css_first(selector)
            if element:
                # Сначала пробуем атрибут data-price
                price_attr = element.attributes.get('data-price') or element.attributes.get('data-cost') or element.attributes.get('data-value')
                if price_attr:
                    try:
                        return float(price_attr.replace(',', '.'))
                    except ValueError:
                        pass
                
                # Затем текст элемента
                price_text = element.text(strip=True)
                if price_text:
                    # Убираем валютные символы и пробелы
                    price_text = re.sub(r'[^\d,.]', '', price_text)
                    if price_text:
                        try:
                            return float(price_text.replace(',', '.'))
                        except ValueError:
                            continue
        
        # Дополнительный поиск в тексте страницы
        page_text = parser.text()
        price_patterns = [
            r'(\d+(?:[.,]\d+)?)\s*₽',
            r'(\d+(?:[.,]\d+)?)\s*руб',
            r'Цена[:\s]*(\d+(?:[.,]\d+)?)',
            r'Стоимость[:\s]*(\d+(?:[.,]\d+)?)',
            r'(\d+(?:[.,]\d+)?)\s*рублей',
            r'(\d+(?:[.,]\d+)?)\s*р\.',
            r'цена[:\s]*(\d+(?:[.,]\d+)?)',
            r'стоимость[:\s]*(\d+(?:[.,]\d+)?)'
        ]
        
        for pattern in price_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                try:
                    return float(match.group(1).replace(',', '.'))
                except ValueError:
                    continue
        
        # Поиск в JSON данных на странице
        try:
            json_pattern = r'window\.__INITIAL_STATE__\s*=\s*({.*?});'
            json_match = re.search(json_pattern, page_text, re.DOTALL)
            if json_match:
                import json
                try:
                    data = json.loads(json_match.group(1))
                    # Ищем цену в JSON структуре
                    if 'product' in data and 'price' in data['product']:
                        return float(data['product']['price'])
                    if 'goods' in data and 'price' in data['goods']:
                        return float(data['goods']['price'])
                except (json.JSONDecodeError, KeyError, ValueError):
                    pass
        except Exception:
            pass
        
        return None
    
    def _extract_portion(self, parser: HTMLParser) -> Optional[float]:
        """Извлечь вес/объем порции."""
        # Ищем блок с характеристиками
        characteristics_selectors = [
            '.product-characteristics',
            '.product-specs',
            '.product-details',
            '.specifications'
        ]
        
        for selector in characteristics_selectors:
            container = parser.css_first(selector)
            if container:
                # Ищем строки с весом/объемом
                text = container.text()
                weight_match = re.search(r'Вес[:\s]+(\d+(?:[.,]\d+)?)\s*г', text, re.IGNORECASE)
                if weight_match:
                    try:
                        return float(weight_match.group(1).replace(',', '.'))
                    except ValueError:
                        continue
                
                volume_match = re.search(r'Объем[:\s]+(\d+(?:[.,]\d+)?)\s*мл', text, re.IGNORECASE)
                if volume_match:
                    try:
                        # Конвертируем мл в граммы (приблизительно 1:1 для жидкостей)
                        return float(volume_match.group(1).replace(',', '.'))
                    except ValueError:
                        continue
        
        return None
    
    def _extract_nutritional_info(self, parser: HTMLParser) -> Dict[str, Optional[float]]:
        """Извлечь БЖУ на 100г."""
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
            '.nutrition-facts',
            '.nutritional-info',
            '.food-nutrition',
            '.product-specs',
            '.characteristics-table',
            '.specifications-table',
            'table.nutrition',
            'table.food-value',
            '.nutrition-block',
            '.nutrition-section'
        ]
        
        for selector in nutrition_selectors:
            table = parser.css_first(selector)
            if table:
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
                                
                                if 'калори' in label or 'энерг' in label or 'kcal' in label.lower():
                                    nutritional_info['kcal'] = value
                                elif 'белк' in label or 'протеин' in label or 'protein' in label.lower():
                                    nutritional_info['protein'] = value
                                elif 'жир' in label or 'fat' in label.lower():
                                    nutritional_info['fat'] = value
                                elif 'углевод' in label or 'карб' in label or 'carb' in label.lower():
                                    nutritional_info['carb'] = value
                            except ValueError:
                                continue
        
        # Альтернативный поиск в тексте
        if not any(nutritional_info.values()):
            page_text = parser.text()
            
            # Ищем блок "Пищевая ценность на 100 г"
            nutrition_patterns = [
                r'Пищевая ценность на 100 г[:\s]*([^.]*)',
                r'Пищевая ценность на 100г[:\s]*([^.]*)',
                r'Пищевая ценность[:\s]*([^.]*)',
                r'Энергетическая ценность[:\s]*([^.]*)',
                r'БЖУ на 100г[:\s]*([^.]*)',
                r'БЖУ на 100 г[:\s]*([^.]*)',
                r'Пищевая ценность.*?100.*?г[:\s]*([^.]*)'
            ]
            
            for pattern in nutrition_patterns:
                nutrition_match = re.search(pattern, page_text, re.IGNORECASE | re.DOTALL)
                if nutrition_match:
                    nutrition_text = nutrition_match.group(1)
                    
                    # Извлекаем значения
                    kcal_match = re.search(r'(\d+(?:[.,]\d+)?)\s*ккал', nutrition_text, re.IGNORECASE)
                    if kcal_match:
                        try:
                            nutritional_info['kcal'] = float(kcal_match.group(1).replace(',', '.'))
                        except ValueError:
                            pass
                    
                    protein_match = re.search(r'белк[аиы]?\s*[:\s]*(\d+(?:[.,]\d+)?)', nutrition_text, re.IGNORECASE)
                    if protein_match:
                        try:
                            nutritional_info['protein'] = float(protein_match.group(1).replace(',', '.'))
                        except ValueError:
                            pass
                    
                    fat_match = re.search(r'жир[аы]?\s*[:\s]*(\d+(?:[.,]\d+)?)', nutrition_text, re.IGNORECASE)
                    if fat_match:
                        try:
                            nutritional_info['fat'] = float(fat_match.group(1).replace(',', '.'))
                        except ValueError:
                            pass
                    
                    carb_match = re.search(r'углевод[аы]?\s*[:\s]*(\d+(?:[.,]\d+)?)', nutrition_text, re.IGNORECASE)
                    if carb_match:
                        try:
                            nutritional_info['carb'] = float(carb_match.group(1).replace(',', '.'))
                        except ValueError:
                            pass
                    
                    break  # Если нашли блок, прекращаем поиск
        
        # Дополнительный поиск отдельных значений
        if not any(nutritional_info.values()):
            page_text = parser.text()
            
            # Ищем отдельные значения БЖУ
            kcal_match = re.search(r'(\d+(?:[.,]\d+)?)\s*ккал', page_text, re.IGNORECASE)
            if kcal_match:
                try:
                    nutritional_info['kcal'] = float(kcal_match.group(1).replace(',', '.'))
                except ValueError:
                    pass
            
            protein_match = re.search(r'белк[аиы]?\s*[:\s]*(\d+(?:[.,]\d+)?)', page_text, re.IGNORECASE)
            if protein_match:
                try:
                    nutritional_info['protein'] = float(protein_match.group(1).replace(',', '.'))
                except ValueError:
                    pass
            
            fat_match = re.search(r'жир[аы]?\s*[:\s]*(\d+(?:[.,]\d+)?)', page_text, re.IGNORECASE)
            if fat_match:
                try:
                    nutritional_info['fat'] = float(fat_match.group(1).replace(',', '.'))
                except ValueError:
                    pass
            
            carb_match = re.search(r'углевод[аы]?\s*[:\s]*(\d+(?:[.,]\d+)?)', page_text, re.IGNORECASE)
            if carb_match:
                try:
                    nutritional_info['carb'] = float(carb_match.group(1).replace(',', '.'))
                except ValueError:
                    pass
        
        return nutritional_info
    
    def _extract_composition(self, parser: HTMLParser) -> str:
        """Извлечь состав товара."""
        composition_selectors = [
            '.product-composition',
            '.composition',
            '.ingredients',
            '[data-testid="composition"]',
            '.product-ingredients'
        ]
        
        for selector in composition_selectors:
            element = parser.css_first(selector)
            if element:
                composition_text = element.text(strip=True)
                if composition_text and 'состав' in composition_text.lower():
                    return composition_text
        
        # Альтернативный поиск
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
            '.main-image img'
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
            '.product-attributes .attribute'
        ]
        
        for selector in tag_selectors:
            elements = parser.css(selector)
            for element in elements:
                tag_text = element.text(strip=True)
                if tag_text and len(tag_text) < 50:  # Фильтруем слишком длинные тексты
                    tags.append(tag_text)
        
        # Ищем в названии категории специальные теги
        category = self._extract_category(parser)
        if category:
            special_tags = [
                'мало калорий', 'много белка', 'без глютена', 'без сахара',
                'без лактозы', 'вегетарианское', 'диетическое', 'кето',
                'палео', 'детокс', 'спортивное питание', 'здоровое питание'
            ]
            
            for tag in special_tags:
                if tag.lower() in category.lower():
                    tags.append(tag)
        
        return '; '.join(tags) if tags else ''
    
    async def _set_location(self, city: str, coords: str):
        """Установить геолокацию для ВкусВилл."""
        try:
            # Сначала загружаем главную страницу для получения сессии
            main_response = await self.client.request(method="GET", url=self.BASE_URL)
            if main_response.status_code != 200:
                logging.warning(f"Failed to load main page: {main_response.status_code}")
                return
            
            # Устанавливаем cookies для геолокации
            location_cookies = {
                'city': city,
                'coords': coords,
                'region': 'moscow' if 'moscow' in city.lower() else 'spb',
                'location_set': '1',
                'user_city': city,
                'user_coords': coords
            }
            
            # Обновляем сессию с cookies
            if self.session:
                for name, value in location_cookies.items():
                    self.session.cookies.set(name, value)
            
            # Попробуем установить город через API
            try:
                # Ищем API endpoint для установки города
                api_endpoints = [
                    f"{self.BASE_URL}/ajax/set_city.php",
                    f"{self.BASE_URL}/api/set_city",
                    f"{self.BASE_URL}/ajax/city.php",
                    f"{self.BASE_URL}/api/location"
                ]
                
                for api_url in api_endpoints:
                    try:
                        response = await self.client.request(
                            method="POST", 
                            url=api_url,
                            data={'city': city, 'coords': coords}
                        )
                        if response.status_code == 200:
                            logging.info(f"Location API call successful for {city} via {api_url}")
                            break
                    except Exception:
                        continue
                        
            except Exception as api_error:
                logging.warning(f"Location API call failed: {api_error}")
            
            # Проверяем, что геолокация установлена
            try:
                test_response = await self.client.request(method="GET", url=f"{self.BASE_URL}/goods/gotovaya-eda/")
                if test_response.status_code == 200:
                    logging.info(f"Location verification successful - catalog accessible")
                else:
                    logging.warning(f"Location verification failed - catalog returned {test_response.status_code}")
            except Exception as test_error:
                logging.warning(f"Location verification failed: {test_error}")
            
            logging.info(f"Location set to {city} ({coords})")
            
        except Exception as e:
            logging.warning(f"Failed to set location: {e}")
    
    async def _scrape_alternative_approach(self) -> set:
        """Альтернативный подход к скрейпингу когда основной не работает."""
        product_urls = set()
        
        try:
            # Попробуем поиск по ключевым словам
            search_terms = [
                "готовая еда",
                "готовые блюда", 
                "салаты готовые",
                "супы готовые",
                "вторые блюда готовые"
            ]
            
            for term in search_terms:
                # Кодируем поисковый запрос для URL
                from urllib.parse import quote
                encoded_term = quote(term)
                search_url = f"{self.BASE_URL}/search/?q={encoded_term}"
                logging.info(f"Trying search: {search_url}")
                
                try:
                    response = await self.client.request(method="GET", url=search_url)
                    if response.status_code == 200:
                        html = response.text
                        parser = HTMLParser(html)
                        
                        # Ищем ссылки на товары в результатах поиска
                        product_links = parser.css('a[href*="/product/"]')
                        for link in product_links:
                            href = link.attributes.get('href')
                            if href and '/product/' in href:
                                full_url = urljoin(self.BASE_URL, href)
                                product_urls.add(full_url)
                        
                        if product_urls:
                            logging.info(f"Found {len(product_urls)} products via search")
                            break
                    
                    await asyncio.sleep(1.0)
                    
                except Exception as e:
                    logging.error(f"Search failed for '{term}': {e}")
                    continue
            
            # Если поиск не дал результатов, попробуем прямые ссылки на популярные товары
            if not product_urls:
                logging.info("Trying direct product URLs")
                # Это примеры - в реальности нужно найти актуальные ID товаров
                sample_products = [
                    "/product/12345",  # Пример ID
                    "/product/12346",
                    "/product/12347"
                ]
                
                for product_path in sample_products:
                    product_url = urljoin(self.BASE_URL, product_path)
                    try:
                        response = await self.client.request(method="GET", url=product_url)
                        if response.status_code == 200:
                            product_urls.add(product_url)
                    except Exception:
                        continue
            
        except Exception as e:
            logging.error(f"Alternative approach failed: {e}")
        
        return product_urls

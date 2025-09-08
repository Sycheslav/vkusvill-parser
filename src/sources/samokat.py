"""
Скрейпер для сервиса Самокат (eda.samokat.ru)
"""
import re
import logging
import time
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin, urlparse
import asyncio

# Используем абсолютные импорты
try:
    from src.sources.base import BaseScraper, ScrapedProduct
except ImportError:
    try:
        from sources.base import BaseScraper, ScrapedProduct
    except ImportError:
        # Для тестирования
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from sources.base import BaseScraper, ScrapedProduct


class SamokatScraper(BaseScraper):
    """Скрейпер для Самоката"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        # Обновленный URL для Самоката
        self.base_url = "https://samokat.ru"
        self.city = config.get('city', 'Москва')
        self.coords = config.get('coords')
        self.session_id = None
        
    async def setup_location(self):
        """Настройка локации для получения доступной витрины"""
        try:
            self.logger.info(f"[{self.__class__.__name__}] setup_location вызван для города: {self.city}")
            
            # Убеждаемся, что браузер готов
            await self._ensure_browser_ready()
            self.logger.info(f"[{self.__class__.__name__}] Браузер готов для настройки локации")
            
            # Переходим на главную страницу Самоката
            self.logger.info(f"[{self.__class__.__name__}] Переходим на главную страницу: {self.base_url}")
            await self.page.goto(self.base_url, timeout=30000)
            await self.page.wait_for_load_state("domcontentloaded", timeout=15000)
            await asyncio.sleep(2)
            
            # Проверяем, что страница загрузилась
            current_url = self.page.url
            self.logger.info(f"[{self.__class__.__name__}] Текущий URL: {current_url}")
            
            # Пытаемся найти элементы для выбора города
            city_selectors = [
                '[data-testid="city-selector"]', '.city-selector', '.location-selector',
                'button:has-text("Москва")', 'button:has-text("Санкт-Петербург")',
                '[class*="city"]', '[class*="location"]'
            ]
            
            city_found = False
            for selector in city_selectors:
                try:
                    city_element = await self.page.query_selector(selector)
                    if city_element:
                        self.logger.info(f"[{self.__class__.__name__}] Найден элемент города: {selector}")
                        city_found = True
                        break
                except:
                    continue
            
            if not city_found:
                self.logger.warning(f"[{self.__class__.__name__}] Элементы выбора города не найдены, продолжаем без настройки")
            
            self.logger.info(f"[{self.__class__.__name__}] Локация настроена для {self.city}")
            
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Ошибка настройки локации: {e}")
            self.logger.error(f"[{self.__class__.__name__}] Traceback: ", exc_info=True)
            # Не прерываем выполнение, продолжаем без настройки локации
            
    async def get_categories(self) -> List[str]:
        """Получить список доступных категорий готовой еды"""
        try:
            self.logger.info(f"[{self.__class__.__name__}] get_categories вызван")
            
            # Возвращаем реальные категории Самоката
            categories = [
                'Вся готовая еда',
                'Салаты и закуски', 
                'Супы',
                'Холодные супы',
                'Всё горячее',
                'Завтрак',
                'Стритфуд',
                'Комбо-наборы',
                'Из ресторанов и кафе',
                'Горячая выпечка',
                'Большие порции',
                'Второе с мясом',
                'Второе с птицей'
            ]
            
            self.logger.info(f"[{self.__class__.__name__}] Возвращаем {len(categories)} категорий")
            return categories
            
        except Exception as e:
            self.logger.error(f"Ошибка получения категорий: {e}")
            return ['Вся готовая еда', 'Салаты и закуски', 'Супы']
            
    async def scrape_category(self, category: str, limit: int = None) -> List[ScrapedProduct]:
        """Скрапить продукты из указанной категории"""
        try:
            self.logger.info(f"[{self.__class__.__name__}] scrape_category вызван для категории: {category}, лимит: {limit}")
            
            # Убеждаемся, что браузер готов
            await self._ensure_browser_ready()
            self.logger.info(f"[{self.__class__.__name__}] Браузер готов для парсинга категории: {category}")
            
            # Настраиваем локацию
            self.logger.info(f"[{self.__class__.__name__}] Настраиваем локацию для категории: {category}")
            await self.setup_location()
            self.logger.info(f"[{self.__class__.__name__}] Локация настроена для категории: {category}")
            
            # Переходим на страницу категории с правильными URL
            category_urls = {
                'Вся готовая еда': 'https://samokat.ru/category/vsya-gotovaya-eda-13',
                'Салаты и закуски': 'https://samokat.ru/category/salaty-i-zakuski',
                'Супы': 'https://samokat.ru/category/supy',
                'Холодные супы': 'https://samokat.ru/category/kholodnye-supy',
                'Всё горячее': 'https://samokat.ru/category/vsyo-goryachee-1',
                'Завтрак': 'https://samokat.ru/category/zavtrak',
                'Стритфуд': 'https://samokat.ru/category/stritfud-1',
                'Комбо-наборы': 'https://samokat.ru/category/kombo-nabory',
                'Из ресторанов и кафе': 'https://samokat.ru/category/iz-restoranov-i-kafe',
                'Горячая выпечка': 'https://samokat.ru/category/goryachaya-vypechka',
                'Большие порции': 'https://samokat.ru/category/bolshie-portsii',
                'Второе с мясом': 'https://samokat.ru/category/vtoroe-s-myasom',
                'Второе с птицей': 'https://samokat.ru/category/vtoroe-s-ptitsey'
            }
            
            category_url = category_urls.get(category, f"{self.base_url}/category/gotovaya-eda-25")
            self.logger.info(f"[{self.__class__.__name__}] Переходим на {category_url}")
            
            try:
                await self.page.goto(category_url, timeout=30000)  # Увеличиваем таймаут для стабильности
                await self.page.wait_for_load_state("domcontentloaded", timeout=15000)
                await asyncio.sleep(2)  # Время для загрузки контента
                
                # Ждем загрузки контента
                await self.page.wait_for_load_state("networkidle", timeout=15000)
                await asyncio.sleep(1)
                
                # Прокручиваем страницу для загрузки товаров
                target_limit = limit or 1000
                await self._scroll_page_for_more_products(target_limit)
                
                # Дополнительная прокрутка для получения большего количества товаров
                await asyncio.sleep(1)
                await self.page.evaluate("window.scrollTo(0, 0)")  # Прокручиваем в начало
                await asyncio.sleep(1)
                await self._scroll_page_for_more_products(target_limit)  # Еще больше прокрутки
                
                # Третья волна прокрутки для максимального количества товаров
                await asyncio.sleep(1)
                await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")  # Прокручиваем к середине
                await asyncio.sleep(1)
                await self._scroll_page_for_more_products(target_limit)
                
            except Exception as e:
                self.logger.warning(f"[{self.__class__.__name__}] Не удалось загрузить страницу категории: {e}")
                # Пробуем альтернативный URL
                try:
                    await self.page.goto('https://samokat.ru/category/gotovaya-eda-25', timeout=60000)
                    await self.page.wait_for_load_state("domcontentloaded", timeout=60000)
                    await asyncio.sleep(8)
                    await self.page.wait_for_load_state("networkidle", timeout=60000)
                    await asyncio.sleep(5)
                    
                    # Прокручиваем альтернативную страницу
                    target_limit = limit or 1000
                    await self._scroll_page_for_more_products(target_limit)
                    
                except Exception as e2:
                    self.logger.error(f"[{self.__class__.__name__}] Не удалось загрузить альтернативную страницу: {e2}")
                    # Продолжаем без загрузки страницы
            
            # Ищем карточки товаров - максимально расширенные селекторы для Самоката
            product_selectors = [
                # Основные селекторы Самоката
                '[data-testid="product-card"]', '.ProductCard', '.product-card',
                '.ProductItem', '.product-item', '.Product',
                # Селекторы каталога
                '.CatalogItem', '.catalog-item', '.ItemCard',
                '.ProductGrid > *', '.product-grid > *',
                '.ProductList > *', '.product-list > *',
                # Общие селекторы
                'article[data-testid]', 'article[class*="product"]',
                '[data-product-id]', '[data-testid*="product"]',
                '.item[class*="product"]', '.card[class*="product"]',
                # Дополнительные селекторы
                'div[class*="Product"]', 'div[class*="Item"]',
                'section[class*="product"]', 'div[class*="catalog"]',
                # Универсальные селекторы для поиска любых товаров
                'div[class*="card"]', 'div[class*="item"]',
                'article', 'section', 'div[role="article"]',
                'div[class*="grid"] > div', 'div[class*="list"] > div',
                'div[class*="container"] > div', 'div[class*="wrapper"] > div',
                # Селекторы для мобильной версии
                '[class*="mobile"] [class*="product"]', '[class*="mobile"] [class*="item"]',
                '[class*="mobile"] [class*="card"]', '[class*="mobile"] article',
                # Селекторы для десктопной версии
                '[class*="desktop"] [class*="product"]', '[class*="desktop"] [class*="item"]',
                '[class*="desktop"] [class*="card"]', '[class*="desktop"] article',
                # Дополнительные селекторы для максимального покрытия
                'div[class*="goods"]', 'div[class*="Goods"]',
                'div[class*="catalog"]', 'div[class*="Catalog"]',
                'div[class*="shop"]', 'div[class*="Shop"]',
                'div[class*="market"]', 'div[class*="Market"]',
                'div[class*="store"]', 'div[class*="Store"]',
                # Селекторы для React компонентов
                '[class*="ProductCard"]', '[class*="ProductItem"]',
                '[class*="CatalogItem"]', '[class*="ItemCard"]',
                '[class*="GoodsItem"]', '[class*="GoodsCard"]',
                # Селекторы для любых элементов с товарной информацией
                'div[class*="price"]', 'div[class*="Price"]',
                'div[class*="name"]', 'div[class*="Name"]',
                'div[class*="title"]', 'div[class*="Title"]'
            ]
            
            products = []
            total_found = 0
            
            self.logger.info(f"[{self.__class__.__name__}] Начинаем поиск товаров с {len(product_selectors)} селекторами")
            
            for i, selector in enumerate(product_selectors):
                try:
                    self.logger.info(f"[{self.__class__.__name__}] Проверяем селектор {i+1}/{len(product_selectors)}: {selector}")
                    elements = await self.page.query_selector_all(selector)
                    if elements:
                        self.logger.info(f"[{self.__class__.__name__}] ✅ Найдено {len(elements)} элементов с селектором {selector}")
                        total_found = len(elements)
                        
                        # Обрабатываем все найденные товары
                        target_limit = limit or 1000  # Целевой лимит 1000 товаров
                        self.logger.info(f"[{self.__class__.__name__}] Обрабатываем {len(elements)} элементов с селектором {selector}")
                        
                        # Обрабатываем все найденные элементы
                        for j, element in enumerate(elements):
                            try:
                                # Быстрое извлечение без детального парсинга
                                product = await self._extract_product_fast(element, category)
                                if product:
                                    products.append(product)
                                    
                                    # Логируем прогресс каждые 25 товаров
                                    if len(products) % 25 == 0:
                                        self.logger.info(f"[{self.__class__.__name__}] Обработано {len(products)} товаров...")
                                
                                # Останавливаемся при достижении лимита
                                if len(products) >= target_limit:
                                    self.logger.info(f"[{self.__class__.__name__}] Достигнут лимит {target_limit} товаров!")
                                    break
                                    
                            except Exception as e:
                                # Игнорируем ошибки отдельных товаров
                                continue
                        
                        # Если достигли лимита, прекращаем поиск
                        if len(products) >= target_limit:
                            self.logger.info(f"[{self.__class__.__name__}] Достигнут целевой лимит товаров: {len(products)}")
                            break
                    else:
                        self.logger.debug(f"[{self.__class__.__name__}] ❌ Элементы не найдены с селектором {selector}")
                except Exception as e:
                    self.logger.debug(f"[{self.__class__.__name__}] Ошибка с селектором {selector}: {e}")
                    continue
            
            # Возвращаем только реальные товары, не создаем тестовые
            if not products:
                self.logger.warning(f"[{self.__class__.__name__}] Не найдено реальных товаров для категории {category}")
                return []
            
            self.logger.info(f"[{self.__class__.__name__}] Найдено {len(products)} реальных товаров")
            
            self.logger.info(f"[{self.__class__.__name__}] Итого товаров: {len(products)}")
            return products
            
        except Exception as e:
            self.logger.error(f"Ошибка скрапинга категории {category}: {e}")
            return []
            
    async def _extract_product_fast(self, element, category: str) -> Optional[ScrapedProduct]:
        """Быстрое извлечение данных продукта без детального парсинга"""
        try:
            if not element:
                return None
                
            # Извлекаем название товара
            name = "Неизвестный товар"
            name_selectors = [
                '.product-name', '.ProductName', '.product-title', '.ProductTitle',
                '.title', '.Title', 'h3', 'h4', 'h5',
                '[class*="name"]', '[class*="title"]', '[class*="Name"]', '[class*="Title"]',
                '[data-testid*="name"]', '[data-testid*="title"]',
                'strong', 'b', '.name', '.Name'
            ]
            
            for selector in name_selectors:
                try:
                    name_elem = await element.query_selector(selector)
                    if name_elem:
                        name_text = await name_elem.text_content()
                        if name_text and len(name_text.strip()) > 3:
                            name = name_text.strip()[:100]
                            break
                except:
                    continue
            
            # Фильтруем мусор и рекламные сообщения
            name_clean = name.strip()
            spam_keywords = [
                'авторизуйтесь', 'укажите адрес', 'персональная скидка', 'случайных товаров',
                'основной ингредиент', 'сортировка', 'загрузка', 'loading', 'загружается',
                'показать еще', 'загрузить еще', 'еще товары', 'больше товаров',
                'реклама', 'advertisement', 'ads', 'баннер', 'banner',
                'cookie', 'куки', 'политика', 'policy', 'соглашение', 'agreement',
                'подписка', 'subscription', 'рассылка', 'newsletter', 'товар', 'из'
            ]
            
            # Проверяем на спам
            name_lower = name_clean.lower()
            for spam_word in spam_keywords:
                if spam_word in name_lower:
                    return None
            
            # Проверяем, что это реальное название товара (содержит буквы)
            if not any(c.isalpha() for c in name_clean):
                return None
            
            # Проверяем минимальную длину реального названия
            if len(name_clean) < 5:
                return None
            
            # Извлекаем цену
            price = None
            price_selectors = [
                '.price', '.Price', '.product-price', '.ProductPrice',
                '.cost', '.Cost', '.item-price', '.ItemPrice',
                '[data-price]', '[class*="price"]', '[class*="Price"]',
                '[class*="cost"]', '[class*="Cost"]'
            ]
            
            for selector in price_selectors:
                try:
                    price_elem = await element.query_selector(selector)
                    if price_elem:
                        price_text = await price_elem.text_content()
                        if price_text:
                            price = self._extract_price(price_text)
                            if price and price > 0:
                                break
                except:
                    continue
            
            # Извлекаем URL товара - максимально агрессивные селекторы
            url = ""
            
            # Метод 1: Поиск по всем возможным селекторам ссылок
            link_selectors = [
                'a[href]', '[href]', 'a', 
                '.product-link', '.ProductLink', '.item-link', '.ItemLink',
                '.card-link', '.CardLink', '.product-card a', '.ProductCard a',
                '.product-item a', '.ProductItem a', '.catalog-item a', '.CatalogItem a',
                '[data-href]', '[data-url]', '[data-link]', '[data-product-url]',
                'a[class*="product"]', 'a[class*="item"]', 'a[class*="card"]',
                'a[class*="Product"]', 'a[class*="Item"]', 'a[class*="Card"]',
                'a[class*="link"]', 'a[class*="Link"]', 'a[class*="url"]',
                'a[onclick]', 'a[data-click]', 'a[data-action]',
                'button[data-href]', 'button[data-url]', 'div[data-href]',
                'span[data-href]', 'div[onclick]', 'span[onclick]'
            ]
            
            for selector in link_selectors:
                try:
                    link_elem = await element.query_selector(selector)
                    if link_elem:
                        # Пробуем разные атрибуты для получения ссылки
                        href_attrs = ['href', 'data-href', 'data-url', 'data-link', 'data-product-url', 'data-action-url']
                        for attr in href_attrs:
                            url = await link_elem.get_attribute(attr) or ""
                            if url and url.strip():
                                break
                        
                        if url and url.strip():
                            # Очищаем URL от лишних символов
                            url = url.strip()
                            # Если URL не полный, делаем его полным
                            if not url.startswith('http'):
                                url = urljoin(self.base_url, url)
                            # Проверяем, что это валидная ссылка на товар
                            if any(pattern in url for pattern in ['/product/', '/goods/', '/item/', '/catalog/', '.html', '/p/']):
                                break
                            else:
                                url = ""  # Сбрасываем если не похоже на ссылку товара
                except:
                    continue
            
            # Метод 2: Если URL не найден, ищем в onclick событиях
            if not url:
                try:
                    onclick_elem = await element.query_selector('[onclick]')
                    if onclick_elem:
                        onclick_text = await onclick_elem.get_attribute('onclick') or ""
                        if onclick_text:
                            # Ищем URL в onclick (например: "window.location='/product/123'")
                            import re
                            url_match = re.search(r"['\"]([^'\"]*/(?:product|goods|item|catalog)/[^'\"]*)['\"]", onclick_text)
                            if url_match:
                                url = url_match.group(1)
                                if not url.startswith('http'):
                                    url = urljoin(self.base_url, url)
                except:
                    pass
            
            # Метод 3: Если URL все еще не найден, ищем в data-атрибутах всего элемента
            if not url:
                try:
                    data_attrs = ['data-href', 'data-url', 'data-link', 'data-product-url', 'data-action-url', 'data-product-id']
                    for attr in data_attrs:
                        url = await element.get_attribute(attr) or ""
                        if url and url.strip():
                            url = url.strip()
                            if not url.startswith('http'):
                                url = urljoin(self.base_url, url)
                            if any(pattern in url for pattern in ['/product/', '/goods/', '/item/', '/catalog/', '.html', '/p/']):
                                break
                            else:
                                url = ""
                except:
                    pass
            
            # Метод 4: Генерируем URL на основе ID товара если найден
            if not url:
                try:
                    # Ищем ID товара в различных атрибутах
                    id_attrs = ['data-product-id', 'data-id', 'data-item-id', 'id']
                    for attr in id_attrs:
                        product_id = await element.get_attribute(attr) or ""
                        if product_id and product_id.strip():
                            # Генерируем URL на основе ID
                            url = f"{self.base_url}/product/{product_id.strip()}"
                            break
                except:
                    pass
            
            # Извлекаем изображение
            image_url = ""
            img_selectors = [
                '.product-image img', '.ProductImage img', '.product-photo img',
                '.item-image img', '.ItemImage img', '.card-image img',
                'img[src]', 'img[data-src]', 'img[data-lazy]'
            ]
            
            for selector in img_selectors:
                try:
                    img_elem = await element.query_selector(selector)
                    if img_elem:
                        image_url = await img_elem.get_attribute('src') or await img_elem.get_attribute('data-src') or await img_elem.get_attribute('data-lazy') or ""
                        if image_url and not image_url.startswith('http'):
                            image_url = urljoin(self.base_url, image_url)
                        if image_url:
                            break
                except:
                    continue
            
            # Извлекаем состав/описание
            composition = ""
            comp_selectors = [
                '.product-description', '.ProductDescription', '.product-composition',
                '.item-description', '.ItemDescription', '.card-description',
                '.description', '.Description', '.composition', '.Composition',
                '[class*="description"]', '[class*="composition"]'
            ]
            
            for selector in comp_selectors:
                try:
                    comp_elem = await element.query_selector(selector)
                    if comp_elem:
                        comp_text = await comp_elem.text_content()
                        if comp_text and len(comp_text.strip()) > 5:
                            composition = comp_text.strip()[:200]
                            break
                except:
                    continue
            
            # Извлекаем вес/порцию
            portion_g = None
            weight_selectors = [
                '.product-weight', '.ProductWeight', '.product-portion',
                '.item-weight', '.ItemWeight', '.item-portion',
                '.weight', '.Weight', '.portion', '.Portion',
                '[class*="weight"]', '[class*="portion"]'
            ]
            
            for selector in weight_selectors:
                try:
                    weight_elem = await element.query_selector(selector)
                    if weight_elem:
                        weight_text = await weight_elem.text_content()
                        if weight_text:
                            # Извлекаем число из текста (например "250г" -> 250)
                            weight_match = re.search(r'(\d+)', weight_text.replace(' ', ''))
                            if weight_match:
                                portion_g = float(weight_match.group(1))
                                break
                except:
                    continue
            
            # Извлекаем бренд
            brand = None
            brand_selectors = [
                '.product-brand', '.ProductBrand', '.brand', '.Brand',
                '.manufacturer', '.Manufacturer', '[class*="brand"]'
            ]
            
            for selector in brand_selectors:
                try:
                    brand_elem = await element.query_selector(selector)
                    if brand_elem:
                        brand_text = await brand_elem.text_content()
                        if brand_text and len(brand_text.strip()) > 2:
                            brand = brand_text.strip()[:50]
                            break
                except:
                    continue
            
            # Генерируем ID из URL или названия
            product_id = f"samokat_{hash(name + str(price))}"
            if url:
                url_parts = urlparse(url).path.split('/')
                for part in url_parts:
                    if part and part.isdigit():
                        product_id = f"samokat_{part}"
                        break
            
            # Создаем продукт только если есть реальные данные
            if name != "Неизвестный товар" and (price or url):
                product = ScrapedProduct(
                    id=product_id,
                    name=name,
                    category=category,
                    price=price,
                    url=url,
                    shop="samokat",
                    composition=composition,
                    portion_g=portion_g
                )
                
                # Если есть URL, получаем детальную информацию
                if url and url.strip():
                    try:
                        detailed_product = await self.scrape_product_page(url)
                        if detailed_product:
                            # Обновляем базовую информацию детальной
                            product.kcal_100g = detailed_product.kcal_100g
                            product.protein_100g = detailed_product.protein_100g
                            product.fat_100g = detailed_product.fat_100g
                            product.carb_100g = detailed_product.carb_100g
                            if detailed_product.portion_g:
                                product.portion_g = detailed_product.portion_g
                            if detailed_product.composition:
                                product.composition = detailed_product.composition
                    except Exception as e:
                        self.logger.debug(f"Не удалось получить детальную информацию для {url}: {e}")
                
                return product
            
            return None
            
        except Exception as e:
            # Игнорируем ошибки для ускорения
            return None
            
    async def _extract_product_from_card(self, element, category: str) -> Optional[ScrapedProduct]:
        """Извлечь данные продукта из карточки товара"""
        try:
            if not element:
                self.logger.warning(f"[{self.__class__.__name__}] Элемент карточки товара не передан")
                return None
                
            self.logger.info(f"[{self.__class__.__name__}] Обрабатываем элемент: {element}")
            
            # Основная информация - расширенные селекторы
            name_selectors = [
                '.product-name', '.item-name', '.title', 'h3', 'h4', 'h5',
                '.product-title', '.item-title', '.name', '.product-name',
                '[class*="name"]', '[class*="title"]', 'strong', 'b'
            ]
            
            name = "Неизвестный товар"
            for selector in name_selectors:
                try:
                    name_elem = await element.query_selector(selector)
                    if name_elem:
                        name_text = await name_elem.text_content()
                        if name_text and len(name_text.strip()) > 3:
                            name = name_text.strip()
                            self.logger.info(f"[{self.__class__.__name__}] Название найдено: {name}")
                            break
                except:
                    continue
            
            # Если название не найдено, берем весь текст элемента
            if name == "Неизвестный товар":
                try:
                    full_text = await element.text_content()
                    if full_text and len(full_text.strip()) > 10:
                        # Берем первые 100 символов как название
                        name = full_text.strip()[:100]
                        self.logger.info(f"[{self.__class__.__name__}] Название из текста элемента: {name}")
                except:
                    pass
            
            # Цена - расширенные селекторы
            price_selectors = [
                '.price', '.cost', '.item-price', '[data-price]', '.product-price',
                '.price-value', '.cost-value', '[class*="price"]', '[class*="cost"]',
                'span[class*="price"]', 'div[class*="price"]'
            ]
            
            price = 0.0
            for selector in price_selectors:
                try:
                    price_elem = await element.query_selector(selector)
                    if price_elem:
                        price_text = await price_elem.text_content()
                        if price_text:
                            price = self._extract_price(price_text)
                            if price > 0:
                                self.logger.info(f"[{self.__class__.__name__}] Цена найдена: {price}")
                                break
                except:
                    continue
            
            # URL товара - расширенные селекторы
            link_selectors = [
                'a[href]', '[href]', 'a', 
                '.product-link', '.ProductLink', '.item-link', '.ItemLink',
                '.card-link', '.CardLink', '.product-card a', '.ProductCard a',
                '.product-item a', '.ProductItem a', '.catalog-item a', '.CatalogItem a',
                '[data-href]', '[data-url]', '[data-link]',
                'a[class*="product"]', 'a[class*="item"]', 'a[class*="card"]',
                'a[class*="Product"]', 'a[class*="Item"]', 'a[class*="Card"]'
            ]
            url = ""
            for selector in link_selectors:
                try:
                    link_elem = await element.query_selector(selector)
                    if link_elem:
                        # Пробуем разные атрибуты для получения ссылки
                        href_attrs = ['href', 'data-href', 'data-url', 'data-link']
                        for attr in href_attrs:
                            url = await link_elem.get_attribute(attr) or ""
                            if url and url.strip():
                                break
                        
                        if url and url.strip():
                            # Очищаем URL от лишних символов
                            url = url.strip()
                            # Если URL не полный, делаем его полным
                            if not url.startswith('http'):
                                url = urljoin(self.base_url, url)
                            # Проверяем, что это валидная ссылка на товар
                            if '/product/' in url or '/goods/' in url or '/item/' in url or url.endswith('.html'):
                                self.logger.info(f"[{self.__class__.__name__}] URL найден: {url}")
                                break
                            else:
                                url = ""  # Сбрасываем если не похоже на ссылку товара
                except:
                    continue
                
            # Изображение
            img_selectors = ['img[src]', 'img[data-src]', 'img', '[src]', '[data-src]']
            image_url = ""
            for selector in img_selectors:
                try:
                    img_elem = await element.query_selector(selector)
                    if img_elem:
                        image_url = await img_elem.get_attribute('src') or await img_elem.get_attribute('data-src')
                        if image_url:
                            if not image_url.startswith('http'):
                                image_url = urljoin(self.base_url, image_url)
                            self.logger.info(f"[{self.__class__.__name__}] Изображение найдено: {image_url}")
                            break
                except:
                    continue
                
            # ID товара
            product_id = await self._extract_product_id(url, element)
            
            # Создаем базовый продукт
            product = ScrapedProduct(
                id=product_id,
                name=name,
                category=category,
                price=price,
                url=url,
                image_url=image_url,
                shop="samokat",
                available=True
            )
            
            self.logger.info(f"[{self.__class__.__name__}] Продукт создан: {name} (цена: {price}, URL: {url})")
            
            # Пропускаем детальную информацию для ускорения
            # if url:
            #     try:
            #         detailed_product = await self.scrape_product_page(url)
            #         if detailed_product:
            #             # Обновляем базовую информацию детальной
            #             product.kcal_100g = detailed_product.kcal_100g
            #             product.protein_100g = detailed_product.protein_100g
            #             product.fat_100g = detailed_product.fat_100g
            #             product.carb_100g = detailed_product.carb_100g
            #             product.portion_g = detailed_product.portion_g
            #             product.composition = detailed_product.composition
            #             product.tags = detailed_product.tags
            #             product.brand = detailed_product.brand
            #             product.allergens = detailed_product.allergens
            #             product.extra = detailed_product.extra
            #     except Exception as e:
            #         self.logger.debug(f"Не удалось получить детальную информацию для {url}: {e}")
                    
            return product
            
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Ошибка извлечения продукта: {e}")
            return None
            
    async def scrape_product_page(self, url: str) -> Optional[ScrapedProduct]:
        """Скрапить детальную страницу продукта для получения пищевой ценности"""
        try:
            if not url or not url.strip():
                return None
                
            self.logger.info(f"[{self.__class__.__name__}] Парсим детальную страницу: {url}")
            
            # Переходим на страницу товара
            await self.page.goto(url, timeout=30000)
            await self.page.wait_for_load_state("domcontentloaded", timeout=15000)
            await asyncio.sleep(2)
            
            # Извлекаем пищевую ценность
            nutrition_data = await self._extract_nutrition_data()
            
            # Извлекаем дополнительную информацию
            composition = await self._extract_detailed_composition()
            portion_g = await self._extract_detailed_portion()
            brand = await self._extract_brand()
            allergens = await self._extract_allergens()
            
            # Создаем продукт с детальной информацией
            product = ScrapedProduct(
                id=f"samokat_detailed_{hash(url)}",
                name="",  # Будет заполнено из базовой информации
                category="",  # Будет заполнено из базовой информации
                kcal_100g=nutrition_data.get('kcal_100g'),
                protein_100g=nutrition_data.get('protein_100g'),
                fat_100g=nutrition_data.get('fat_100g'),
                carb_100g=nutrition_data.get('carb_100g'),
                portion_g=portion_g,
                composition=composition,
                url=url,
                shop="samokat"
            )
            
            self.logger.info(f"[{self.__class__.__name__}] Детальная информация извлечена: {nutrition_data}")
            return product
            
        except Exception as e:
            self.logger.error(f"Ошибка парсинга детальной страницы {url}: {e}")
            return None
    
    async def _extract_nutrition_data(self) -> Dict[str, Optional[float]]:
        """Извлечь данные о пищевой ценности"""
        nutrition_data = {
            'kcal_100g': None,
            'protein_100g': None,
            'fat_100g': None,
            'carb_100g': None
        }
        
        try:
            # Селекторы для пищевой ценности
            nutrition_selectors = [
                '.nutrition-table', '.nutrition-info', '.nutrition-facts',
                '.product-nutrition', '.nutritional-info', '.nutrition',
                '[class*="nutrition"]', '[class*="calorie"]', '[class*="protein"]',
                '.kcal', '.calories', '.energy', '.protein', '.fat', '.carb'
            ]
            
            for selector in nutrition_selectors:
                try:
                    nutrition_elem = await self.page.query_selector(selector)
                    if nutrition_elem:
                        # Ищем калории
                        kcal_selectors = [
                            '.kcal', '.calories', '.energy', '[class*="kcal"]', '[class*="calorie"]'
                        ]
                        for kcal_sel in kcal_selectors:
                            kcal_elem = await nutrition_elem.query_selector(kcal_sel)
                            if kcal_elem:
                                kcal_text = await kcal_elem.text_content()
                                if kcal_text:
                                    kcal_match = re.search(r'(\d+(?:[.,]\d+)?)', kcal_text.replace(' ', ''))
                                    if kcal_match:
                                        nutrition_data['kcal_100g'] = float(kcal_match.group(1).replace(',', '.'))
                                        break
                        
                        # Ищем белки
                        protein_selectors = [
                            '.protein', '[class*="protein"]', '.proteins'
                        ]
                        for prot_sel in protein_selectors:
                            prot_elem = await nutrition_elem.query_selector(prot_sel)
                            if prot_elem:
                                prot_text = await prot_elem.text_content()
                                if prot_text:
                                    prot_match = re.search(r'(\d+(?:[.,]\d+)?)', prot_text.replace(' ', ''))
                                    if prot_match:
                                        nutrition_data['protein_100g'] = float(prot_match.group(1).replace(',', '.'))
                                        break
                        
                        # Ищем жиры
                        fat_selectors = [
                            '.fat', '[class*="fat"]', '.fats'
                        ]
                        for fat_sel in fat_selectors:
                            fat_elem = await nutrition_elem.query_selector(fat_sel)
                            if fat_elem:
                                fat_text = await fat_elem.text_content()
                                if fat_text:
                                    fat_match = re.search(r'(\d+(?:[.,]\d+)?)', fat_text.replace(' ', ''))
                                    if fat_match:
                                        nutrition_data['fat_100g'] = float(fat_match.group(1).replace(',', '.'))
                                        break
                        
                        # Ищем углеводы
                        carb_selectors = [
                            '.carb', '.carbohydrate', '[class*="carb"]', '.carbs'
                        ]
                        for carb_sel in carb_selectors:
                            carb_elem = await nutrition_elem.query_selector(carb_sel)
                            if carb_elem:
                                carb_text = await carb_elem.text_content()
                                if carb_text:
                                    carb_match = re.search(r'(\d+(?:[.,]\d+)?)', carb_text.replace(' ', ''))
                                    if carb_match:
                                        nutrition_data['carb_100g'] = float(carb_match.group(1).replace(',', '.'))
                                        break
                        
                        break
                except:
                    continue
            
            # Если не нашли в специальных блоках, ищем по всему тексту страницы
            if not any(nutrition_data.values()):
                try:
                    page_text = await self.page.text_content('body')
                    if page_text:
                        # Ищем калории в тексте
                        kcal_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:ккал|kcal|калори)', page_text, re.IGNORECASE)
                        if kcal_match:
                            nutrition_data['kcal_100g'] = float(kcal_match.group(1).replace(',', '.'))
                        
                        # Ищем белки в тексте
                        protein_match = re.search(r'белк[аиы]?\s*(\d+(?:[.,]\d+)?)', page_text, re.IGNORECASE)
                        if protein_match:
                            nutrition_data['protein_100g'] = float(protein_match.group(1).replace(',', '.'))
                        
                        # Ищем жиры в тексте
                        fat_match = re.search(r'жир[аы]?\s*(\d+(?:[.,]\d+)?)', page_text, re.IGNORECASE)
                        if fat_match:
                            nutrition_data['fat_100g'] = float(fat_match.group(1).replace(',', '.'))
                        
                        # Ищем углеводы в тексте
                        carb_match = re.search(r'углевод[аы]?\s*(\d+(?:[.,]\d+)?)', page_text, re.IGNORECASE)
                        if carb_match:
                            nutrition_data['carb_100g'] = float(carb_match.group(1).replace(',', '.'))
                except:
                    pass
            
        except Exception as e:
            self.logger.error(f"Ошибка извлечения пищевой ценности: {e}")
        
        return nutrition_data
    
    async def _extract_detailed_composition(self) -> str:
        """Извлечь детальный состав"""
        try:
            composition_selectors = [
                '.composition', '.ingredients', '.product-composition',
                '.ingredient-list', '.product-ingredients', '[class*="composition"]',
                '[class*="ingredient"]', '.product-description'
            ]
            
            for selector in composition_selectors:
                try:
                    comp_elem = await self.page.query_selector(selector)
                    if comp_elem:
                        comp_text = await comp_elem.text_content()
                        if comp_text and len(comp_text.strip()) > 10:
                            return comp_text.strip()[:500]
                except:
                    continue
        except:
            pass
        
        return ""
    
    async def _extract_detailed_portion(self) -> Optional[float]:
        """Извлечь детальный вес порции"""
        try:
            weight_selectors = [
                '.weight', '.portion', '.product-weight', '.product-portion',
                '[class*="weight"]', '[class*="portion"]', '.size', '.volume'
            ]
            
            for selector in weight_selectors:
                try:
                    weight_elem = await self.page.query_selector(selector)
                    if weight_elem:
                        weight_text = await weight_elem.text_content()
                        if weight_text:
                            # Ищем число с единицами измерения
                            weight_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:г|гр|g|кг|kg|мл|ml|л|l)', weight_text, re.IGNORECASE)
                            if weight_match:
                                weight_value = float(weight_match.group(1).replace(',', '.'))
                                unit = weight_match.group(0).lower()
                                
                                # Конвертируем в граммы
                                if 'кг' in unit or 'kg' in unit:
                                    weight_value *= 1000
                                elif 'л' in unit or 'l' in unit:
                                    weight_value *= 1000  # Приблизительно для жидкостей
                                elif 'мл' in unit or 'ml' in unit:
                                    pass  # Уже в граммах
                                
                                return weight_value
                except:
                    continue
        except:
            pass
        
        return None
    
    async def _extract_brand(self) -> str:
        """Извлечь бренд"""
        try:
            brand_selectors = [
                '.brand', '.manufacturer', '.producer', '.product-brand',
                '[class*="brand"]', '[class*="manufacturer"]'
            ]
            
            for selector in brand_selectors:
                try:
                    brand_elem = await self.page.query_selector(selector)
                    if brand_elem:
                        brand_text = await brand_elem.text_content()
                        if brand_text and len(brand_text.strip()) > 2:
                            return brand_text.strip()[:100]
                except:
                    continue
        except:
            pass
        
        return ""
    
    async def _extract_allergens(self) -> List[str]:
        """Извлечь аллергены"""
        try:
            allergen_selectors = [
                '.allergens', '.allergen-info', '.product-allergens',
                '[class*="allergen"]', '.warning', '.contains'
            ]
            
            for selector in allergen_selectors:
                try:
                    allergen_elem = await self.page.query_selector(selector)
                    if allergen_elem:
                        allergen_text = await allergen_elem.text_content()
                        if allergen_text:
                            # Разбиваем на список аллергенов
                            allergens = [a.strip() for a in allergen_text.split(',') if a.strip()]
                            return allergens[:10]  # Ограничиваем количество
                except:
                    continue
        except:
            pass
        
        return []
            
    def _extract_price(self, price_text: str) -> Optional[float]:
        """Извлечь цену из текста"""
        if not price_text:
            return None
            
        # Убираем лишние символы и извлекаем число
        price_match = re.search(r'(\d+(?:[.,]\d+)?)', price_text.replace(' ', ''))
        if price_match:
            return float(price_match.group(1).replace(',', '.'))
        return None
        
    async def _extract_product_id(self, url: str, element) -> str:
        """Извлечь ID продукта"""
        try:
            # Пробуем извлечь из URL
            if url:
                url_parts = urlparse(url).path.split('/')
                for part in url_parts:
                    if part and part.isdigit():
                        return f"samokat:{part}"
                        
            # Пробуем извлечь из data-атрибутов
            try:
                data_id = await element.get_attribute('data-id')
                if data_id:
                    return f"samokat:{data_id}"
            except:
                pass
                
            # Пробуем извлечь из href
            try:
                href = await element.get_attribute('href')
                if href:
                    href_parts = href.split('/')
                    for part in href_parts:
                        if part and part.isdigit():
                            return f"samokat:{part}"
            except:
                pass
                
        except:
            pass
            
        # Генерируем уникальный ID
        import uuid
        return f"samokat:{str(uuid.uuid4())[:8]}"

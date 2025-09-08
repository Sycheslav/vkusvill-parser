"""
Скрейпер для ВкусВилла (vkusvill.ru)
"""
import re
import logging
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


class VkusvillScraper(BaseScraper):
    """Скрейпер для ВкусВилла"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = "https://vkusvill.ru"
        self.city = config.get('city', 'Москва')
        self.coords = config.get('coords')
        
    async def setup_location(self):
        """Настройка локации для получения доступной витрины"""
        try:
            self.logger.info(f"[{self.__class__.__name__}] setup_location вызван")
            self.logger.info(f"[{self.__class__.__name__}] Сайт ВкусВилл медленно загружается, используем заглушку")
            
            # Пропускаем загрузку страницы для ускорения
            self.logger.info(f"Страница пропущена для {self.city}")
            await self.random_delay(1, 2)  # Уменьшаем задержку
            
        except Exception as e:
            self.logger.error(f"Ошибка настройки локации: {e}")
            raise
            
    async def get_categories(self) -> List[str]:
        """Получить список доступных категорий готовой еды"""
        try:
            self.logger.info(f"[{self.__class__.__name__}] get_categories вызван")
            
            # Возвращаем реальные категории ВкусВилла
            categories = [
                'Хаб «Готовая еда»',
                'Вторые блюда',
                'Салаты',
                'Супы',
                'Завтраки',
                'Закуски',
                'Сэндвичи, шаурма и бургеры',
                'Роллы и сеты',
                'Онигири',
                'Пироги, пирожки и лепёшки',
                'Кухни мира',
                'Привезём горячим',
                'Окрошки и летние супы',
                'Веганские и постные блюда',
                'Больше белка, меньше калорий',
                'Тарелка здорового питания',
                'Новинки'
            ]
            
            self.logger.info(f"[{self.__class__.__name__}] Возвращаем {len(categories)} категорий")
            return categories
            
        except Exception as e:
            self.logger.error(f"Ошибка получения категорий: {e}")
            return ['Хаб «Готовая еда»', 'Вторые блюда', 'Салаты', 'Супы']
            
    async def scrape_category(self, category: str, limit: int = None) -> List[ScrapedProduct]:
        """Скрапить продукты из указанной категории"""
        try:
            self.logger.info(f"[{self.__class__.__name__}] scrape_category вызван для категории: {category}")
            
            # Убеждаемся, что браузер готов
            await self._ensure_browser_ready()
            
            # Настраиваем локацию
            await self.setup_location()
            
            # Переходим на страницу категории с правильными URL
            category_urls = {
                'Хаб «Готовая еда»': 'https://vkusvill.ru/goods/gotovaya-eda/',
                'Вторые блюда': 'https://vkusvill.ru/goods/gotovaya-eda/vtorye-blyuda/',
                'Салаты': 'https://vkusvill.ru/goods/gotovaya-eda/salaty/',
                'Супы': 'https://vkusvill.ru/goods/gotovaya-eda/supy/',
                'Завтраки': 'https://vkusvill.ru/goods/gotovaya-eda/zavtraki/',
                'Закуски': 'https://vkusvill.ru/goods/gotovaya-eda/zakuski/',
                'Сэндвичи, шаурма и бургеры': 'https://vkusvill.ru/goods/gotovaya-eda/sendvichi-shaurma-i-burgery/',
                'Роллы и сеты': 'https://vkusvill.ru/goods/gotovaya-eda/rolly-i-sety/',
                'Онигири': 'https://vkusvill.ru/goods/gotovaya-eda/onigiri/',
                'Пироги, пирожки и лепёшки': 'https://vkusvill.ru/goods/gotovaya-eda/pirogi-pirozhki-i-lepeshki/',
                'Кухни мира': 'https://vkusvill.ru/goods/gotovaya-eda/kukhni-mira/',
                'Привезём горячим': 'https://vkusvill.ru/goods/gotovaya-eda/privezem-goryachim/',
                'Окрошки и летние супы': 'https://vkusvill.ru/goods/gotovaya-eda/okroshki-i-letnie-supy/',
                'Веганские и постные блюда': 'https://vkusvill.ru/goods/gotovaya-eda/veganskie-i-postnye-blyuda/',
                'Больше белка, меньше калорий': 'https://vkusvill.ru/goods/gotovaya-eda/bolshe-belka-menshe-kaloriy/',
                'Тарелка здорового питания': 'https://vkusvill.ru/goods/gotovaya-eda/tarelka-zdorovogo-pitaniya/',
                'Новинки': 'https://vkusvill.ru/goods/gotovaya-eda/novinki/'
            }
            
            category_url = category_urls.get(category, 'https://vkusvill.ru/goods/gotovaya-eda/')
            self.logger.info(f"[{self.__class__.__name__}] Переходим на {category_url}")
            
            try:
                await self.page.goto(category_url, timeout=20000)
                await self.page.wait_for_load_state("domcontentloaded", timeout=15000)
                await asyncio.sleep(2)  # Уменьшаем время ожидания
                
                # Быстрая загрузка контента
                await self.page.wait_for_load_state("networkidle", timeout=15000)
                await asyncio.sleep(1)
                
                # Прокручиваем страницу для загрузки товаров
                target_limit = limit or 1000
                await self._scroll_page_for_more_products(target_limit)
                
                # Дополнительная прокрутка для загрузки большего количества товаров
                await asyncio.sleep(2)
                await self.page.evaluate("window.scrollTo(0, 0)")  # Прокручиваем в начало
                await asyncio.sleep(1)
                await self._scroll_page_for_more_products(target_limit)  # Еще больше прокрутки
                
                # Третья волна прокрутки для гарантированного получения 1000 товаров
                await asyncio.sleep(2)
                await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")  # Прокручиваем к середине
                await asyncio.sleep(1)
                await self._scroll_page_for_more_products(target_limit)
                
            except Exception as e:
                self.logger.warning(f"[{self.__class__.__name__}] Не удалось загрузить страницу категории: {e}")
                # Пробуем альтернативный URL
                try:
                    await self.page.goto('https://vkusvill.ru/goods/gotovaya-eda/', timeout=30000)
                    await self.page.wait_for_load_state("domcontentloaded", timeout=30000)
                    await asyncio.sleep(5)
                    await self.page.wait_for_load_state("networkidle", timeout=30000)
                    await asyncio.sleep(3)
                except Exception as e2:
                    self.logger.error(f"[{self.__class__.__name__}] Не удалось загрузить альтернативную страницу: {e2}")
                    return []
            
            # Ищем карточки товаров - расширенные селекторы для ВкусВилла
            product_selectors = [
                # Основные селекторы ВкусВилла
                '[data-testid="product-card"]', '.ProductCard', '.product-card',
                '.ProductItem', '.product-item', '.Product',
                # Селекторы каталога ВкусВилла
                '.CatalogItem', '.catalog-item', '.ItemCard',
                '.ProductGrid > *', '.product-grid > *',
                '.ProductList > *', '.product-list > *',
                # Специфичные селекторы ВкусВилла
                '.GoodsItem', '.goods-item', '.GoodsCard',
                '.CatalogGrid > *', '.catalog-grid > *',
                '.ProductCatalog > *', '.product-catalog > *',
                '.ItemGrid > *', '.item-grid > *',
                '.CardGrid > *', '.card-grid > *',
                # Общие селекторы
                'article[data-testid]', 'article[class*="product"]',
                '[data-product-id]', '[data-testid*="product"]',
                '.item[class*="product"]', '.card[class*="product"]',
                # Дополнительные селекторы
                'div[class*="Product"]', 'div[class*="Item"]',
                'div[class*="Goods"]', 'div[class*="Catalog"]',
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
                '[class*="desktop"] [class*="card"]', '[class*="desktop"] article'
            ]
            
            products = []
            total_found = 0
            
            for selector in product_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    if elements:
                        self.logger.info(f"[{self.__class__.__name__}] Найдено {len(elements)} элементов с селектором {selector}")
                        total_found = len(elements)
                        
                        # Обрабатываем больше товаров для достижения лимита
                        target_limit = limit or 1000  # Увеличиваем лимит до 1000
                        # Берем все найденные элементы, не ограничиваемся лимитом
                        elements_to_process = elements  # Обрабатываем все найденные элементы
                        
                        for i, element in enumerate(elements_to_process):
                            try:
                                # Быстрое извлечение без детального парсинга
                                product = await self._extract_product_fast(element, category)
                                if product:
                                    products.append(product)
                                    
                                    # Логируем прогресс каждые 50 товаров
                                    if len(products) % 50 == 0:
                                        self.logger.info(f"[{self.__class__.__name__}] Обработано {len(products)} товаров...")
                                
                                # Останавливаемся при достижении лимита
                                if len(products) >= target_limit:
                                    break
                                    
                            except Exception as e:
                                # Игнорируем ошибки отдельных товаров
                                continue
                        
                        # Продолжаем поиск с другими селекторами для нахождения большего количества товаров
                        if len(products) >= target_limit:
                            break  # Останавливаемся только при достижении лимита
                except Exception as e:
                    self.logger.debug(f"[{self.__class__.__name__}] Ошибка с селектором {selector}: {e}")
                    continue
            
            # Возвращаем только реальные товары, не создаем тестовые
            if not products:
                self.logger.warning(f"[{self.__class__.__name__}] Не найдено реальных товаров для категории {category}")
                return []
            
            self.logger.info(f"[{self.__class__.__name__}] Найдено {len(products)} реальных товаров")
            
            self.logger.info(f"[{self.__class__.__name__}] Найдено товаров: {len(products)}")
            return products
            
        except Exception as e:
            self.logger.error(f"Ошибка скрапинга категории {category}: {e}")
            return []
            
    async def _extract_product_from_card(self, element, category: str) -> Optional[ScrapedProduct]:
        """Извлечь данные продукта из карточки товара"""
        try:
            if not element:
                self.logger.warning("Элемент карточки товара не передан")
                return None
                
            # Основная информация
            name_elem = await element.query_selector('.product-name, .item-name, .title, h3, h4, .product-title')
            name = await name_elem.text_content() if name_elem else "Неизвестный товар"
            name = name.strip() if name else "Неизвестный товар"
            
            # Цена
            price_elem = await element.query_selector('.price, .cost, .item-price, [data-price], .product-price')
            price_text = await price_elem.text_content() if price_elem else "0"
            price = self._extract_price(price_text)
            
            # URL товара - расширенные селекторы
            link_selectors = [
                'a[href]', '[href]', 'a', 
                '.product-link', '.ProductLink', '.item-link', '.ItemLink',
                '.card-link', '.CardLink', '.product-card a', '.ProductCard a',
                '.product-item a', '.ProductItem a', '.catalog-item a', '.CatalogItem a',
                '.goods-item a', '.GoodsItem a', '.goods-card a', '.GoodsCard a',
                '[data-href]', '[data-url]', '[data-link]',
                'a[class*="product"]', 'a[class*="item"]', 'a[class*="card"]',
                'a[class*="Product"]', 'a[class*="Item"]', 'a[class*="Card"]',
                'a[class*="goods"]', 'a[class*="Goods"]', 'a[class*="catalog"]', 'a[class*="Catalog"]'
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
                                break
                            else:
                                url = ""  # Сбрасываем если не похоже на ссылку товара
                except:
                    continue
                
            # Изображение
            img_elem = await element.query_selector('img[src], img[data-src]')
            image_url = ""
            if img_elem:
                try:
                    image_url = await img_elem.get_attribute('src') or await img_elem.get_attribute('data-src') or ""
                except:
                    image_url = ""
            if image_url and not image_url.startswith('http'):
                image_url = urljoin(self.base_url, image_url)
                
            # ID товара (из URL или data-атрибута)
            product_id = self._extract_product_id(url, element)
            
            # Создаем базовый продукт
            product = ScrapedProduct(
                id=product_id,
                name=name,
                category=category,
                price=price,
                url=url,
                image_url=image_url,
                shop="vkusvill",
                available=True
            )
            
            # Пропускаем детальный парсинг для ускорения
            pass
                    
            return product
            
        except Exception as e:
            self.logger.error(f"Ошибка извлечения продукта: {e}")
            return None
            
    async def _extract_product_fast(self, element, category: str) -> Optional[ScrapedProduct]:
        """Быстрое извлечение данных продукта без детального парсинга"""
        try:
            if not element:
                return None
                
            # Извлекаем название товара
            name = "Неизвестный товар"
            try:
                # Пробуем разные селекторы для названия
                name_selectors = [
                    '.ProductCard__title', '.ProductCard__name', '.product-title',
                    '.ProductItem__title', '.ProductItem__name', '.item-title',
                    '.GoodsItem__title', '.GoodsItem__name', '.goods-title',
                    '.CatalogItem__title', '.CatalogItem__name', '.catalog-title',
                    'h3', 'h4', '.title', '[class*="title"]', '[class*="name"]'
                ]
                
                for selector in name_selectors:
                    name_elem = await element.query_selector(selector)
                    if name_elem:
                        name_text = await name_elem.text_content()
                        if name_text and len(name_text.strip()) > 3:
                            name = name_text.strip()[:100]
                            break
            except:
                pass
            
            # Пропускаем товары с фейковыми названиями
            if "Товар" in name and "из" in name:
                return None
            
            # Извлекаем цену
            price = None
            try:
                price_selectors = [
                    '.ProductCard__price', '.ProductCard__cost', '.product-price',
                    '.ProductItem__price', '.ProductItem__cost', '.item-price',
                    '.GoodsItem__price', '.GoodsItem__cost', '.goods-price',
                    '.CatalogItem__price', '.CatalogItem__cost', '.catalog-price',
                    '.price', '.cost', '[data-price]', '[class*="price"]'
                ]
                
                for selector in price_selectors:
                    price_elem = await element.query_selector(selector)
                    if price_elem:
                        price_text = await price_elem.text_content()
                        if price_text:
                            price = self._extract_price(price_text)
                            if price and price > 0:
                                break
            except:
                pass
            
            # Извлекаем URL товара - максимально агрессивные селекторы
            url = ""
            
            # Метод 1: Поиск по всем возможным селекторам ссылок
            link_selectors = [
                'a[href]', '[href]', 'a', 
                '.product-link', '.ProductLink', '.item-link', '.ItemLink',
                '.card-link', '.CardLink', '.product-card a', '.ProductCard a',
                '.product-item a', '.ProductItem a', '.catalog-item a', '.CatalogItem a',
                '.goods-item a', '.GoodsItem a', '.goods-card a', '.GoodsCard a',
                '[data-href]', '[data-url]', '[data-link]', '[data-product-url]',
                'a[class*="product"]', 'a[class*="item"]', 'a[class*="card"]',
                'a[class*="Product"]', 'a[class*="Item"]', 'a[class*="Card"]',
                'a[class*="goods"]', 'a[class*="Goods"]', 'a[class*="catalog"]', 'a[class*="Catalog"]',
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
                            # Ищем URL в onclick (например: "window.location='/goods/123'")
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
                            url = f"{self.base_url}/goods/{product_id.strip()}.html"
                            break
                except:
                    pass
            
            # Извлекаем изображение
            image_url = ""
            try:
                img_selectors = [
                    '.ProductCard__image img', '.ProductCard__photo img', '.product-image img',
                    '.ProductItem__image img', '.ProductItem__photo img', '.item-image img',
                    '.GoodsItem__image img', '.GoodsItem__photo img', '.goods-image img',
                    '.CatalogItem__image img', '.CatalogItem__photo img', '.catalog-image img',
                    'img[src]', 'img[data-src]', 'img[data-lazy]'
                ]
                
                for selector in img_selectors:
                    img_elem = await element.query_selector(selector)
                    if img_elem:
                        image_url = await img_elem.get_attribute('src') or await img_elem.get_attribute('data-src') or await img_elem.get_attribute('data-lazy') or ""
                        if image_url and not image_url.startswith('http'):
                            image_url = urljoin(self.base_url, image_url)
                        if image_url:
                            break
            except:
                pass
            
            # Извлекаем состав/описание
            composition = ""
            try:
                comp_selectors = [
                    '.ProductCard__description', '.ProductCard__composition', '.product-description',
                    '.ProductItem__description', '.ProductItem__composition', '.item-description',
                    '.GoodsItem__description', '.GoodsItem__composition', '.goods-description',
                    '.CatalogItem__description', '.CatalogItem__composition', '.catalog-description',
                    '.description', '.composition', '[class*="description"]', '[class*="composition"]'
                ]
                
                for selector in comp_selectors:
                    comp_elem = await element.query_selector(selector)
                    if comp_elem:
                        comp_text = await comp_elem.text_content()
                        if comp_text and len(comp_text.strip()) > 5:
                            composition = comp_text.strip()[:200]
                            break
            except:
                pass
            
            # Извлекаем вес/порцию
            portion_g = None
            try:
                weight_selectors = [
                    '.ProductCard__weight', '.ProductCard__portion', '.product-weight',
                    '.ProductItem__weight', '.ProductItem__portion', '.item-weight',
                    '.GoodsItem__weight', '.GoodsItem__portion', '.goods-weight',
                    '.CatalogItem__weight', '.CatalogItem__portion', '.catalog-weight',
                    '.weight', '.portion', '[class*="weight"]', '[class*="portion"]'
                ]
                
                for selector in weight_selectors:
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
                pass
            
            # Генерируем ID из URL или названия
            product_id = f"vkusvill_{hash(name + str(price))}"
            if url:
                url_parts = urlparse(url).path.split('/')
                for part in url_parts:
                    if part and part.isdigit():
                        product_id = f"vkusvill_{part}"
                        break
            
            # Создаем продукт только если есть реальные данные
            if name != "Неизвестный товар" and (price or url):
                product = ScrapedProduct(
                    id=product_id,
                    name=name,
                    category=category,
                    price=price,
                    url=url,
                    shop="vkusvill",
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
                id=f"vkusvill_detailed_{hash(url)}",
                name="",  # Будет заполнено из базовой информации
                category="",  # Будет заполнено из базовой информации
                kcal_100g=nutrition_data.get('kcal_100g'),
                protein_100g=nutrition_data.get('protein_100g'),
                fat_100g=nutrition_data.get('fat_100g'),
                carb_100g=nutrition_data.get('carb_100g'),
                portion_g=portion_g,
                composition=composition,
                url=url,
                shop="vkusvill"
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
            # Селекторы для пищевой ценности ВкусВилла
            nutrition_selectors = [
                '.nutrition-table', '.nutrition-info', '.nutrition-facts',
                '.product-nutrition', '.nutritional-info', '.nutrition',
                '[class*="nutrition"]', '[class*="calorie"]', '[class*="protein"]',
                '.kcal', '.calories', '.energy', '.protein', '.fat', '.carb',
                '.nutritional-value', '.nutritional-values', '.product-nutritional'
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
        
    def _extract_product_id(self, url: str, element) -> str:
        """Извлечь ID продукта"""
        try:
            # Пробуем извлечь из URL
            if url:
                url_parts = urlparse(url).path.split('/')
                for part in url_parts:
                    if part and part.isdigit():
                        return f"vkusvill:{part}"
                        
            # Пробуем извлечь из data-атрибутов
            data_id = asyncio.run(element.get_attribute('data-id'))
            if data_id:
                return f"vkusvill:{data_id}"
                
            # Пробуем извлечь из href
            href = asyncio.run(element.get_attribute('href'))
            if href:
                href_parts = href.split('/')
                for part in href_parts:
                    if part and part.isdigit():
                        return f"vkusvill:{part}"
                        
        except:
            pass
            
        # Генерируем уникальный ID
        import uuid
        return f"vkusvill:{str(uuid.uuid4())[:8]}"

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
            
            # URL товара
            link_elem = await element.query_selector('a[href]')
            url = ""
            if link_elem:
                try:
                    url = await link_elem.get_attribute('href') or ""
                except:
                    url = ""
            if url and not url.startswith('http'):
                url = urljoin(self.base_url, url)
                
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
            
            # Извлекаем URL товара
            url = ""
            try:
                link_elem = await element.query_selector('a[href]')
                if link_elem:
                    url = await link_elem.get_attribute('href') or ""
                    if url and not url.startswith('http'):
                        url = urljoin(self.base_url, url)
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
                
                return product
            
            return None
            
        except Exception as e:
            # Игнорируем ошибки для ускорения
            return None
            
    async def scrape_product_page(self, url: str) -> Optional[ScrapedProduct]:
        """Скрапить детальную страницу продукта - отключено для ускорения"""
        try:
            self.logger.info(f"[{self.__class__.__name__}] scrape_product_page отключен для ускорения")
            return None
            
        except Exception as e:
            self.logger.error(f"Ошибка в отключенном scrape_product_page: {e}")
            return None
            
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

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
                await self.page.goto(category_url, timeout=30000)
                await self.page.wait_for_load_state("domcontentloaded", timeout=30000)
                await asyncio.sleep(5)  # Увеличиваем время ожидания JavaScript
                
                # Дополнительно ждем загрузки контента
                await self.page.wait_for_load_state("networkidle", timeout=30000)
                await asyncio.sleep(3)
                
                # Прокручиваем страницу для загрузки большего количества товаров
                target_limit = limit or 500
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
                '.product-card', '.product-item', '.product',
                '[data-product-id]', '[class*="product"]',
                '.catalog-item', '.item-card',
                '.product-list > *', '.products > *', '.items > *',
                'article', '.item', '.card', '.product-tile',
                '[class*="catalog"]', '[class*="item"]', '[class*="card"]',
                '.catalog-grid > *', '.product-catalog > *', '.goods > *',
                '.product-grid > *', '.item-grid > *', '.card-grid > *'
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
                        target_limit = limit or 200  # Увеличиваем лимит
                        elements_to_process = elements[:target_limit]
                        
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
            
            if not products:
                self.logger.warning(f"[{self.__class__.__name__}] Не найдено товаров для категории {category}")
                # Возвращаем пустой список вместо заглушки
                products = []
            
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
                
            # Быстрое извлечение названия
            name = "Неизвестный товар"
            try:
                name_elem = await element.query_selector('.product-name, .title, h3, h4, [class*="name"]')
                if name_elem:
                    name_text = await name_elem.text_content()
                    if name_text and len(name_text.strip()) > 3:
                        name = name_text.strip()[:100]  # Ограничиваем длину
            except:
                pass
            
            # Быстрое извлечение цены
            price = 0.0
            try:
                price_elem = await element.query_selector('.price, [data-price], [class*="price"]')
                if price_elem:
                    price_text = await price_elem.text_content()
                    if price_text:
                        price = self._extract_price(price_text)
            except:
                pass
            
            # Быстрое извлечение URL
            url = ""
            try:
                link_elem = await element.query_selector('a[href]')
                if link_elem:
                    url = await link_elem.get_attribute('href') or ""
                    if url and not url.startswith('http'):
                        url = urljoin(self.base_url, url)
            except:
                pass
            
            # Быстрое извлечение изображения
            image_url = ""
            try:
                img_elem = await element.query_selector('img[src]')
                if img_elem:
                    image_url = await img_elem.get_attribute('src') or ""
                    if image_url and not image_url.startswith('http'):
                        image_url = urljoin(self.base_url, image_url)
            except:
                pass
            
            # Генерируем ID если не найден
            product_id = f"vkusvill_{category}_{hash(name)}"
            
            # Создаем продукт
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
            
            return product
            
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

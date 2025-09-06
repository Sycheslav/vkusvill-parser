"""
Скрейпер для Яндекс.Лавки (lavka.yandex.ru) - исправленная версия
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


class LavkaScraper(BaseScraper):
    """Скрейпер для Яндекс.Лавки"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = "https://lavka.yandex.ru"
        self.city = config.get('city', 'Москва')
        self.coords = config.get('coords')
        
    async def setup_location(self):
        """Настройка локации для получения доступной витрины с обходом блокировки и капчи"""
        try:
            self.logger.info(f"[{self.__class__.__name__}] setup_location вызван")
            
            # Убеждаемся, что браузер готов
            await self._ensure_browser_ready()
            
            # Устанавливаем более продвинутые заголовки для обхода блокировки
            await self.page.set_extra_http_headers({
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0',
                'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"'
            })
            
            # Добавляем скрипт для обхода детекции автоматизации
            await self.page.add_init_script("""
                // Скрываем автоматизацию
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['ru-RU', 'ru', 'en-US', 'en'] });
                
                // Эмулируем человеческое поведение
                const originalQuerySelector = document.querySelector;
                document.querySelector = function(selector) {
                    if (selector.includes('captcha') || selector.includes('robot')) {
                        return null; // Скрываем элементы капчи
                    }
                    return originalQuerySelector.call(this, selector);
                };
                
                // Скрываем элементы капчи
                const hideCaptcha = () => {
                    const captchaElements = document.querySelectorAll('[class*="captcha"], [class*="robot"], [class*="challenge"]');
                    captchaElements.forEach(el => {
                        el.style.display = 'none';
                        el.style.visibility = 'hidden';
                        el.style.opacity = '0';
                    });
                };
                
                // Запускаем скрытие капчи
                setInterval(hideCaptcha, 1000);
                hideCaptcha();
            """)
            
            # Переходим на главную страницу
            await self.page.goto(self.base_url, timeout=30000)
            await self.page.wait_for_load_state("domcontentloaded", timeout=30000)
            await asyncio.sleep(3)
            
            # Проверяем наличие капчи и пытаемся её обойти
            captcha_selectors = [
                '[class*="captcha"]', '[class*="robot"]', '[class*="challenge"]',
                'iframe[src*="captcha"]', 'iframe[src*="robot"]', 'iframe[src*="challenge"]'
            ]
            
            for selector in captcha_selectors:
                try:
                    captcha_element = await self.page.query_selector(selector)
                    if captcha_element:
                        self.logger.info(f"Найдена капча: {selector}, пытаемся скрыть")
                        await self.page.evaluate(f"""
                            const elements = document.querySelectorAll('{selector}');
                            elements.forEach(el => {{
                                el.style.display = 'none';
                                el.style.visibility = 'hidden';
                                el.style.opacity = '0';
                            }});
                        """)
                except:
                    continue
            
            # Пытаемся найти и кликнуть по кнопке "Еда" или "Готовая еда"
            food_selectors = [
                'a[href*="eda"]', 'a[href*="food"]', 'a[href*="catalog"]',
                'button:has-text("Еда")', 'button:has-text("Готовая еда")',
                '[data-testid="food-button"]', '.food-button', '.eda-button'
            ]
            
            for selector in food_selectors:
                try:
                    food_button = await self.page.query_selector(selector)
                    if food_button:
                        await food_button.click()
                        await asyncio.sleep(2)
                        self.logger.info(f"Найдена и нажата кнопка еды: {selector}")
                        break
                except:
                    continue
            
            self.logger.info(f"Локация настроена для {self.city}")
            
        except Exception as e:
            self.logger.error(f"Ошибка настройки локации: {e}")
            # Не прерываем выполнение, продолжаем без настройки локации
            
    async def get_categories(self) -> List[str]:
        """Получить список доступных категорий готовой еды"""
        try:
            self.logger.info(f"[{self.__class__.__name__}] get_categories вызван")
            
            # Возвращаем реальные категории Лавки
            categories = [
                'Хаб «Готовая еда»',
                'Основное меню',
                'Салаты и закуски',
                'Супы и вторые блюда',
                'Есть горячее',
                'Придумали вместе с ресторанами',
                'Новинки',
                'Здоровый рацион'
            ]
            
            self.logger.info(f"[{self.__class__.__name__}] Возвращаем {len(categories)} категорий")
            return categories
            
        except Exception as e:
            self.logger.error(f"Ошибка получения категорий: {e}")
            return ['Хаб «Готовая еда»', 'Салаты и закуски', 'Супы и вторые блюда']
            
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
                'Хаб «Готовая еда»': 'https://lavka.yandex.ru/catalog/ready_to_eat',
                'Основное меню': 'https://lavka.yandex.ru/category/gotovaya_eda',
                'Салаты и закуски': 'https://lavka.yandex.ru/catalog/ready_to_eat/category/gotovaya_eda',
                'Супы и вторые блюда': 'https://lavka.yandex.ru/catalog/ready_to_eat',
                'Есть горячее': 'https://lavka.yandex.ru/catalog/ready_to_eat/category/hot_streetfood',
                'Придумали вместе с ресторанами': 'https://lavka.yandex.ru/catalog/ready_to_eat/category/from_restaurants',
                'Новинки': 'https://lavka.yandex.ru/catalog/ready_to_eat/category/new_goods',
                'Здоровый рацион': 'https://lavka.yandex.ru/catalog/ready_to_eat/category/healthydiet'
            }
            
            category_url = category_urls.get(category, 'https://lavka.yandex.ru/catalog/ready_to_eat')
            self.logger.info(f"[{self.__class__.__name__}] Переходим на {category_url}")
            
            try:
                await self.page.goto(category_url, timeout=30000)
                await self.page.wait_for_load_state("domcontentloaded", timeout=30000)
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
                    await self.page.goto('https://lavka.yandex.ru/catalog/ready_to_eat', timeout=30000)
                    await self.page.wait_for_load_state("domcontentloaded", timeout=30000)
                    await asyncio.sleep(5)
                    await self.page.wait_for_load_state("networkidle", timeout=30000)
                    await asyncio.sleep(3)
                except Exception as e2:
                    self.logger.error(f"[{self.__class__.__name__}] Не удалось загрузить альтернативную страницу: {e2}")
                    return []
            
            # Ищем карточки товаров - расширенные селекторы для Яндекс.Лавки
            product_selectors = [
                # Основные селекторы Лавки
                '[data-testid="product-card"]', '.ProductCard', '.product-card',
                '.ProductItem', '.product-item', '.Product',
                # Селекторы каталога Лавки
                '.CatalogItem', '.catalog-item', '.ItemCard',
                '.ProductGrid > *', '.product-grid > *',
                '.ProductList > *', '.product-list > *',
                # Специфичные селекторы Лавки
                '.GoodsItem', '.goods-item', '.GoodsCard',
                '.CatalogGrid > *', '.catalog-grid > *',
                '.ProductCatalog > *', '.product-catalog > *',
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
            
            # Пропускаем товары с фейковыми названиями
            if "Товар" in name and "из" in name:
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
            product_id = f"lavka_{hash(name + str(price))}"
            if url:
                url_parts = urlparse(url).path.split('/')
                for part in url_parts:
                    if part and part.isdigit():
                        product_id = f"lavka_{part}"
                        break
            
            # Создаем продукт только если есть реальные данные
            if name != "Неизвестный товар" and (price or url):
                product = ScrapedProduct(
                    id=product_id,
                    name=name,
                    category=category,
                    price=price,
                    url=url,
                    shop="lavka",
                    composition=composition,
                    portion_g=portion_g
                )
                
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
                                break
                except:
                    continue
            
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
            
            # ID товара
            product_id = self._extract_product_id(url, element)
            
            # Создаем продукт
            product = ScrapedProduct(
                id=product_id,
                name=name,
                category=category,
                price=price,
                url=url,
                image_url=image_url,
                shop="lavka",
                available=True
            )
            
            return product
            
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Ошибка извлечения продукта: {e}")
            return None
            
    async def scrape_product_page(self, url: str) -> Optional[ScrapedProduct]:
        """Скрапить детальную страницу продукта - отключено для ускорения"""
        try:
            self.logger.info(f"[{self.__class__.__name__}] scrape_product_page отключен для ускорения")
            return None
            
        except Exception as e:
            self.logger.error(f"Ошибка в отключенном scrape_product_page: {e}")
            return None
            
    def _extract_price(self, price_text: str) -> float:
        """Извлечь цену из текста"""
        try:
            # Убираем все символы кроме цифр и точки
            price_str = re.sub(r'[^\d.,]', '', price_text)
            # Заменяем запятую на точку
            price_str = price_str.replace(',', '.')
            return float(price_str) if price_str else 0.0
        except:
            return 0.0
            
    def _extract_product_id(self, url: str, element) -> str:
        """Извлечь ID продукта из URL или элемента"""
        try:
            if url:
                # Пытаемся извлечь ID из URL
                parsed = urlparse(url)
                path_parts = parsed.path.strip('/').split('/')
                if path_parts:
                    return path_parts[-1]
            return f"lavka_{int(asyncio.get_event_loop().time())}"
        except:
            return f"lavka_{int(asyncio.get_event_loop().time())}"

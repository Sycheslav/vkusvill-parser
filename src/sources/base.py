"""
Базовый класс для всех скрейперов
"""
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

# Убираем импорты, чтобы избежать циклических зависимостей
# DataNormalizer и DataStorage будут импортироваться лениво при необходимости


@dataclass
class ScrapedProduct:
    """Структура данных для собранного продукта"""
    id: str
    name: str
    category: str
    kcal_100g: Optional[float] = None
    protein_100g: Optional[float] = None
    fat_100g: Optional[float] = None
    carb_100g: Optional[float] = None
    portion_g: Optional[float] = None
    price: Optional[float] = None
    shop: str = ""
    tags: List[str] = None
    composition: str = ""
    url: str = ""

    def __post_init__(self):
        if self.tags is None:
            self.tags = []


class BaseScraper(ABC):
    """Базовый класс для всех скрейперов"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        # self.normalizer = DataNormalizer() # Удалено
        # self.storage = DataStorage() # Удалено
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        
        # Настройки по умолчанию
        self.headless = config.get('headless', True)
        self.max_concurrent = config.get('max_concurrent', 2)
        self.throttle_min = config.get('throttle_min', 1.0)
        self.throttle_max = config.get('throttle_max', 2.0)
        
    async def __aenter__(self):
        """Асинхронный контекстный менеджер - вход"""
        try:
            self.logger.info(f"[{self.__class__.__name__}] Вход в контекстный менеджер")
            await self.setup_browser()
            self.logger.info(f"[{self.__class__.__name__}] setup_browser завершен. Page: {self.page}")
            
            if not self.page:
                self.logger.error(f"[{self.__class__.__name__}] Страница не инициализирована!")
                raise Exception("Не удалось инициализировать страницу браузера")
                
            self.logger.info(f"[{self.__class__.__name__}] Контекстный менеджер успешно инициализирован")
            return self
            
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Ошибка инициализации браузера: {e}")
            self.logger.error(f"[{self.__class__.__name__}] Traceback: ", exc_info=True)
            raise
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Асинхронный контекстный менеджер - выход"""
        await self.cleanup()
        
    async def setup_browser(self):
        """Настройка браузера Playwright"""
        try:
            self.logger.info(f"[{self.__class__.__name__}] Начинаем настройку браузера...")
            
            self.playwright = await async_playwright().start()
            self.logger.info(f"[{self.__class__.__name__}] Playwright запущен")
            
            # Используем WebKit на macOS (более стабильный)
            if hasattr(self, 'playwright'):
                try:
                    self.browser = await self.playwright.webkit.launch(
                        headless=self.headless
                    )
                    self.logger.info(f"[{self.__class__.__name__}] WebKit браузер запущен")
                except Exception as e:
                    self.logger.warning(f"[{self.__class__.__name__}] WebKit не удался, пробуем Chromium: {e}")
                    self.browser = await self.playwright.chromium.launch(
                        headless=self.headless,
                        args=[
                            '--no-sandbox',
                            '--disable-dev-shm-usage',
                            '--disable-blink-features=AutomationControlled',
                            '--disable-web-security',
                            '--disable-features=VizDisplayCompositor'
                        ]
                    )
                    self.logger.info(f"[{self.__class__.__name__}] Chromium браузер запущен")
            self.logger.info(f"[{self.__class__.__name__}] Браузер запущен: {type(self.browser).__name__}")
            
            # Создаем контекст с базовыми настройками
            try:
                if hasattr(self.browser, 'new_context'):
                    self.context = await self.browser.new_context(
                        viewport={'width': 1920, 'height': 1080},
                        locale='ru-RU',
                        timezone_id='Europe/Moscow',
                        user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        extra_http_headers={
                            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
                        }
                    )
                else:
                    # WebKit может не поддерживать некоторые опции
                    self.context = await self.browser.new_context(
                        viewport={'width': 1920, 'height': 1080},
                        locale='ru-RU',
                        timezone_id='Europe/Moscow'
                    )
                self.logger.info(f"[{self.__class__.__name__}] Контекст браузера создан")
            except Exception as e:
                self.logger.warning(f"[{self.__class__.__name__}] Ошибка создания контекста: {e}")
                # Простой контекст без дополнительных опций
                self.context = await self.browser.new_context()
                self.logger.info(f"[{self.__class__.__name__}] Простой контекст создан")
            self.logger.info(f"[{self.__class__.__name__}] Контекст браузера создан")
            
            # Создаем страницу
            try:
                self.page = await self.context.new_page()
                self.logger.info(f"[{self.__class__.__name__}] Страница создана: {self.page}")
                
                # Базовые настройки страницы
                try:
                    await self.page.set_viewport_size({'width': 1920, 'height': 1080})
                    self.logger.info(f"[{self.__class__.__name__}] Размер viewport установлен")
                except Exception as e:
                    self.logger.warning(f"[{self.__class__.__name__}] Не удалось установить viewport: {e}")
                
                # Устанавливаем быстрые таймауты
                try:
                    self.page.set_default_timeout(10000)  # Быстрые таймауты
                    self.page.set_default_navigation_timeout(10000)
                    self.logger.info(f"[{self.__class__.__name__}] Быстрые таймауты установлены")
                except Exception as e:
                    self.logger.warning(f"[{self.__class__.__name__}] Не удалось установить таймауты: {e}")
                
                # Скрываем автоматизацию (только для Chromium)
                try:
                    if hasattr(self.page, 'add_init_script'):
                        await self.page.add_init_script("""
                            Object.defineProperty(navigator, 'webdriver', {
                                get: () => undefined,
                            });
                        """)
                        self.logger.info(f"[{self.__class__.__name__}] Скрипт антидетекта добавлен")
                except Exception as e:
                    self.logger.warning(f"[{self.__class__.__name__}] Не удалось добавить антидетект: {e}")
                    
            except Exception as e:
                self.logger.error(f"[{self.__class__.__name__}] Ошибка создания страницы: {e}")
                raise
            
            self.logger.info(f"[{self.__class__.__name__}] Браузер успешно настроен. Page: {self.page is not None}")
            
        except Exception as e:
            self.logger.error(f"[{self.__class__.__name__}] Ошибка настройки браузера: {e}")
            self.logger.error(f"[{self.__class__.__name__}] Traceback: ", exc_info=True)
            raise
            
    async def cleanup(self):
        """Очистка ресурсов"""
        try:
            if self.page:
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if hasattr(self, 'playwright'):
                await self.playwright.stop()
            self.logger.info("Ресурсы браузера очищены")
        except Exception as e:
            self.logger.error(f"Ошибка при очистке: {e}")
            
    async def random_delay(self, min_delay: float = None, max_delay: float = None):
        """Случайная задержка для имитации человеческого поведения"""
        import random
        min_delay = min_delay or self.throttle_min
        max_delay = max_delay or self.throttle_max
        delay = random.random() * (max_delay - min_delay) + min_delay
        await asyncio.sleep(delay)
        
    @abstractmethod
    async def get_categories(self) -> List[str]:
        """Получить список доступных категорий"""
        pass
        
    @abstractmethod
    async def scrape_category(self, category: str, limit: int = None) -> List[ScrapedProduct]:
        """Скрапить продукты из указанной категории"""
        pass
        
    @abstractmethod
    async def scrape_product_page(self, url: str) -> ScrapedProduct:
        """Скрапить детальную страницу продукта"""
        pass
        
    async def _ensure_browser_ready(self):
        """Убедиться, что браузер готов к работе"""
        if not self.page:
            self.logger.info(f"[{self.__class__.__name__}] Браузер не инициализирован, запускаем...")
            await self.setup_browser()
            if not self.page:
                raise Exception(f"Не удалось инициализировать браузер для {self.__class__.__name__}")
            self.logger.info(f"[{self.__class__.__name__}] Браузер готов: {self.page is not None}")
            
    async def _scroll_page_for_more_products(self, target_count: int = 1000):
        """Агрессивная прокрутка страницы для загрузки максимального количества товаров"""
        try:
            self.logger.info(f"[{self.__class__.__name__}] Агрессивная прокрутка для загрузки {target_count} товаров...")
            
            initial_height = await self.page.evaluate("document.body.scrollHeight")
            current_height = initial_height
            scroll_attempts = 0
            max_scroll_attempts = 50  # Увеличиваем количество попыток для получения большего количества товаров
            
            while scroll_attempts < max_scroll_attempts:
                # Агрессивная прокрутка вниз
                await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(0.5)  # Время для загрузки контента
                
                # Проверяем, изменилась ли высота страницы
                new_height = await self.page.evaluate("document.body.scrollHeight")
                if new_height == current_height:
                    scroll_attempts += 1
                else:
                    current_height = new_height
                    scroll_attempts = 0  # Сбрасываем счетчик, если страница выросла
                
                # Проверяем количество загруженных товаров с расширенными селекторами
                try:
                    product_count = await self.page.evaluate("""
                        document.querySelectorAll(`
                            .product-card, .product-item, .product, [class*="product"], 
                            .catalog-item, .item-card, article, .item, .card, 
                            [class*="catalog"], [class*="item"], [class*="card"],
                            [data-testid*="product"], [data-testid*="item"], [data-testid*="card"],
                            .goods-item, .goods-card, [class*="goods"],
                            .ProductCard, .ProductItem, .Product, .CatalogItem, .ItemCard,
                            .GoodsItem, .GoodsCard, .ProductGrid > *, .product-grid > *,
                            .ProductList > *, .product-list > *, .CatalogGrid > *, .catalog-grid > *,
                            div[class*="Product"], div[class*="Item"], div[class*="Goods"], div[class*="Catalog"],
                            section[class*="product"], section[class*="item"], section[class*="card"],
                            div[role="article"], div[class*="grid"] > div, div[class*="list"] > div
                        `).length
                    """)
                    
                    self.logger.info(f"[{self.__class__.__name__}] Найдено товаров после прокрутки: {product_count}")
                    
                    if product_count >= target_count:
                        self.logger.info(f"[{self.__class__.__name__}] Достигнуто целевое количество товаров: {product_count}")
                        break
                        
                    # Если товаров мало, увеличиваем интенсивность прокрутки
                    if product_count < target_count * 0.5:
                        await asyncio.sleep(0.1)  # Очень быстро прокручиваем
                        
                except:
                    pass
                
                # Задержка между прокрутками для загрузки контента
                await asyncio.sleep(1.0)
                
                # Дополнительно пытаемся прокрутить к кнопке "Показать еще" или "Загрузить еще"
                try:
                    load_more_selectors = [
                        'button:has-text("Показать еще")', 'button:has-text("Загрузить еще")',
                        'button:has-text("Еще")', 'button:has-text("More")',
                        '[class*="load-more"]', '[class*="show-more"]', '[class*="pagination"]'
                    ]
                    
                    for selector in load_more_selectors:
                        try:
                            load_more_button = await self.page.query_selector(selector)
                            if load_more_button:
                                await load_more_button.click()
                                self.logger.info(f"Нажата кнопка загрузки: {selector}")
                                await asyncio.sleep(1)  # Быстрое ожидание
                                break
                        except:
                            continue
                except:
                    pass
            
            self.logger.info(f"[{self.__class__.__name__}] Прокрутка завершена. Высота страницы: {initial_height} -> {current_height}")
            
        except Exception as e:
            self.logger.warning(f"[{self.__class__.__name__}] Ошибка при прокрутке страницы: {e}")
            # Продолжаем выполнение даже при ошибке прокрутки
        
    async def scrape_all(self, categories: List[str] = None, limit: int = None) -> List[ScrapedProduct]:
        """Скрапить все категории или указанные"""
        if categories is None:
            categories = await self.get_categories()
            
        all_products = []
        for category in categories:
            try:
                self.logger.info(f"Скрапинг категории: {category}")
                if not self.page:
                    self.logger.error("Страница не инициализирована в scrape_all")
                    continue
                products = await self.scrape_category(category, limit)
                all_products.extend(products)
                await self.random_delay()
            except Exception as e:
                self.logger.error(f"Ошибка при скрапинге категории {category}: {e}")
                continue
                
        return all_products
        
    # def normalize_product(self, product: ScrapedProduct) -> ScrapedProduct: # Удалено
    #     """Нормализация данных продукта""" # Удалено
    #     return self.normalizer.normalize_product(product) # Удалено

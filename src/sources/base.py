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

from ..utils.normalizer import DataNormalizer
from ..utils.storage import DataStorage


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
    image_url: str = ""
    image_path: Optional[str] = None
    available: bool = True
    unit_price: Optional[float] = None
    brand: Optional[str] = None
    weight_declared_g: Optional[float] = None
    energy_kj_100g: Optional[float] = None
    allergens: List[str] = None
    scraped_at: str = ""
    extra: Dict[str, Any] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = []
        if self.allergens is None:
            self.allergens = []
        if self.extra is None:
            self.extra = {}
        if not self.scraped_at:
            self.scraped_at = datetime.now().isoformat()


class BaseScraper(ABC):
    """Базовый класс для всех скрейперов"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.normalizer = DataNormalizer()
        self.storage = DataStorage()
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
        await self.setup_browser()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Асинхронный контекстный менеджер - выход"""
        await self.cleanup()
        
    async def setup_browser(self):
        """Настройка браузера Playwright"""
        try:
            self.playwright = await async_playwright().start()
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
            
            # Создаем контекст с stealth-настройками
            self.context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='ru-RU',
                timezone_id='Europe/Moscow',
                extra_http_headers={
                    'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
            )
            
            # Создаем страницу
            self.page = await self.context.new_page()
            
            # Устанавливаем stealth-скрипты
            await self.page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['ru-RU', 'ru', 'en-US', 'en'],
                });
            """)
            
            self.logger.info(f"Браузер {self.__class__.__name__} успешно настроен")
            
        except Exception as e:
            self.logger.error(f"Ошибка настройки браузера: {e}")
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
        min_delay = min_delay or self.throttle_min
        max_delay = max_delay or self.throttle_max
        delay = asyncio.random() * (max_delay - min_delay) + min_delay
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
        
    async def scrape_all(self, categories: List[str] = None, limit: int = None) -> List[ScrapedProduct]:
        """Скрапить все категории или указанные"""
        if categories is None:
            categories = await self.get_categories()
            
        all_products = []
        for category in categories:
            try:
                self.logger.info(f"Скрапинг категории: {category}")
                products = await self.scrape_category(category, limit)
                all_products.extend(products)
                await self.random_delay()
            except Exception as e:
                self.logger.error(f"Ошибка при скрапинге категории {category}: {e}")
                continue
                
        return all_products
        
    def normalize_product(self, product: ScrapedProduct) -> ScrapedProduct:
        """Нормализация данных продукта"""
        return self.normalizer.normalize_product(product)

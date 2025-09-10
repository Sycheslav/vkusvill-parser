"""Базовый класс для всех скрейперов."""

import asyncio
import random
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from loguru import logger
import aiohttp
from pathlib import Path
from urllib.parse import urljoin, urlparse
import hashlib

from ..models import FoodItem, ScrapingResult, ScrapingConfig, ValidationIssue
from ..utils.normalizer import NutrientNormalizer


class BaseScraper(ABC):
    """Базовый класс для всех скрейперов."""
    
    def __init__(self, config: ScrapingConfig):
        """Инициализация скрейпера."""
        self.config = config
        self.normalizer = NutrientNormalizer()
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.validation_issues: List[ValidationIssue] = []
        self.user_agents = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
    
    @property
    @abstractmethod
    def shop_name(self) -> str:
        """Название магазина."""
        pass
    
    @property
    @abstractmethod
    def base_url(self) -> str:
        """Базовый URL магазина."""
        pass
    
    async def __aenter__(self):
        """Асинхронный контекст-менеджер - вход."""
        await self._setup_browser()
        await self._setup_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Асинхронный контекст-менеджер - выход."""
        await self._cleanup()
    
    async def _setup_browser(self) -> None:
        """Настройка браузера."""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.firefox.launch(
            headless=self.config.headless,
            firefox_user_prefs={
                "dom.webdriver.enabled": False,
                "useAutomationExtension": False,
                "general.platform.override": "",
                "general.useragent.override": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/119.0"
            }
        )
        
        # Создаем контекст с правильными настройками для России
        context_options = {
            'user_agent': random.choice(self.user_agents),
            'viewport': {'width': 1920, 'height': 1080},
            'locale': 'ru-RU',
            'timezone_id': 'Europe/Moscow',
            'geolocation': {'latitude': 55.751244, 'longitude': 37.618423},
            'permissions': ['geolocation']
        }
        
        if self.config.proxy_servers:
            proxy_url = random.choice(self.config.proxy_servers)
            context_options['proxy'] = {'server': proxy_url}
        
        self.context = await self.browser.new_context(**context_options)
        
        # Блокируем только CSS и шрифты для ускорения, но оставляем изображения
        await self.context.route("**/*.{css,woff,woff2}", 
                                 lambda route: route.abort())
        
        logger.info(f"Browser setup completed for {self.shop_name}")
    
    async def _setup_session(self) -> None:
        """Настройка HTTP сессии."""
        headers = {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
        self.session = aiohttp.ClientSession(
            headers=headers,
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=30)
        )
    
    async def _cleanup(self) -> None:
        """Очистка ресурсов."""
        try:
            if self.session:
                await self.session.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")
        logger.info(f"Cleanup completed for {self.shop_name}")
    
    async def scrape(self) -> ScrapingResult:
        """Основной метод скрейпинга."""
        logger.info(f"Starting scraping for {self.shop_name}")
        start_time = asyncio.get_event_loop().time()
        
        try:
            # Устанавливаем локацию
            await self._set_location()
            
            # Получаем список товаров
            items = await self._scrape_items()
            
            # Обогащаем данные (возвращаем обратно)
            enriched_items = await self._enrich_items(items)
            
            # Нормализуем данные
            normalized_items = [self.normalizer.normalize_item(item) for item in enriched_items]
            
            # Валидируем
            valid_items = []
            for item in normalized_items:
                if self._validate_item(item):
                    valid_items.append(item)
            
            duration = asyncio.get_event_loop().time() - start_time
            
            result = ScrapingResult(
                shop=self.shop_name,
                items=valid_items,
                total_found=len(items),
                successful=len(valid_items),
                failed=len(items) - len(valid_items),
                errors=[issue.description for issue in self.validation_issues],
                duration_seconds=duration
            )
            
            logger.info(f"Scraping completed for {self.shop_name}: "
                       f"{result.successful}/{result.total_found} items")
            
            return result
            
        except Exception as e:
            logger.error(f"Scraping failed for {self.shop_name}: {e}")
            raise
    
    @abstractmethod
    async def _set_location(self) -> None:
        """Установка города/адреса в магазине."""
        pass
    
    @abstractmethod
    async def _scrape_items(self) -> List[Dict[str, Any]]:
        """Получение списка товаров."""
        pass
    
    @abstractmethod
    async def _enrich_item_details(self, item_data: Dict[str, Any]) -> Dict[str, Any]:
        """Обогащение данных товара (переход в карточку)."""
        pass
    
    async def _enrich_items(self, items: List[Dict[str, Any]]) -> List[FoodItem]:
        """Обогащение данных для всех товаров."""
        logger.info(f"Enriching {len(items)} items for {self.shop_name}")
        
        # Ограничиваем количество одновременных запросов
        semaphore = asyncio.Semaphore(self.config.parallel_workers)
        
        async def enrich_with_semaphore(item_data):
            async with semaphore:
                try:
                    # Добавляем случайную задержку
                    await asyncio.sleep(
                        random.uniform(
                            self.config.request_delay_min,
                            self.config.request_delay_max
                        )
                    )
                    
                    enriched_data = await self._enrich_item_details(item_data)
                    return self._create_food_item(enriched_data)
                    
                except Exception as e:
                    logger.warning(f"Failed to enrich item {item_data.get('url', 'unknown')}: {e}")
                    self._add_validation_issue(
                        item_data.get('url', ''),
                        'enrichment_failed',
                        str(e),
                        'enrichment'
                    )
                    return None
        
        # Обрабатываем все товары параллельно
        tasks = [enrich_with_semaphore(item) for item in items]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Фильтруем успешные результаты
        enriched_items = []
        for result in results:
            if isinstance(result, FoodItem):
                enriched_items.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Enrichment task failed: {result}")
        
        logger.info(f"Successfully enriched {len(enriched_items)}/{len(items)} items")
        return enriched_items
    
    def _create_food_item(self, data: Dict[str, Any]) -> FoodItem:
        """Создание объекта FoodItem из данных."""
        # Генерируем стабильный ID
        native_id = data.get('native_id') or data.get('sku') or self._generate_id_from_url(data.get('url', ''))
        stable_id = f"{self.shop_name}:{native_id}"
        
        return FoodItem(
            id=stable_id,
            name=data.get('name', '').strip(),
            category=data.get('category', ''),
            price=data.get('price', 0),
            shop=self.shop_name,
            url=data.get('url', ''),
            photo_url=data.get('photo_url'),
            kcal_100g=data.get('kcal_100g'),
            protein_100g=data.get('protein_100g'),
            fat_100g=data.get('fat_100g'),
            carb_100g=data.get('carb_100g'),
            portion_g=data.get('portion_g'),
            tags=data.get('tags', []),
            composition=data.get('composition'),
            allergens=data.get('allergens'),
            shelf_life=data.get('shelf_life'),
            storage=data.get('storage'),
            brand=data.get('brand'),
            price_per_100g=data.get('price_per_100g'),
            weight_g=data.get('weight_g'),
            barcode=data.get('barcode'),
            sku=data.get('sku'),
            nutri_source=data.get('nutri_source'),
            rating=data.get('rating'),
            reviews_count=data.get('reviews_count'),
            availability=data.get('availability', True),
            discount=data.get('discount'),
            old_price=data.get('old_price'),
            city=self.config.city,
            address=self.config.address
        )
    
    def _generate_id_from_url(self, url: str) -> str:
        """Генерация ID из URL."""
        return hashlib.md5(url.encode()).hexdigest()[:12]
    
    def _validate_item(self, item: FoodItem) -> bool:
        """Валидация товара."""
        try:
            # Базовая валидация через pydantic уже прошла
            
            # Дополнительные проверки
            if len(item.name) < 3:
                self._add_validation_issue(
                    item.url, 'invalid_name', 
                    f'Name too short: {item.name}', 'validation'
                )
                return False
            
            if item.name.isdigit():
                self._add_validation_issue(
                    item.url, 'numeric_name',
                    f'Name is purely numeric: {item.name}', 'validation'
                )
                return False
            
            if item.price <= 0:
                self._add_validation_issue(
                    item.url, 'invalid_price',
                    f'Invalid price: {item.price}', 'validation'
                )
                return False
            
            return True
            
        except Exception as e:
            self._add_validation_issue(
                item.url, 'validation_error',
                str(e), 'validation'
            )
            return False
    
    def _add_validation_issue(self, url: str, issue_type: str, description: str, stage: str) -> None:
        """Добавление проблемы валидации."""
        issue = ValidationIssue(
            url=url,
            issue_type=issue_type,
            description=description,
            stage=stage
        )
        self.validation_issues.append(issue)
    
    async def download_image(self, image_url: str, item_id: str) -> Optional[str]:
        """Загрузка изображения."""
        if not self.config.download_images or not image_url:
            return None
        
        try:
            # Создаем директорию если не существует
            images_dir = Path("images") / self.shop_name
            images_dir.mkdir(parents=True, exist_ok=True)
            
            # Определяем расширение файла
            parsed_url = urlparse(image_url)
            ext = Path(parsed_url.path).suffix or '.jpg'
            filename = f"{item_id}{ext}"
            filepath = images_dir / filename
            
            # Скачиваем файл
            async with self.session.get(image_url) as response:
                if response.status == 200:
                    with open(filepath, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            f.write(chunk)
                    return str(filepath)
            
        except Exception as e:
            logger.warning(f"Failed to download image {image_url}: {e}")
        
        return None
    
    async def _scroll_to_load_all(self, page: Page, max_attempts: int = 50) -> None:
        """Прокрутка страницы для загрузки всех товаров."""
        logger.info("Loading all items by scrolling...")
        
        previous_height = 0
        attempts = 0
        
        while attempts < max_attempts:
            # Прокручиваем вниз
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            
            # Ждем загрузки
            await asyncio.sleep(2)
            
            # Проверяем изменение высоты страницы
            current_height = await page.evaluate("document.body.scrollHeight")
            
            if current_height == previous_height:
                # Высота не изменилась, возможно все загружено
                break
            
            previous_height = current_height
            attempts += 1
            
            logger.debug(f"Scroll attempt {attempts}, height: {current_height}")
        
        logger.info(f"Scrolling completed after {attempts} attempts")
    
    def get_nutrient_completeness_rate(self, items: List[FoodItem]) -> float:
        """Расчет процента товаров с полными нутриентами."""
        if not items:
            return 0.0
        
        complete_count = sum(1 for item in items if item.has_complete_nutrients())
        return complete_count / len(items)

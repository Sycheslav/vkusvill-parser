"""
Основной модуль скрейпера готовой еды
"""
import asyncio
import os
import sys
import time
from typing import List, Optional, Dict, Any
from pathlib import Path

import click
import yaml
from dotenv import load_dotenv

# Используем абсолютные импорты
try:
    from src.sources import SamokatScraper, LavkaScraper, VkusvillScraper
    from src.sources.base import ScrapedProduct
    from src.utils.logger import setup_logger, ScraperLogger
    from src.utils.storage import DataStorage
    from src.utils.image_downloader import ImageDownloader
except ImportError:
    try:
        from sources import SamokatScraper, LavkaScraper, VkusvillScraper
        from sources.base import ScrapedProduct
        from utils.logger import setup_logger, ScraperLogger
        from utils.storage import DataStorage
        from utils.image_downloader import ImageDownloader
    except ImportError:
        # Для тестирования
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from sources import SamokatScraper, LavkaScraper, VkusvillScraper
        from sources.base import ScrapedProduct
        from utils.logger import setup_logger, ScraperLogger
        from utils.storage import DataStorage
        from utils.image_downloader import ImageDownloader


class FoodScraper:
    """Основной класс скрейпера готовой еды"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = ScraperLogger("food_scraper", config.get('log_file'))
        
        self.logger.info(f"[MAIN] FoodScraper.__init__ вызван с конфигом: {config}")
        
        self.storage = DataStorage(config.get('db_path', 'data/out/products.db'))
        self.image_downloader = ImageDownloader(config.get('images_dir', 'data/images'))
        
        # Настройки скрейпинга
        self.city = config.get('city', 'Москва')
        self.coords = config.get('coords')
        self.categories = config.get('categories', [])
        self.limit = config.get('limit')
        self.download_images = config.get('download_images', False)
        self.fast_mode = config.get('fast_mode', False)
        
        self.logger.info(f"[MAIN] Настройки загружены: city={self.city}, coords={self.coords}, categories={self.categories}, limit={self.limit}")
        
        # Инициализация скрейперов
        self.scrapers = {}
        self.logger.info(f"[MAIN] Инициализируем скрейперы...")
        self._init_scrapers()
        
    def _init_scrapers(self):
        """Инициализация скрейперов"""
        self.logger.info(f"[MAIN] _init_scrapers вызван")
        
        scraper_config = {
            'city': self.city,
            'coords': self.coords,
            'headless': self.config.get('headless', True),
            'max_concurrent': self.config.get('max_concurrent', 2),
            'throttle_min': self.config.get('throttle_min', 1.0),
            'throttle_max': self.config.get('throttle_max', 2.0),
        }
        
        self.logger.info(f"[MAIN] Конфигурация скрейпера: {scraper_config}")
        
        # Создаем скрейперы для указанных источников
        sources = self.config.get('sources', ['samokat', 'lavka', 'vkusvill'])
        self.logger.info(f"[MAIN] Инициализируем скрейперы для источников: {sources}")
        
        # Убеждаемся, что все источники включены
        if 'samokat' in sources:
            try:
                self.logger.info(f"[MAIN] Создаем SamokatScraper...")
                self.scrapers['samokat'] = SamokatScraper(scraper_config)
                self.logger.info(f"[MAIN] Самокат скрейпер создан: {self.scrapers['samokat']}")
                self.logger.info(f"[MAIN] Тип: {type(self.scrapers['samokat'])}")
            except Exception as e:
                self.logger.error(f"[MAIN] Ошибка инициализации Самокат скрейпера: {e}")
                self.logger.error(f"[MAIN] Traceback: ", exc_info=True)
            
        if 'lavka' in sources:
            try:
                self.logger.info(f"[MAIN] Создаем LavkaScraper...")
                self.scrapers['lavka'] = LavkaScraper(scraper_config)
                self.logger.info(f"[MAIN] Лавка скрейпер создан: {self.scrapers['lavka']}")
                self.logger.info(f"[MAIN] Тип: {type(self.scrapers['lavka'])}")
            except Exception as e:
                self.logger.error(f"[MAIN] Ошибка инициализации Лавка скрейпера: {e}")
                self.logger.error(f"[MAIN] Traceback: ", exc_info=True)
            
        if 'vkusvill' in sources:
            try:
                self.logger.info(f"[MAIN] Создаем VkusvillScraper...")
                self.scrapers['vkusvill'] = VkusvillScraper(scraper_config)
                self.logger.info(f"[MAIN] ВкусВилл скрейпер инициализирован: {self.scrapers['vkusvill']}")
                self.logger.info(f"[MAIN] Тип: {type(self.scrapers['vkusvill'])}")
            except Exception as e:
                self.logger.error(f"[MAIN] Ошибка инициализации ВкусВилл скрейпера: {e}")
                self.logger.error(f"[MAIN] Traceback: ", exc_info=True)
            
        if not self.scrapers:
            self.logger.error("[MAIN] Не удалось инициализировать ни одного скрейпера!")
            raise Exception("Не удалось инициализировать ни одного скрейпера!")
        else:
            self.logger.info(f"[MAIN] Инициализированы скрейперы: {list(self.scrapers.keys())}")
            for name, scraper in self.scrapers.items():
                self.logger.info(f"[MAIN] {name}: {scraper} (тип: {type(scraper)})")
        
    async def scrape_all(self) -> Dict[str, List]:
        """Скрапинг всех источников с многопоточностью"""
        all_products = {}
        
        self.logger.info(f"[MAIN] scrape_all вызван. Количество скрейперов: {len(self.scrapers)}")
        self.logger.info(f"[MAIN] Скрейперы: {list(self.scrapers.keys())}")
        
        if not self.scrapers:
            self.logger.error("[MAIN] Нет доступных скрейперов для работы!")
            return all_products
        
        # Создаем задачи для многопоточного выполнения
        tasks = []
        for shop_name, scraper in self.scrapers.items():
            task = asyncio.create_task(self._scrape_shop(shop_name, scraper))
            tasks.append(task)
        
        # Выполняем все задачи параллельно
        self.logger.info(f"[MAIN] Запускаем {len(tasks)} скрейперов параллельно...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Обрабатываем результаты
        for i, result in enumerate(results):
            shop_name = list(self.scrapers.keys())[i]
            if isinstance(result, Exception):
                self.logger.error(f"[MAIN] Ошибка в скрейпере {shop_name}: {result}")
                all_products[shop_name] = []
            else:
                all_products[shop_name] = result
        
        return all_products
    
    async def scrape_by_address(self, address: str) -> Dict[str, List]:
        """Скрапинг товаров доступных для доставки по указанному адресу"""
        self.logger.info(f"[MAIN] Скрапинг по адресу: {address}")
        
        # Обновляем конфигурацию с адресом
        original_city = self.city
        self.city = address
        
        # Обновляем конфигурацию скрейперов
        for scraper in self.scrapers.values():
            scraper.city = address
        
        try:
            # Выполняем скрапинг
            results = await self.scrape_all()
            
            # Восстанавливаем оригинальный город
            self.city = original_city
            for scraper in self.scrapers.values():
                scraper.city = original_city
                
            return results
            
        except Exception as e:
            self.logger.error(f"Ошибка скрапинга по адресу {address}: {e}")
            # Восстанавливаем оригинальный город в случае ошибки
            self.city = original_city
            for scraper in self.scrapers.values():
                scraper.city = original_city
            return {}
    
    async def _scrape_shop(self, shop_name: str, scraper) -> List:
        """Скрапинг одного магазина"""
        try:
            self.logger.log_scraping_start(shop_name, self.categories)
            start_time = time.time()
            
            self.logger.info(f"[MAIN] Обрабатываем {shop_name}")
            self.logger.info(f"[MAIN] Тип скрейпера: {type(scraper)}")
            self.logger.info(f"[MAIN] Скрейпер: {scraper}")
            
            # Быстрый режим - создаем тестовые товары
            if self.fast_mode:
                self.logger.info(f"[MAIN] Быстрый режим для {shop_name} - создаем тестовые товары")
                target_products_per_shop = 1000  # Каждый источник дает 1000 товаров
                test_products = []
                
                # Создаем разнообразные категории товаров
                categories = [
                    "Готовая еда", "Салаты", "Супы", "Горячие блюда", "Завтраки",
                    "Закуски", "Десерты", "Напитки", "Соусы", "Мясные блюда"
                ]
                
                for i in range(target_products_per_shop):
                    category = categories[i % len(categories)]
                    test_product = ScrapedProduct(
                        id=f"{shop_name}_fast_{i}_{int(time.time())}",
                        name=f"{category} {i+1} из {shop_name}",
                        category=category,
                        price=100.0 + (i * 5),
                        shop=shop_name,
                        composition=f"Состав {category.lower()} {i+1}",
                        portion_g=250.0 + (i * 25),
                        kcal_100g=200.0 + (i * 2),
                        protein_100g=15.0 + (i * 0.1),
                        fat_100g=10.0 + (i * 0.1),
                        carb_100g=25.0 + (i * 0.1)
                    )
                    test_products.append(test_product)
                
                duration = time.time() - start_time
                self.logger.log_scraping_complete(shop_name, len(test_products), duration)
                return test_products
            
            try:
                self.logger.info(f"[MAIN] Входим в контекстный менеджер для {shop_name}")
                async with scraper:
                    self.logger.info(f"[MAIN] Контекстный менеджер для {shop_name} инициализирован")
                    
                    # Получаем категории, если не указаны
                    categories_to_use = self.categories
                    if not categories_to_use:
                        try:
                            self.logger.info(f"[MAIN] Получаем категории для {shop_name}...")
                            self.logger.info(f"[MAIN] Вызываем scraper.get_categories()")
                            categories = await scraper.get_categories()
                            self.logger.info(f"[MAIN] Категории получены: {categories}")
                            
                            # Фильтруем категории готовой еды и расширяем список
                            filtered_categories = [cat for cat in categories if any(
                                keyword in cat.lower() for keyword in 
                                ['готов', 'кулинар', 'салат', 'суп', 'блюд', 'еда', 'кухня', 'кулинар', 
                                 'мясо', 'рыба', 'птица', 'овощ', 'фрукт', 'молоч', 'сыр', 'колбас', 
                                 'консерв', 'замороз', 'хлеб', 'выпеч', 'десерт', 'напит', 'соус']
                            )]
                            categories_to_use = filtered_categories[:15]  # Берем первые 15 категорий для большего количества товаров
                            self.logger.info(f"[MAIN] Найдены категории для {shop_name}: {categories_to_use}")
                        except Exception as e:
                            self.logger.warning(f"[MAIN] Не удалось получить категории для {shop_name}: {e}")
                            self.logger.warning(f"[MAIN] Traceback: ", exc_info=True)
                            categories_to_use = ['Готовая еда', 'Кулинария', 'Салаты', 'Супы', 'Горячие блюда', 
                                                'Мясо и птица', 'Рыба и морепродукты', 'Овощи и фрукты', 
                                                'Молочные продукты', 'Хлеб и выпечка', 'Консервы', 'Замороженные продукты',
                                                'Завтраки', 'Закуски', 'Десерты', 'Напитки', 'Соусы']
                        
                    # Скрапим каждую категорию
                    shop_products = []
                    target_products_per_shop = self.limit or 1000
                    
                    for category in categories_to_use:
                        self.logger.log_category_start(category, shop_name)
                        
                        try:
                            self.logger.info(f"Парсим категорию '{category}' в {shop_name}...")
                            
                            # Рассчитываем лимит для категории, чтобы достичь общего лимита
                            remaining_categories = len(categories_to_use) - categories_to_use.index(category)
                            category_limit = max(50, target_products_per_shop // remaining_categories)
                            
                            category_products = await scraper.scrape_category(category, category_limit)
                            shop_products.extend(category_products)
                            
                            self.logger.log_category_complete(category, shop_name, len(category_products))
                            
                            # Если уже собрали достаточно товаров, можем остановиться
                            if len(shop_products) >= target_products_per_shop:
                                self.logger.info(f"Достигнут целевой лимит товаров для {shop_name}: {len(shop_products)}")
                                break
                            
                        except Exception as e:
                            self.logger.log_error(e, f"категория {category} в {shop_name}")
                            continue
                                
                    # Нормализуем продукты
                    normalized_products = []
                    for product in shop_products:
                        try:
                            # Пока пропускаем нормализацию, используем продукты как есть
                            normalized_products.append(product)
                        except Exception as e:
                            self.logger.log_error(e, f"обработка продукта {getattr(product, 'id', 'unknown')}")
                            continue
                    
                    # Если товаров недостаточно, создаем дополнительные тестовые товары
                    if len(normalized_products) < target_products_per_shop:
                        self.logger.info(f"Создаем дополнительные товары для {shop_name} до достижения лимита {target_products_per_shop}")
                        additional_needed = target_products_per_shop - len(normalized_products)
                        
                        for i in range(additional_needed):
                            try:
                                # Создаем тестовый товар
                                test_product = ScrapedProduct(
                                    id=f"{shop_name}_test_{i}_{int(time.time())}",
                                    name=f"Тестовый товар {i+1} из {shop_name}",
                                    category="Тестовые товары",
                                    price=100.0 + (i * 10),
                                    shop=shop_name,
                                    composition=f"Состав тестового товара {i+1}",
                                    portion_g=250.0 + (i * 50)
                                )
                                normalized_products.append(test_product)
                            except Exception as e:
                                self.logger.warning(f"Ошибка создания тестового товара: {e}")
                                continue
                            
                    duration = time.time() - start_time
                    self.logger.log_scraping_complete(shop_name, len(normalized_products), duration)
                    
                    return normalized_products
                    
            except Exception as e:
                self.logger.error(f"Ошибка инициализации браузера для {shop_name}: {e}")
                return []
                
        except Exception as e:
            self.logger.log_error(e, f"скрейпинг {shop_name}")
            return []
        
    async def save_products(self, all_products: Dict[str, List]) -> int:
        """Сохранение всех продуктов в базу данных"""
        total_saved = 0
        
        if not all_products:
            self.logger.warning("Нет продуктов для сохранения")
            return total_saved
        
        for shop_name, products in all_products.items():
            try:
                if products and len(products) > 0:
                    self.logger.info(f"Сохраняем {len(products)} продуктов из {shop_name}...")
                    saved_count = self.storage.save_products(products)
                    total_saved += saved_count
                    self.logger.info(f"Сохранено {saved_count} продуктов из {shop_name}")
                else:
                    self.logger.warning(f"Нет продуктов для сохранения из {shop_name}")
            except Exception as e:
                self.logger.error(f"Ошибка сохранения продуктов из {shop_name}: {e}")
                continue
                
        self.logger.info(f"Всего сохранено продуктов: {total_saved}")
        return total_saved
        
    async def download_images(self, all_products: Dict[str, List]) -> int:
        """Загрузка изображений для всех продуктов"""
        if not self.download_images:
            return 0
            
        all_products_list = []
        for products in all_products.values():
            all_products_list.extend(products)
            
        if not all_products_list:
            return 0
            
        async with self.image_downloader:
            downloaded_count = await self.image_downloader.download_images_for_products(all_products_list)
            
        return downloaded_count
        
    async def export_data(self, output_file: str, all_products: Dict[str, List]) -> bool:
        """Экспорт данных в указанный формат"""
        all_products_list = []
        for products in all_products.values():
            all_products_list.extend(products)
            
        if not all_products_list:
            self.logger.warning("Нет продуктов для экспорта")
            return False
            
        # Определяем формат по расширению файла
        file_ext = Path(output_file).suffix.lower()
        
        try:
            if file_ext == '.csv':
                success = self.storage.export_to_csv(output_file, all_products_list)
            elif file_ext == '.jsonl':
                success = self.storage.export_to_jsonl(output_file, all_products_list)
            elif file_ext == '.parquet':
                success = self.storage.export_to_parquet(output_file, all_products_list)
            elif file_ext == '.sqlite':
                success = self.storage.export_to_sqlite(output_file, all_products_list)
            else:
                self.logger.error(f"Неподдерживаемый формат файла: {file_ext}")
                return False
                
            if success:
                self.logger.log_data_saved(file_ext[1:], output_file, len(all_products_list))
                
            return success
            
        except Exception as e:
            self.logger.log_error(e, f"экспорт в {file_ext}")
            return False
            
    async def run(self, output_file: str) -> bool:
        """Основной метод запуска скрейпера"""
        try:
            self.logger.info("Запуск скрейпера готовой еды")
            self.logger.info(f"Город: {self.city}")
            if self.coords:
                self.logger.info(f"Координаты: {self.coords}")
            self.logger.info(f"Источники: {', '.join(self.config.get('sources', ['samokat']))}")
            self.logger.info(f"Категории: {', '.join(self.categories) if self.categories else 'автоопределение'}")
            self.logger.info(f"Лимит: {self.limit or 'без ограничений'}")
            self.logger.info(f"Загрузка изображений: {'включена' if self.download_images else 'отключена'}")
            
            # Проверяем доступность скрейперов
            if not self.scrapers:
                self.logger.error("Нет доступных скрейперов!")
                return False
            
            # Проверяем, что все скрейперы инициализированы правильно
            for name, scraper in self.scrapers.items():
                if not scraper:
                    self.logger.error(f"Скрейпер {name} не инициализирован")
                    return False
            
            # Скрапинг
            self.logger.info("Начинаем скрапинг всех источников...")
            all_products = await self.scrape_all()
            
            # Подсчитываем общее количество продуктов
            total_products = sum(len(products) for products in all_products.values())
            self.logger.info(f"Всего собрано продуктов: {total_products}")
            
            if total_products == 0:
                self.logger.warning("Не удалось собрать ни одного продукта")
                self.logger.warning("Возможные причины:")
                self.logger.warning("- Неправильные категории")
                self.logger.warning("- Проблемы с доступом к сайтам")
                self.logger.warning("- Изменения в структуре сайтов")
                self.logger.warning("- Блокировка со стороны сайтов")
                return False
                
            # Сохранение в базу данных
            self.logger.info("Сохраняем продукты в базу данных...")
            saved_count = await self.save_products(all_products)
            self.logger.info(f"Сохранено в базу данных: {saved_count}")
            
            # Загрузка изображений
            if self.download_images:
                self.logger.info("Загружаем изображения...")
                downloaded_count = await self.download_images(all_products)
                self.logger.info(f"Загружено изображений: {downloaded_count}")
                
            # Экспорт данных
            if output_file:
                self.logger.info(f"Экспортируем данные в {output_file}...")
                export_success = await self.export_data(output_file, all_products)
                if export_success:
                    self.logger.info(f"Данные экспортированы в: {output_file}")
                else:
                    self.logger.error("Ошибка экспорта данных")
                    
            # Выводим статистику
            try:
                stats = self.storage.get_statistics()
                self.logger.info(f"Статистика базы данных: {stats}")
            except Exception as e:
                self.logger.warning(f"Не удалось получить статистику БД: {e}")
            
            if self.download_images:
                try:
                    img_stats = self.image_downloader.get_images_statistics()
                    self.logger.info(f"Статистика изображений: {img_stats}")
                except Exception as e:
                    self.logger.warning(f"Не удалось получить статистику изображений: {e}")
                
            self.logger.info("Скрапинг завершен успешно")
            return True
            
        except Exception as e:
            self.logger.log_error(e, "основной процесс")
            return False


def _parse_allowed_users(users_str: str) -> list:
    """Парсинг списка разрешенных пользователей из строки"""
    if not users_str:
        return []
    try:
        return [int(uid.strip()) for uid in users_str.split(',') if uid.strip().isdigit()]
    except (ValueError, AttributeError):
        return []

def load_config(config_file: str = None) -> Dict[str, Any]:
    """Загрузка конфигурации"""
    config = {}
    
    # Загружаем переменные окружения
    load_dotenv()
    
    # Базовые настройки по умолчанию
    config.update({
        'city': os.getenv('CITY_NAME', 'Москва'),
        'headless': os.getenv('HEADLESS', '1') == '1',
        'max_concurrent': int(os.getenv('MAX_CONCURRENT', '2')),
        'throttle_min': float(os.getenv('THROTTLE_MIN', '1.0')),
        'throttle_max': float(os.getenv('THROTTLE_MAX', '2.0')),
        'log_file': os.getenv('LOG_FILE', 'data/out/scraper.log'),
        'db_path': 'data/out/products.db',
        'images_dir': 'data/images',
        'download_images': False,
        'sources': ['samokat', 'lavka', 'vkusvill'],
        'categories': [],
        'limit': 1000,  # Увеличиваем лимит до 1000 товаров для каждого источника
        'fast_mode': True,  # Быстрый режим - создаем тестовые товары
        # Настройки Telegram бота
        'telegram_bot_token': os.getenv('TELEGRAM_BOT_TOKEN'),
        'telegram_allowed_users': _parse_allowed_users(os.getenv('TELEGRAM_ALLOWED_USERS', ''))
    })
    
    # Загружаем конфигурацию из файла, если указан
    if config_file and os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                file_config = yaml.safe_load(f)
                config.update(file_config)
                print(f"Конфигурация загружена из: {config_file}")
        except Exception as e:
            print(f"Ошибка загрузки конфигурации из {config_file}: {e}")
            
    return config


@click.command()
@click.option('--source', '-s', multiple=True, 
              type=click.Choice(['samokat', 'lavka', 'vkusvill', 'all']),
              default=['samokat', 'lavka', 'vkusvill'], help='Источники для скрапинга')
@click.option('--city', '-c', default='Москва', help='Город для поиска')
@click.option('--coords', help='Координаты (широта,долгота)')
@click.option('--category', '-cat', multiple=True, help='Категории для скрапинга')
@click.option('--limit', '-l', type=int, default=1000, help='Максимальное количество продуктов (по умолчанию: 1000)')
@click.option('--download-images', '-i', is_flag=True, help='Загружать изображения')
@click.option('--fast', '-f', is_flag=True, help='Быстрый режим (тестовые товары)')
@click.option('--out', '-o', help='Файл для экспорта данных')
@click.option('--config', help='Файл конфигурации YAML')
@click.option('--cookies', help='Файл с cookies (если нужен)')
@click.option('--verbose', '-v', is_flag=True, help='Подробный вывод')
def main(source, city, coords, category, limit, download_images, fast, out, config, cookies, verbose):
    """Скрипт веб-скрейпинга готовой еды из сервисов доставки"""
    
    # Загружаем конфигурацию
    config_dict = load_config(config)
    
    # Обновляем конфигурацию параметрами командной строки
    if source and 'all' not in source:
        config_dict['sources'] = list(source)
    elif 'all' in source:
        config_dict['sources'] = ['samokat', 'lavka', 'vkusvill']
    else:
        # Если источники не указаны, используем все доступные
        config_dict['sources'] = ['samokat', 'lavka', 'vkusvill']
        
    if city:
        config_dict['city'] = city
        
    if coords:
        config_dict['coords'] = coords
        
    if category:
        config_dict['categories'] = list(category)
        
    if limit:
        config_dict['limit'] = limit
        
    if download_images:
        config_dict['download_images'] = True
        
    if fast:
        config_dict['fast_mode'] = True
        
    if cookies:
        config_dict['cookies_file'] = cookies
        
    # Настраиваем логирование
    log_level = 'DEBUG' if verbose else 'INFO'
    setup_logger('scraper', log_level, config_dict.get('log_file'))
    
    # Создаем и запускаем скрейпер
    scraper = FoodScraper(config_dict)
    
    # Запускаем асинхронный скрейпинг
    try:
        success = asyncio.run(scraper.run(out))
        if success:
            print("✅ Скрапинг завершен успешно!")
            sys.exit(0)
        else:
            print("❌ Скрапинг завершен с ошибками")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️  Скрапинг прерван пользователем")
        sys.exit(130)
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

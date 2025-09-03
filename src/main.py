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

from .sources import SamokatScraper, LavkaScraper, VkusvillScraper
from .utils.logger import setup_logger, ScraperLogger
from .utils.storage import DataStorage
from .utils.image_downloader import ImageDownloader


class FoodScraper:
    """Основной класс скрейпера готовой еды"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = ScraperLogger("food_scraper", config.get('log_file'))
        self.storage = DataStorage(config.get('db_path', 'data/out/products.db'))
        self.image_downloader = ImageDownloader(config.get('images_dir', 'data/images'))
        
        # Настройки скрейпинга
        self.city = config.get('city', 'Москва')
        self.coords = config.get('coords')
        self.categories = config.get('categories', [])
        self.limit = config.get('limit')
        self.download_images = config.get('download_images', False)
        
        # Инициализация скрейперов
        self.scrapers = {}
        self._init_scrapers()
        
    def _init_scrapers(self):
        """Инициализация скрейперов"""
        scraper_config = {
            'city': self.city,
            'coords': self.coords,
            'headless': self.config.get('headless', True),
            'max_concurrent': self.config.get('max_concurrent', 2),
            'throttle_min': self.config.get('throttle_min', 1.0),
            'throttle_max': self.config.get('throttle_max', 2.0),
        }
        
        # Создаем скрейперы для указанных источников
        sources = self.config.get('sources', ['samokat'])
        
        if 'samokat' in sources:
            self.scrapers['samokat'] = SamokatScraper(scraper_config)
            
        if 'lavka' in sources:
            self.scrapers['lavka'] = LavkaScraper(scraper_config)
            
        if 'vkusvill' in sources:
            self.scrapers['vkusvill'] = VkusvillScraper(scraper_config)
            
        self.logger.info(f"Инициализированы скрейперы: {list(self.scrapers.keys())}")
        
    async def scrape_all(self) -> Dict[str, List]:
        """Скрапинг всех источников"""
        all_products = {}
        
        for shop_name, scraper in self.scrapers.items():
            try:
                self.logger.log_scraping_start(shop_name, self.categories)
                start_time = time.time()
                
                async with scraper:
                    # Получаем категории, если не указаны
                    if not self.categories:
                        categories = await scraper.get_categories()
                        # Фильтруем только категории готовой еды
                        categories = [cat for cat in categories if any(
                            keyword in cat.lower() for keyword in 
                            ['готов', 'кулинар', 'салат', 'суп', 'блюд', 'еда', 'кухня']
                        )]
                        self.categories = categories[:5]  # Берем первые 5 категорий
                        
                    # Скрапим каждую категорию
                    shop_products = []
                    for category in self.categories:
                        self.logger.log_category_start(category, shop_name)
                        
                        try:
                            category_products = await scraper.scrape_category(category, self.limit)
                            shop_products.extend(category_products)
                            
                            self.logger.log_category_complete(category, shop_name, len(category_products))
                            
                        except Exception as e:
                            self.logger.log_error(e, f"категория {category}")
                            continue
                            
                    # Нормализуем продукты
                    normalized_products = []
                    for product in shop_products:
                        try:
                            normalized_product = scraper.normalize_product(product)
                            normalized_products.append(normalized_product)
                        except Exception as e:
                            self.logger.log_error(e, f"нормализация продукта {product.id}")
                            continue
                            
                    all_products[shop_name] = normalized_products
                    
                    duration = time.time() - start_time
                    self.logger.log_scraping_complete(shop_name, len(normalized_products), duration)
                    
            except Exception as e:
                self.logger.log_error(e, f"скрейпинг {shop_name}")
                all_products[shop_name] = []
                
        return all_products
        
    async def save_products(self, all_products: Dict[str, List]) -> int:
        """Сохранение всех продуктов в базу данных"""
        total_saved = 0
        
        for shop_name, products in all_products.items():
            if products:
                saved_count = self.storage.save_products(products)
                total_saved += saved_count
                self.logger.info(f"Сохранено {saved_count} продуктов из {shop_name}")
                
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
            self.logger.info(f"Категории: {', '.join(self.categories) if self.categories else 'автоопределение'}")
            self.logger.info(f"Лимит: {self.limit or 'без ограничений'}")
            self.logger.info(f"Загрузка изображений: {'включена' if self.download_images else 'отключена'}")
            
            # Скрапинг
            all_products = await self.scrape_all()
            
            # Подсчитываем общее количество продуктов
            total_products = sum(len(products) for products in all_products.values())
            self.logger.info(f"Всего собрано продуктов: {total_products}")
            
            if total_products == 0:
                self.logger.warning("Не удалось собрать ни одного продукта")
                return False
                
            # Сохранение в базу данных
            saved_count = await self.save_products(all_products)
            self.logger.info(f"Сохранено в базу данных: {saved_count}")
            
            # Загрузка изображений
            if self.download_images:
                downloaded_count = await self.download_images(all_products)
                self.logger.info(f"Загружено изображений: {downloaded_count}")
                
            # Экспорт данных
            if output_file:
                export_success = await self.export_data(output_file, all_products)
                if export_success:
                    self.logger.info(f"Данные экспортированы в: {output_file}")
                else:
                    self.logger.error("Ошибка экспорта данных")
                    
            # Выводим статистику
            stats = self.storage.get_statistics()
            self.logger.info(f"Статистика базы данных: {stats}")
            
            if self.download_images:
                img_stats = self.image_downloader.get_images_statistics()
                self.logger.info(f"Статистика изображений: {img_stats}")
                
            self.logger.info("Скрапинг завершен успешно")
            return True
            
        except Exception as e:
            self.logger.log_error(e, "основной процесс")
            return False


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
        'sources': ['samokat'],
        'categories': [],
        'limit': None
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
              default=['samokat'], help='Источники для скрапинга')
@click.option('--city', '-c', default='Москва', help='Город для поиска')
@click.option('--coords', help='Координаты (широта,долгота)')
@click.option('--category', '-cat', multiple=True, help='Категории для скрапинга')
@click.option('--limit', '-l', type=int, help='Максимальное количество продуктов')
@click.option('--download-images', '-i', is_flag=True, help='Загружать изображения')
@click.option('--out', '-o', help='Файл для экспорта данных')
@click.option('--config', help='Файл конфигурации YAML')
@click.option('--cookies', help='Файл с cookies (если нужен)')
@click.option('--verbose', '-v', is_flag=True, help='Подробный вывод')
def main(source, city, coords, category, limit, download_images, out, config, cookies, verbose):
    """Скрипт веб-скрейпинга готовой еды из сервисов доставки"""
    
    # Загружаем конфигурацию
    config_dict = load_config(config)
    
    # Обновляем конфигурацию параметрами командной строки
    if source and 'all' not in source:
        config_dict['sources'] = list(source)
    elif 'all' in source:
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

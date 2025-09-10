"""CLI интерфейс для скрейпера готовой еды."""

import asyncio
import sys
from pathlib import Path
from typing import List, Optional
import click
import yaml
from dotenv import load_dotenv
import os
from loguru import logger

from .models import ScrapingConfig
from .scrapers import SamokratScraper, LavkaScraper, VkusvillScraper
from .utils.logger import setup_logger
from .utils.storage import DataExporter
from .utils.image_downloader import ImageDownloader


# Загружаем переменные окружения
load_dotenv()


def load_config(config_file: Optional[str] = None) -> dict:
    """Загрузка конфигурации из файла."""
    if config_file and Path(config_file).exists():
        config_path = Path(config_file)
    else:
        config_path = Path("config.yaml")
    
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    else:
        logger.warning(f"Config file {config_path} not found, using defaults")
        return {}


def get_scraper_class(shop: str):
    """Получение класса скрейпера по названию магазина."""
    scrapers = {
        'samokat': SamokratScraper,
        'lavka': LavkaScraper,
        'vkusvill': VkusvillScraper
    }
    return scrapers.get(shop)


@click.group()
@click.version_option(version="1.0.0")
def cli():
    """Ready Food Scraper - скрейпер готовой еды из сервисов доставки."""
    pass


@cli.command()
@click.option('--shop', 
              type=click.Choice(['samokat', 'lavka', 'vkusvill', 'all']),
              default='all',
              help='Магазин для скрейпинга')
@click.option('--city', 
              default='Москва',
              help='Город для скрейпинга')
@click.option('--address',
              help='Адрес доставки')
@click.option('--out', 
              default='data/foods.csv',
              help='Выходной файл')
@click.option('--format',
              'output_formats',
              multiple=True,
              type=click.Choice(['csv', 'json', 'jsonl', 'parquet']),
              default=['csv'],
              help='Форматы вывода (можно указать несколько)')
@click.option('--download-images/--no-download-images',
              default=False,
              help='Скачивать изображения товаров')
@click.option('--images-dir',
              default='images',
              help='Директория для изображений')
@click.option('--parallel',
              default=3,
              type=int,
              help='Количество параллельных процессов')
@click.option('--headless/--no-headless',
              default=False,  # По умолчанию видимый браузер для прохождения капчи
              help='Запуск браузера в headless режиме')
@click.option('--config',
              help='Путь к файлу конфигурации')
@click.option('--log-level',
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
              default='INFO',
              help='Уровень логирования')
@click.option('--log-file',
              help='Файл для логов')
@click.option('--csv-separator',
              default=',',
              help='Разделитель для CSV файлов')
@click.option('--proxy',
              multiple=True,
              help='Прокси серверы (можно указать несколько)')
@click.option('--max-retries',
              default=3,
              type=int,
              help='Максимальное количество повторных попыток')
@click.option('--delay-min',
              default=1.0,
              type=float,
              help='Минимальная задержка между запросами (сек)')
@click.option('--delay-max',
              default=3.0,
              type=float,
              help='Максимальная задержка между запросами (сек)')
def scrape(**kwargs):
    """Запуск скрейпинга готовой еды."""
    asyncio.run(run_scraping(**kwargs))


async def run_scraping(**kwargs):
    """Основная функция скрейпинга."""
    
    # Загружаем конфигурацию
    config_data = load_config(kwargs.get('config'))
    
    # Настраиваем логирование
    log_level = kwargs.get('log_level') or config_data.get('logging', {}).get('level', 'INFO')
    log_file = kwargs.get('log_file') or config_data.get('logging', {}).get('file')
    
    setup_logger(level=log_level, log_file=log_file)
    
    logger.info("Starting Ready Food Scraper")
    logger.info(f"Configuration: {kwargs}")
    
    # Создаем конфигурацию скрейпинга
    scraping_config = ScrapingConfig(
        city=kwargs.get('city') or config_data.get('defaults', {}).get('city', 'Москва'),
        address=kwargs.get('address') or config_data.get('defaults', {}).get('address'),
        parallel_workers=kwargs.get('parallel') or config_data.get('scraping', {}).get('parallel_workers', 3),
        download_images=kwargs.get('download_images') or config_data.get('export', {}).get('download_images', False),
        output_formats=list(kwargs.get('output_formats')) or config_data.get('export', {}).get('formats', ['csv']),
        max_retries=kwargs.get('max_retries') or config_data.get('scraping', {}).get('max_retries', 3),
        request_delay_min=kwargs.get('delay_min') or config_data.get('scraping', {}).get('request_delay_min', 1.0),
        request_delay_max=kwargs.get('delay_max') or config_data.get('scraping', {}).get('request_delay_max', 3.0),
        headless=kwargs.get('headless') if kwargs.get('headless') is not None else config_data.get('browser', {}).get('headless', True),
        proxy_servers=list(kwargs.get('proxy')) or config_data.get('proxy', {}).get('servers', [])
    )
    
    # Определяем магазины для скрейпинга
    shop = kwargs.get('shop', 'all')
    if shop == 'all':
        shops = ['samokat', 'lavka', 'vkusvill']
    else:
        shops = [shop]
    
    # Запускаем скрейпинг
    results = []
    
    for shop_name in shops:
        try:
            logger.info(f"Starting scraping for {shop_name}")
            
            scraper_class = get_scraper_class(shop_name)
            if not scraper_class:
                logger.error(f"Unknown shop: {shop_name}")
                continue
            
            async with scraper_class(scraping_config) as scraper:
                result = await scraper.scrape()
                results.append(result)
                
                # Проверяем качество данных
                completeness = scraper.get_nutrient_completeness_rate(result.items)
                logger.info(f"Nutrient completeness for {shop_name}: {completeness:.1%}")
                
                if completeness < 0.99:
                    logger.warning(f"Low nutrient completeness for {shop_name}: {completeness:.1%}")
            
        except Exception as e:
            logger.error(f"Failed to scrape {shop_name}: {e}")
            continue
    
    if not results:
        logger.error("No data scraped from any shop")
        sys.exit(1)
    
    # Экспортируем данные
    try:
        logger.info("Exporting scraped data")
        
        output_file = kwargs.get('out', 'data/foods.csv')
        output_path = Path(output_file)
        filename_prefix = output_path.stem
        output_dir = output_path.parent
        
        exporter = DataExporter(str(output_dir))
        exported_files = exporter.export_results(
            results,
            filename_prefix=filename_prefix,
            formats=scraping_config.output_formats,
            csv_separator=kwargs.get('csv_separator', ',')
        )
        
        logger.info("Export completed:")
        for format_name, file_path in exported_files.items():
            logger.info(f"  {format_name.upper()}: {file_path}")
        
        # Скачиваем изображения если нужно
        if scraping_config.download_images:
            await download_images_for_results(results, kwargs.get('images_dir', 'images'))
        
        # Выводим итоговую статистику
        total_items = sum(result.successful for result in results)
        total_found = sum(result.total_found for result in results)
        
        logger.info(f"Scraping completed successfully!")
        logger.info(f"Total items found: {total_found}")
        logger.info(f"Successfully processed: {total_items}")
        logger.info(f"Success rate: {(total_items / total_found * 100):.1f}%")
        
        # Проверяем общую полноту нутриентов
        all_items = []
        for result in results:
            all_items.extend(result.items)
        
        if all_items:
            complete_nutrients = sum(1 for item in all_items if item.has_complete_nutrients())
            overall_completeness = complete_nutrients / len(all_items)
            logger.info(f"Overall nutrient completeness: {overall_completeness:.1%} ({complete_nutrients}/{len(all_items)})")
            
            if overall_completeness < 0.99:
                logger.warning("Nutrient completeness is below 99% threshold!")
        
    except Exception as e:
        logger.error(f"Failed to export data: {e}")
        sys.exit(1)


async def download_images_for_results(results: List, images_dir: str):
    """Скачивание изображений для результатов скрейпинга."""
    logger.info("Starting image download")
    
    # Собираем все URL изображений
    image_urls = []
    for result in results:
        for item in result.items:
            if item.photo_url:
                image_urls.append((item.photo_url, item.id.replace(':', '_'), item.shop))
    
    if not image_urls:
        logger.info("No images to download")
        return
    
    # Скачиваем изображения
    async with ImageDownloader(images_dir) as downloader:
        downloaded = await downloader.download_images(image_urls)
        
        successful_downloads = [d for d in downloaded if d[1]]
        logger.info(f"Downloaded {len(successful_downloads)}/{len(image_urls)} images")


@cli.command()
@click.option('--config',
              help='Путь к файлу конфигурации')
def validate_config(config):
    """Валидация файла конфигурации."""
    try:
        config_data = load_config(config)
        logger.info("Configuration is valid")
        
        # Выводим основные настройки
        logger.info("Configuration summary:")
        if 'defaults' in config_data:
            defaults = config_data['defaults']
            logger.info(f"  Default city: {defaults.get('city', 'Not set')}")
            logger.info(f"  Default address: {defaults.get('address', 'Not set')}")
        
        if 'scraping' in config_data:
            scraping = config_data['scraping']
            logger.info(f"  Parallel workers: {scraping.get('parallel_workers', 'Not set')}")
            logger.info(f"  Max retries: {scraping.get('max_retries', 'Not set')}")
        
        if 'export' in config_data:
            export = config_data['export']
            logger.info(f"  Export formats: {export.get('formats', 'Not set')}")
            logger.info(f"  Download images: {export.get('download_images', 'Not set')}")
        
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        sys.exit(1)


@cli.command()
@click.option('--days',
              default=7,
              type=int,
              help='Удалить изображения старше N дней')
@click.option('--images-dir',
              default='images',
              help='Директория с изображениями')
def cleanup_images(days, images_dir):
    """Очистка старых изображений."""
    try:
        from .utils.image_downloader import ImageDownloader
        
        downloader = ImageDownloader(images_dir)
        removed_count = downloader.cleanup_old_images(days)
        
        logger.info(f"Cleaned up {removed_count} old images")
        
    except Exception as e:
        logger.error(f"Failed to cleanup images: {e}")
        sys.exit(1)


@cli.command()
def install_browser():
    """Установка браузера Playwright."""
    try:
        import subprocess
        
        logger.info("Installing Playwright browser...")
        result = subprocess.run(['playwright', 'install', 'chromium'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("Browser installed successfully")
        else:
            logger.error(f"Failed to install browser: {result.stderr}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Failed to install browser: {e}")
        sys.exit(1)


def main():
    """Точка входа для CLI."""
    cli()


if __name__ == '__main__':
    main()

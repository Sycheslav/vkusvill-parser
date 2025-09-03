"""
Модуль для загрузки изображений продуктов
"""
import os
import asyncio
import aiohttp
import aiofiles
from typing import Optional, List
from urllib.parse import urlparse
import logging
from PIL import Image
import io

# Используем абсолютные импорты
try:
    from src.sources.base import ScrapedProduct
except ImportError:
    try:
        from sources.base import ScrapedProduct
    except ImportError:
        # Для тестирования
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from sources.base import ScrapedProduct


class ImageDownloader:
    """Класс для загрузки изображений продуктов"""
    
    def __init__(self, base_dir: str = "data/images"):
        self.base_dir = base_dir
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Создаем базовую директорию
        os.makedirs(base_dir, exist_ok=True)
        
    async def __aenter__(self):
        """Асинхронный контекстный менеджер - вход"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Асинхронный контекстный менеджер - выход"""
        if self.session:
            await self.session.close()
            
    async def download_image(self, image_url: str, product_id: str, shop: str) -> Optional[str]:
        """
        Загрузка изображения для продукта
        
        Args:
            image_url: URL изображения
            product_id: ID продукта
            shop: Название магазина
            
        Returns:
            Путь к сохраненному файлу или None при ошибке
        """
        if not image_url:
            return None
            
        try:
            # Создаем директорию для магазина
            shop_dir = os.path.join(self.base_dir, shop)
            os.makedirs(shop_dir, exist_ok=True)
            
            # Формируем имя файла
            file_extension = self._get_file_extension(image_url)
            filename = f"{product_id}{file_extension}"
            file_path = os.path.join(shop_dir, filename)
            
            # Проверяем, существует ли файл
            if os.path.exists(file_path):
                self.logger.debug(f"Изображение уже существует: {file_path}")
                return file_path
                
            # Загружаем изображение
            async with self.session.get(image_url) as response:
                if response.status == 200:
                    # Читаем данные изображения
                    image_data = await response.read()
                    
                    # Проверяем, что это действительно изображение
                    if not self._is_valid_image(image_data):
                        self.logger.warning(f"Неверный формат изображения: {image_url}")
                        return None
                        
                    # Сохраняем изображение
                    async with aiofiles.open(file_path, 'wb') as f:
                        await f.write(image_data)
                        
                    # Оптимизируем изображение
                    await self._optimize_image(file_path)
                    
                    self.logger.debug(f"Изображение загружено: {image_url} -> {file_path}")
                    return file_path
                    
                else:
                    self.logger.warning(f"Не удалось загрузить изображение {image_url}: HTTP {response.status}")
                    return None
                    
        except Exception as e:
            self.logger.error(f"Ошибка загрузки изображения {image_url}: {e}")
            return None
            
    async def download_images_for_products(self, products: List[ScrapedProduct]) -> int:
        """
        Загрузка изображений для списка продуктов
        
        Args:
            products: Список продуктов
            
        Returns:
            Количество успешно загруженных изображений
        """
        if not products:
            return 0
            
        self.logger.info(f"Начинаем загрузку изображений для {len(products)} продуктов")
        
        # Группируем продукты по магазинам для лучшей организации
        products_by_shop = {}
        for product in products:
            if product.shop not in products_by_shop:
                products_by_shop[product.shop] = []
            products_by_shop[product.shop].append(product)
            
        total_downloaded = 0
        
        for shop, shop_products in products_by_shop.items():
            self.logger.info(f"Загружаем изображения для магазина {shop} ({len(shop_products)} продуктов)")
            
            # Создаем директорию для магазина
            shop_dir = os.path.join(self.base_dir, shop)
            os.makedirs(shop_dir, exist_ok=True)
            
            # Загружаем изображения с ограничением параллельности
            semaphore = asyncio.Semaphore(5)  # Максимум 5 одновременных загрузок
            
            async def download_with_semaphore(product):
                async with semaphore:
                    return await self.download_image(product.image_url, product.id, product.shop)
                    
            # Создаем задачи для загрузки
            tasks = [download_with_semaphore(product) for product in shop_products if product.image_url]
            
            # Выполняем загрузки
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Подсчитываем успешные загрузки
            for result in results:
                if isinstance(result, Exception):
                    self.logger.error(f"Ошибка загрузки изображения: {result}")
                elif result:
                    total_downloaded += 1
                    
            # Небольшая задержка между магазинами
            await asyncio.sleep(1)
            
        self.logger.info(f"Загрузка изображений завершена. Успешно загружено: {total_downloaded}")
        return total_downloaded
        
    def _get_file_extension(self, image_url: str) -> str:
        """Получение расширения файла из URL"""
        try:
            # Пробуем извлечь расширение из URL
            parsed_url = urlparse(image_url)
            path = parsed_url.path.lower()
            
            # Проверяем известные расширения изображений
            if path.endswith('.jpg') or path.endswith('.jpeg'):
                return '.jpg'
            elif path.endswith('.png'):
                return '.png'
            elif path.endswith('.webp'):
                return '.webp'
            elif path.endswith('.gif'):
                return '.gif'
            elif path.endswith('.bmp'):
                return '.bmp'
            else:
                # По умолчанию используем .jpg
                return '.jpg'
                
        except Exception:
            return '.jpg'
            
    def _is_valid_image(self, image_data: bytes) -> bool:
        """Проверка, что данные являются валидным изображением"""
        try:
            # Пробуем открыть изображение с помощью PIL
            Image.open(io.BytesIO(image_data))
            return True
        except Exception:
            return False
            
    async def _optimize_image(self, file_path: str):
        """Оптимизация изображения"""
        try:
            # Открываем изображение
            with Image.open(file_path) as img:
                # Конвертируем в RGB, если изображение в RGBA
                if img.mode in ('RGBA', 'LA', 'P'):
                    img = img.convert('RGB')
                    
                # Уменьшаем размер, если изображение слишком большое
                max_size = (800, 800)
                if img.size[0] > max_size[0] or img.size[1] > max_size[1]:
                    img.thumbnail(max_size, Image.Resampling.LANCZOS)
                    
                # Сохраняем оптимизированное изображение
                img.save(file_path, 'JPEG', quality=85, optimize=True)
                
        except Exception as e:
            self.logger.debug(f"Не удалось оптимизировать изображение {file_path}: {e}")
            
    def get_image_path(self, product_id: str, shop: str) -> Optional[str]:
        """
        Получение пути к изображению продукта
        
        Args:
            product_id: ID продукта
            shop: Название магазина
            
        Returns:
            Путь к файлу изображения или None, если файл не найден
        """
        shop_dir = os.path.join(self.base_dir, shop)
        
        # Проверяем различные расширения
        for ext in ['.jpg', '.jpeg', '.png', '.webp', '.gif']:
            file_path = os.path.join(shop_dir, f"{product_id}{ext}")
            if os.path.exists(file_path):
                return file_path
                
        return None
        
    def cleanup_orphaned_images(self, existing_product_ids: List[str], shop: str):
        """
        Удаление изображений для несуществующих продуктов
        
        Args:
            existing_product_ids: Список существующих ID продуктов
            shop: Название магазина
        """
        try:
            shop_dir = os.path.join(self.base_dir, shop)
            if not os.path.exists(shop_dir):
                return
                
            # Получаем список всех файлов в директории магазина
            for filename in os.listdir(shop_dir):
                if not filename.endswith(('.jpg', '.jpeg', '.png', '.webp', '.gif')):
                    continue
                    
                # Извлекаем ID продукта из имени файла
                product_id = os.path.splitext(filename)[0]
                
                # Если продукт не существует, удаляем изображение
                if product_id not in existing_product_ids:
                    file_path = os.path.join(shop_dir, filename)
                    try:
                        os.remove(file_path)
                        self.logger.debug(f"Удалено изображение для несуществующего продукта: {file_path}")
                    except Exception as e:
                        self.logger.error(f"Не удалось удалить файл {file_path}: {e}")
                        
        except Exception as e:
            self.logger.error(f"Ошибка очистки изображений для магазина {shop}: {e}")
            
    def get_images_statistics(self) -> dict:
        """Получение статистики по изображениям"""
        try:
            stats = {
                'total_images': 0,
                'total_size_mb': 0,
                'shop_counts': {},
                'format_counts': {}
            }
            
            if not os.path.exists(self.base_dir):
                return stats
                
            # Обходим все директории магазинов
            for shop_dir in os.listdir(self.base_dir):
                shop_path = os.path.join(self.base_dir, shop_dir)
                if not os.path.isdir(shop_path):
                    continue
                    
                shop_images = 0
                shop_size = 0
                
                # Подсчитываем изображения в директории магазина
                for filename in os.listdir(shop_path):
                    if filename.endswith(('.jpg', '.jpeg', '.png', '.webp', '.gif')):
                        file_path = os.path.join(shop_path, filename)
                        try:
                            file_size = os.path.getsize(file_path)
                            shop_images += 1
                            shop_size += file_size
                            
                            # Подсчитываем форматы
                            ext = os.path.splitext(filename)[1].lower()
                            if ext == '.jpeg':
                                ext = '.jpg'
                            stats['format_counts'][ext] = stats['format_counts'].get(ext, 0) + 1
                            
                        except Exception:
                            continue
                            
                stats['shop_counts'][shop_dir] = shop_images
                stats['total_images'] += shop_images
                stats['total_size_mb'] += shop_size / (1024 * 1024)
                
            stats['total_size_mb'] = round(stats['total_size_mb'], 2)
            return stats
            
        except Exception as e:
            self.logger.error(f"Ошибка получения статистики изображений: {e}")
            return {}

"""Утилита для загрузки изображений."""

import asyncio
import aiohttp
from pathlib import Path
from typing import Optional, List, Tuple
from urllib.parse import urlparse
from loguru import logger


class ImageDownloader:
    """Класс для загрузки изображений товаров."""
    
    def __init__(self, images_dir: str = "images", max_concurrent: int = 10):
        """Инициализация загрузчика изображений."""
        self.images_dir = Path(images_dir)
        self.images_dir.mkdir(parents=True, exist_ok=True)
        self.max_concurrent = max_concurrent
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Асинхронный контекст-менеджер - вход."""
        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=30)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Асинхронный контекст-менеджер - выход."""
        if self.session:
            await self.session.close()
    
    async def download_images(
        self,
        image_urls: List[Tuple[str, str, str]]  # (url, item_id, shop)
    ) -> List[Tuple[str, Optional[str]]]:  # (item_id, local_path)
        """Загрузка списка изображений."""
        logger.info(f"Starting download of {len(image_urls)} images")
        
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def download_single(url: str, item_id: str, shop: str) -> Tuple[str, Optional[str]]:
            async with semaphore:
                try:
                    local_path = await self.download_image(url, item_id, shop)
                    return item_id, local_path
                except Exception as e:
                    logger.warning(f"Failed to download image for {item_id}: {e}")
                    return item_id, None
        
        tasks = [download_single(url, item_id, shop) for url, item_id, shop in image_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Фильтруем успешные результаты
        successful_downloads = []
        for result in results:
            if isinstance(result, tuple):
                successful_downloads.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Image download task failed: {result}")
        
        logger.info(f"Successfully downloaded {len([r for r in successful_downloads if r[1]])} images")
        return successful_downloads
    
    async def download_image(self, url: str, item_id: str, shop: str) -> Optional[str]:
        """Загрузка одного изображения."""
        if not url or not self.session:
            return None
        
        try:
            # Создаем директорию для магазина
            shop_dir = self.images_dir / shop
            shop_dir.mkdir(exist_ok=True)
            
            # Определяем расширение файла
            parsed_url = urlparse(url)
            ext = Path(parsed_url.path).suffix.lower()
            
            # Если расширение не определено, пробуем определить по Content-Type
            if not ext or ext not in ['.jpg', '.jpeg', '.png', '.webp', '.gif']:
                ext = '.jpg'  # По умолчанию
            
            filename = f"{item_id}{ext}"
            filepath = shop_dir / filename
            
            # Пропускаем если файл уже существует
            if filepath.exists():
                logger.debug(f"Image already exists: {filepath}")
                return str(filepath.relative_to(self.images_dir.parent))
            
            # Загружаем файл
            async with self.session.get(url) as response:
                if response.status == 200:
                    content = await response.read()
                    
                    # Проверяем, что это действительно изображение
                    if self._is_valid_image(content):
                        with open(filepath, 'wb') as f:
                            f.write(content)
                        
                        logger.debug(f"Downloaded image: {filepath}")
                        return str(filepath.relative_to(self.images_dir.parent))
                    else:
                        logger.warning(f"Invalid image content from {url}")
                else:
                    logger.warning(f"Failed to download image {url}: HTTP {response.status}")
        
        except Exception as e:
            logger.warning(f"Error downloading image {url}: {e}")
        
        return None
    
    def _is_valid_image(self, content: bytes) -> bool:
        """Проверка, что содержимое является изображением."""
        if not content:
            return False
        
        # Проверяем сигнатуры файлов
        image_signatures = [
            b'\xFF\xD8\xFF',  # JPEG
            b'\x89PNG\r\n\x1a\n',  # PNG
            b'GIF87a',  # GIF87a
            b'GIF89a',  # GIF89a
            b'RIFF',  # WebP (начинается с RIFF)
        ]
        
        for signature in image_signatures:
            if content.startswith(signature):
                return True
        
        # Дополнительная проверка для WebP
        if content.startswith(b'RIFF') and b'WEBP' in content[:20]:
            return True
        
        return False
    
    def cleanup_old_images(self, days: int = 7) -> int:
        """Очистка старых изображений."""
        if not self.images_dir.exists():
            return 0
        
        import time
        cutoff_time = time.time() - (days * 24 * 60 * 60)
        removed_count = 0
        
        for image_file in self.images_dir.rglob("*"):
            if image_file.is_file():
                if image_file.stat().st_mtime < cutoff_time:
                    try:
                        image_file.unlink()
                        removed_count += 1
                        logger.debug(f"Removed old image: {image_file}")
                    except Exception as e:
                        logger.warning(f"Failed to remove old image {image_file}: {e}")
        
        logger.info(f"Cleaned up {removed_count} old images")
        return removed_count

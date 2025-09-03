"""
Модуль для настройки логирования
"""
import logging
import logging.handlers
import os
from datetime import datetime
from typing import Optional


def setup_logger(
    name: str = "scraper",
    level: str = "INFO",
    log_file: Optional[str] = None,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
) -> logging.Logger:
    """
    Настройка логгера
    
    Args:
        name: Имя логгера
        level: Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Путь к файлу лога (если None, логируются только в консоль)
        log_format: Формат сообщений лога
        
    Returns:
        Настроенный логгер
    """
    # Создаем логгер
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Очищаем существующие обработчики
    logger.handlers.clear()
    
    # Создаем форматтер
    formatter = logging.Formatter(log_format)
    
    # Обработчик для консоли
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, level.upper()))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Обработчик для файла (если указан)
    if log_file:
        # Создаем директорию для логов, если её нет
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
            
        # Создаем ротационный файловый обработчик
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(getattr(logging, level.upper()))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
    # Отключаем передачу логов родительским логгерам
    logger.propagate = False
    
    return logger


def get_logger(name: str = "scraper") -> logging.Logger:
    """
    Получить существующий логгер или создать новый с настройками по умолчанию
    
    Args:
        name: Имя логгера
        
    Returns:
        Логгер
    """
    logger = logging.getLogger(name)
    
    # Если логгер еще не настроен, настраиваем его
    if not logger.handlers:
        setup_logger(name)
        
    return logger


class ScraperLogger:
    """Класс для удобного логирования в скрейпере"""
    
    def __init__(self, name: str = "scraper", log_file: Optional[str] = None):
        self.logger = setup_logger(name, log_file=log_file)
        
    def info(self, message: str):
        """Информационное сообщение"""
        self.logger.info(message)
        
    def warning(self, message: str):
        """Предупреждение"""
        self.logger.warning(message)
        
    def error(self, message: str):
        """Ошибка"""
        self.logger.error(message)
        
    def debug(self, message: str):
        """Отладочное сообщение"""
        self.logger.debug(message)
        
    def critical(self, message: str):
        """Критическая ошибка"""
        self.logger.critical(message)
        
    def log_progress(self, current: int, total: int, message: str = "Прогресс"):
        """Логирование прогресса"""
        percentage = (current / total) * 100 if total > 0 else 0
        self.info(f"{message}: {current}/{total} ({percentage:.1f}%)")
        
    def log_product_processed(self, product_id: str, shop: str, success: bool = True):
        """Логирование обработки продукта"""
        status = "успешно" if success else "с ошибкой"
        self.debug(f"Продукт {product_id} из {shop} обработан {status}")
        
    def log_category_start(self, category: str, shop: str):
        """Логирование начала обработки категории"""
        self.info(f"Начинаем обработку категории '{category}' в {shop}")
        
    def log_category_complete(self, category: str, shop: str, products_count: int):
        """Логирование завершения обработки категории"""
        self.info(f"Категория '{category}' в {shop} обработана. Найдено товаров: {products_count}")
        
    def log_scraping_start(self, shop: str, categories: list):
        """Логирование начала скрапинга"""
        self.info(f"Начинаем скрапинг {shop}. Категории: {', '.join(categories)}")
        
    def log_scraping_complete(self, shop: str, total_products: int, duration: float):
        """Логирование завершения скрапинга"""
        self.info(f"Скрапинг {shop} завершен. Обработано товаров: {total_products}. Время: {duration:.2f}с")
        
    def log_error(self, error: Exception, context: str = ""):
        """Логирование ошибки с контекстом"""
        error_msg = f"Ошибка в {context}: {str(error)}" if context else str(error)
        self.error(error_msg)
        
    def log_retry(self, attempt: int, max_attempts: int, error: Exception):
        """Логирование повторной попытки"""
        self.warning(f"Попытка {attempt}/{max_attempts} не удалась: {str(error)}")
        
    def log_rate_limit(self, delay: float):
        """Логирование ограничения скорости"""
        self.info(f"Ограничение скорости: ждем {delay:.2f}с")
        
    def log_image_download(self, image_url: str, success: bool, file_path: Optional[str] = None):
        """Логирование загрузки изображения"""
        if success:
            self.debug(f"Изображение загружено: {image_url} -> {file_path}")
        else:
            self.warning(f"Не удалось загрузить изображение: {image_url}")
            
    def log_data_saved(self, format_type: str, file_path: str, records_count: int):
        """Логирование сохранения данных"""
        self.info(f"Данные сохранены в {format_type}: {file_path} ({records_count} записей)")
        
    def log_duplicate_found(self, product_id: str, shop: str):
        """Логирование найденного дубликата"""
        self.debug(f"Найден дубликат: {product_id} в {shop}")
        
    def log_update_existing(self, product_id: str, shop: str, field: str, old_value, new_value):
        """Логирование обновления существующего продукта"""
        self.debug(f"Обновляем {field} для {product_id} в {shop}: {old_value} -> {new_value}")


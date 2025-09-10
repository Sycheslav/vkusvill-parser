"""Настройка логирования для скрейпера."""

import sys
from pathlib import Path
from loguru import logger
from typing import Optional


def setup_logger(
    level: str = "INFO",
    log_file: Optional[str] = None,
    format_string: Optional[str] = None
) -> None:
    """Настройка логирования."""
    
    # Удаляем стандартный обработчик
    logger.remove()
    
    # Формат по умолчанию
    if not format_string:
        format_string = (
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        )
    
    # Консольный вывод
    logger.add(
        sys.stdout,
        level=level,
        format=format_string,
        colorize=True,
        backtrace=True,
        diagnose=True
    )
    
    # Файловый вывод
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.add(
            log_file,
            level=level,
            format=format_string,
            rotation="10 MB",
            retention="7 days",
            compression="zip",
            backtrace=True,
            diagnose=True
        )
    
    logger.info(f"Logger configured with level: {level}")


def get_logger(name: str):
    """Получение логгера с указанным именем."""
    return logger.bind(name=name)

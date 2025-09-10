"""Модуль скрейперов для различных магазинов."""

from .base import BaseScraper
from .samokat import SamokratScraper
from .lavka import LavkaScraper
from .vkusvill import VkusvillScraper

__all__ = [
    'BaseScraper',
    'SamokratScraper', 
    'LavkaScraper',
    'VkusvillScraper'
]

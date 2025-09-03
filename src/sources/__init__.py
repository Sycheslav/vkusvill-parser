# Пакет с источниками данных для скрейпинга
from .base import BaseScraper
from .samokat import SamokatScraper
from .lavka import LavkaScraper
from .vkusvill import VkusvillScraper

__all__ = ['BaseScraper', 'SamokatScraper', 'LavkaScraper', 'VkusvillScraper']

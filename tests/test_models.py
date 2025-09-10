"""Тесты для моделей данных."""

import pytest
from decimal import Decimal
from datetime import datetime

from app.models import FoodItem, ScrapingResult, ScrapingConfig, ValidationIssue


class TestFoodItem:
    """Тесты для модели FoodItem."""
    
    def test_valid_food_item(self):
        """Тест создания валидного товара."""
        item = FoodItem(
            id="samokat:test123",
            name="Салат Цезарь",
            category="готовая еда/салаты",
            price=Decimal("299.50"),
            shop="samokat",
            url="https://samokat.ru/product/test123"
        )
        
        assert item.id == "samokat:test123"
        assert item.name == "Салат Цезарь"
        assert item.price == Decimal("299.50")
        assert item.shop == "samokat"
        assert isinstance(item.scraped_at, datetime)
    
    def test_invalid_shop(self):
        """Тест валидации магазина."""
        with pytest.raises(ValueError, match="Shop must be one of"):
            FoodItem(
                id="invalid:test123",
                name="Салат Цезарь",
                category="готовая еда/салаты", 
                price=Decimal("299.50"),
                shop="invalid_shop",
                url="https://example.com/product/test123"
            )
    
    def test_invalid_name_numeric(self):
        """Тест валидации числового названия."""
        with pytest.raises(ValueError, match="Name cannot be purely numeric"):
            FoodItem(
                id="samokat:test123",
                name="12345",
                category="готовая еда/салаты",
                price=Decimal("299.50"),
                shop="samokat",
                url="https://samokat.ru/product/test123"
            )
    
    def test_invalid_price(self):
        """Тест валидации цены."""
        with pytest.raises(ValueError):
            FoodItem(
                id="samokat:test123",
                name="Салат Цезарь",
                category="готовая еда/салаты",
                price=Decimal("-10"),
                shop="samokat",
                url="https://samokat.ru/product/test123"
            )
    
    def test_has_complete_nutrients(self):
        """Тест проверки полноты нутриентов."""
        # Товар с полными нутриентами
        item_complete = FoodItem(
            id="samokat:test123",
            name="Салат Цезарь",
            category="готовая еда/салаты",
            price=Decimal("299.50"),
            shop="samokat",
            url="https://samokat.ru/product/test123",
            kcal_100g=Decimal("150"),
            protein_100g=Decimal("12.5"),
            fat_100g=Decimal("8.2"),
            carb_100g=Decimal("15.3")
        )
        
        assert item_complete.has_complete_nutrients() is True
        
        # Товар с неполными нутриентами
        item_incomplete = FoodItem(
            id="samokat:test123",
            name="Салат Цезарь",
            category="готовая еда/салаты",
            price=Decimal("299.50"),
            shop="samokat",
            url="https://samokat.ru/product/test123",
            kcal_100g=Decimal("150"),
            protein_100g=Decimal("12.5")
            # fat_100g и carb_100g отсутствуют
        )
        
        assert item_incomplete.has_complete_nutrients() is False
    
    def test_name_strip(self):
        """Тест очистки названия от пробелов."""
        item = FoodItem(
            id="samokat:test123",
            name="  Салат Цезарь  ",
            category="готовая еда/салаты",
            price=Decimal("299.50"),
            shop="samokat",
            url="https://samokat.ru/product/test123"
        )
        
        assert item.name == "Салат Цезарь"


class TestScrapingResult:
    """Тесты для модели ScrapingResult."""
    
    def test_scraping_result(self):
        """Тест создания результата скрейпинга."""
        items = [
            FoodItem(
                id="samokat:test123",
                name="Салат Цезарь",
                category="готовая еда/салаты",
                price=Decimal("299.50"),
                shop="samokat",
                url="https://samokat.ru/product/test123"
            )
        ]
        
        result = ScrapingResult(
            shop="samokat",
            items=items,
            total_found=10,
            successful=1,
            failed=9,
            errors=["Error 1", "Error 2"],
            duration_seconds=120.5
        )
        
        assert result.shop == "samokat"
        assert len(result.items) == 1
        assert result.total_found == 10
        assert result.successful == 1
        assert result.failed == 9
        assert len(result.errors) == 2
        assert result.duration_seconds == 120.5
        assert isinstance(result.scraped_at, datetime)


class TestScrapingConfig:
    """Тесты для модели ScrapingConfig."""
    
    def test_default_config(self):
        """Тест конфигурации по умолчанию."""
        config = ScrapingConfig()
        
        assert config.city == "Москва"
        assert config.parallel_workers == 3
        assert config.download_images is False
        assert config.output_formats == ["csv"]
        assert config.max_retries == 3
        assert config.headless is True
        assert config.proxy_servers == []
    
    def test_custom_config(self):
        """Тест пользовательской конфигурации."""
        config = ScrapingConfig(
            city="Санкт-Петербург",
            address="Невский проспект, 1",
            parallel_workers=5,
            download_images=True,
            output_formats=["csv", "json", "parquet"],
            headless=False
        )
        
        assert config.city == "Санкт-Петербург"
        assert config.address == "Невский проспект, 1"
        assert config.parallel_workers == 5
        assert config.download_images is True
        assert config.output_formats == ["csv", "json", "parquet"]
        assert config.headless is False


class TestValidationIssue:
    """Тесты для модели ValidationIssue."""
    
    def test_validation_issue(self):
        """Тест создания проблемы валидации."""
        issue = ValidationIssue(
            url="https://example.com/product/123",
            issue_type="invalid_price",
            description="Price is negative",
            stage="validation"
        )
        
        assert issue.url == "https://example.com/product/123"
        assert issue.issue_type == "invalid_price"
        assert issue.description == "Price is negative"
        assert issue.stage == "validation"
        assert isinstance(issue.timestamp, datetime)

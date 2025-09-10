"""Тесты для нормализатора данных."""

import pytest
from decimal import Decimal

from app.utils.normalizer import NutrientNormalizer
from app.models import FoodItem


class TestNutrientNormalizer:
    """Тесты для нормализатора нутриентов."""
    
    def setUp(self):
        """Настройка тестов."""
        self.normalizer = NutrientNormalizer()
    
    def test_normalize_name(self):
        """Тест нормализации названий."""
        normalizer = NutrientNormalizer()
        
        # Удаление лишних пробелов
        assert normalizer.normalize_name("  Салат Цезарь  ") == "Салат Цезарь"
        
        # Удаление категорий из названия
        assert normalizer.normalize_name("Готовая еда Салат Цезарь") == "Салат Цезарь"
        
        # Удаление веса из названия
        assert normalizer.normalize_name("Салат Цезарь 200г") == "Салат Цезарь"
        
        # Удаление артикулов
        assert normalizer.normalize_name("Салат Цезарь 123456") == "Салат Цезарь"
    
    def test_normalize_category(self):
        """Тест нормализации категорий."""
        normalizer = NutrientNormalizer()
        
        # Точное совпадение
        assert normalizer.normalize_category("салаты") == "готовая еда/салаты"
        assert normalizer.normalize_category("кулинария") == "готовая еда/кулинария"
        
        # Частичное совпадение
        assert normalizer.normalize_category("горячие блюда") == "готовая еда/горячие блюда"
        
        # Неизвестная категория
        assert normalizer.normalize_category("новая категория") == "готовая еда/новая категория"
        
        # Пустая категория
        assert normalizer.normalize_category("") == "готовая еда"
    
    def test_extract_number_from_text(self):
        """Тест извлечения чисел из текста."""
        normalizer = NutrientNormalizer()
        
        # Целое число
        assert normalizer.extract_number_from_text("150 ккал") == Decimal("150")
        
        # Дробное число с точкой
        assert normalizer.extract_number_from_text("12.5 г белков") == Decimal("12.5")
        
        # Дробное число с запятой
        assert normalizer.extract_number_from_text("8,2 г жиров") == Decimal("8.2")
        
        # Число с единицами измерения
        assert normalizer.extract_number_from_text("250 г") == Decimal("250")
        
        # Отсутствие числа
        assert normalizer.extract_number_from_text("нет данных") is None
        
        # Пустая строка
        assert normalizer.extract_number_from_text("") is None
    
    def test_calculate_price_per_100g(self):
        """Тест расчета цены за 100г."""
        normalizer = NutrientNormalizer()
        
        # Обычный расчет
        price_per_100g = normalizer.calculate_price_per_100g(
            Decimal("200"), Decimal("250")
        )
        assert price_per_100g == Decimal("80.00")
        
        # Нулевой вес
        price_per_100g = normalizer.calculate_price_per_100g(
            Decimal("200"), Decimal("0")
        )
        assert price_per_100g == Decimal("200")
        
        # Отрицательный вес
        price_per_100g = normalizer.calculate_price_per_100g(
            Decimal("200"), Decimal("-10")
        )
        assert price_per_100g == Decimal("200")
    
    def test_normalize_composition(self):
        """Тест нормализации состава."""
        normalizer = NutrientNormalizer()
        
        # Удаление HTML тегов
        composition = normalizer.normalize_composition(
            "<p>Салат, <b>помидоры</b>, курица</p>"
        )
        assert composition == "Салат, помидоры, курица"
        
        # Удаление лишних пробелов
        composition = normalizer.normalize_composition(
            "Салат,    помидоры,\n\n   курица"
        )
        assert composition == "Салат, помидоры, курица"
        
        # Удаление технических фраз
        composition = normalizer.normalize_composition(
            "Состав: Салат, помидоры, курица"
        )
        assert composition == "Салат, помидоры, курица"
        
        # Пустой состав
        assert normalizer.normalize_composition("") == ""
        assert normalizer.normalize_composition(None) == ""
    
    def test_normalize_tags(self):
        """Тест нормализации тегов."""
        normalizer = NutrientNormalizer()
        
        # Обычные теги
        tags = normalizer.normalize_tags(["Острое", "ВЕГЕТАРИАНСКИЙ", "пп!"])
        assert "острое" in tags
        assert "вегетарианское" in tags
        assert "диетическое" in tags
        
        # Удаление дубликатов
        tags = normalizer.normalize_tags(["острое", "Острое", "ОСТРОЕ"])
        assert len(tags) == 1
        assert tags[0] == "острое"
        
        # Пустой список
        assert normalizer.normalize_tags([]) == []
        assert normalizer.normalize_tags(None) == []
        
        # Короткие теги (должны быть отфильтрованы)
        tags = normalizer.normalize_tags(["а", "пп", "вегетарианский"])
        assert "а" not in tags
        assert "диетическое" in tags
        assert "вегетарианское" in tags
    
    def test_normalize_nutrients_conversion(self):
        """Тест конвертации нутриентов с порции на 100г."""
        normalizer = NutrientNormalizer()
        
        # Создаем товар с нутриентами на порцию
        item = FoodItem(
            id="test:123",
            name="Салат",
            category="салаты",
            price=Decimal("200"),
            shop="samokat",
            url="http://example.com",
            portion_g=Decimal("200"),  # порция 200г
            kcal_100g=Decimal("300"),  # калории на порцию
            protein_100g=Decimal("20"),  # белки на порцию
            fat_100g=Decimal("10"),    # жиры на порцию
            carb_100g=Decimal("30")    # углеводы на порцию
        )
        
        # Мокаем метод определения единиц измерения
        normalizer._are_nutrients_per_100g = lambda x: False
        
        normalized_item = normalizer.normalize_nutrients(item)
        
        # Проверяем, что значения пересчитались на 100г
        assert normalized_item.kcal_100g == Decimal("150")  # 300 / 2
        assert normalized_item.protein_100g == Decimal("10.0")  # 20 / 2
        assert normalized_item.fat_100g == Decimal("5.0")   # 10 / 2
        assert normalized_item.carb_100g == Decimal("15.0")  # 30 / 2
    
    def test_normalize_item_full(self):
        """Тест полной нормализации товара."""
        normalizer = NutrientNormalizer()
        
        item = FoodItem(
            id="test:123",
            name="  Готовая еда Салат Цезарь 250г  ",
            category="салаты и закуски",
            price=Decimal("250"),
            shop="samokat",
            url="http://example.com",
            portion_g=Decimal("250"),
            composition="<p>Состав: салат, помидоры, курица</p>",
            tags=["Острое!", "ВЕГЕТАРИАНСКИЙ", "пп"]
        )
        
        normalized_item = normalizer.normalize_item(item)
        
        # Проверяем нормализацию
        assert normalized_item.name == "Салат Цезарь"
        assert normalized_item.category == "готовая еда/салаты"
        assert normalized_item.composition == "салат, помидоры, курица"
        assert "острое" in normalized_item.tags
        assert "вегетарианское" in normalized_item.tags
        assert "диетическое" in normalized_item.tags
        
        # Проверяем расчет цены за 100г
        assert normalized_item.price_per_100g == Decimal("100.00")  # 250 * 100 / 250

"""Тесты для утилит сохранения данных."""

import pytest
import tempfile
import csv
import json
from pathlib import Path
from decimal import Decimal

from app.utils.storage import DataExporter
from app.models import FoodItem, ScrapingResult


class TestDataExporter:
    """Тесты для экспортера данных."""
    
    def setup_method(self):
        """Настройка тестов."""
        self.temp_dir = tempfile.mkdtemp()
        self.exporter = DataExporter(self.temp_dir)
    
    def create_test_items(self):
        """Создание тестовых товаров."""
        return [
            FoodItem(
                id="samokat:test1",
                name="Салат Цезарь",
                category="готовая еда/салаты",
                price=Decimal("299.50"),
                shop="samokat",
                url="https://samokat.ru/product/test1",
                kcal_100g=Decimal("150"),
                protein_100g=Decimal("12.5"),
                fat_100g=Decimal("8.2"),
                carb_100g=Decimal("15.3"),
                tags=["острое", "вегетарианский"]
            ),
            FoodItem(
                id="lavka:test2", 
                name="Суп Борщ",
                category="готовая еда/супы",
                price=Decimal("199.00"),
                shop="lavka",
                url="https://lavka.yandex.ru/product/test2",
                kcal_100g=Decimal("80"),
                protein_100g=Decimal("4.2"),
                fat_100g=Decimal("3.1"),
                carb_100g=Decimal("8.5"),
                tags=["традиционный"]
            )
        ]
    
    def test_export_csv(self):
        """Тест экспорта в CSV."""
        items = self.create_test_items()
        results = [
            ScrapingResult(
                shop="samokat",
                items=items,
                total_found=2,
                successful=2,
                failed=0,
                errors=[],
                duration_seconds=60.0
            )
        ]
        
        exported_files = self.exporter.export_results(
            results, 
            filename_prefix="test",
            formats=["csv"]
        )
        
        assert "csv" in exported_files
        csv_path = Path(exported_files["csv"])
        assert csv_path.exists()
        
        # Проверяем содержимое CSV
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            assert len(rows) == 2
            assert rows[0]['name'] == "Салат Цезарь"
            assert rows[0]['price'] == "299.50"
            assert rows[0]['shop'] == "samokat"
            assert "острое; вегетарианский" in rows[0]['tags']
            
            assert rows[1]['name'] == "Суп Борщ"
            assert rows[1]['shop'] == "lavka"
    
    def test_export_jsonl(self):
        """Тест экспорта в JSON Lines."""
        items = self.create_test_items()
        results = [
            ScrapingResult(
                shop="samokat",
                items=items,
                total_found=2,
                successful=2,
                failed=0,
                errors=[],
                duration_seconds=60.0
            )
        ]
        
        exported_files = self.exporter.export_results(
            results,
            filename_prefix="test", 
            formats=["jsonl"]
        )
        
        assert "jsonl" in exported_files
        jsonl_path = Path(exported_files["jsonl"])
        assert jsonl_path.exists()
        
        # Проверяем содержимое JSONL
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
            assert len(lines) == 2
            
            item1 = json.loads(lines[0])
            assert item1['name'] == "Салат Цезарь"
            assert item1['price'] == 299.5
            assert item1['shop'] == "samokat"
            assert "острое" in item1['tags']
            
            item2 = json.loads(lines[1])
            assert item2['name'] == "Суп Борщ"
            assert item2['shop'] == "lavka"
    
    def test_export_multiple_formats(self):
        """Тест экспорта в несколько форматов."""
        items = self.create_test_items()
        results = [
            ScrapingResult(
                shop="samokat",
                items=items,
                total_found=2,
                successful=2,
                failed=0,
                errors=[],
                duration_seconds=60.0
            )
        ]
        
        exported_files = self.exporter.export_results(
            results,
            filename_prefix="test",
            formats=["csv", "json"]
        )
        
        assert "csv" in exported_files
        assert "jsonl" in exported_files
        
        # Проверяем, что оба файла созданы
        assert Path(exported_files["csv"]).exists()
        assert Path(exported_files["jsonl"]).exists()
    
    def test_export_report(self):
        """Тест создания отчета."""
        items = self.create_test_items()
        results = [
            ScrapingResult(
                shop="samokat",
                items=[items[0]],
                total_found=1,
                successful=1,
                failed=0,
                errors=[],
                duration_seconds=30.0
            ),
            ScrapingResult(
                shop="lavka",
                items=[items[1]],
                total_found=1,
                successful=1,
                failed=0,
                errors=[],
                duration_seconds=25.0
            )
        ]
        
        exported_files = self.exporter.export_results(
            results,
            filename_prefix="test",
            formats=["csv"]
        )
        
        assert "report" in exported_files
        report_path = Path(exported_files["report"])
        assert report_path.exists()
        
        # Проверяем содержимое отчета
        with open(report_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
            assert "Отчет о скрейпинге готовой еды" in content
            assert "Всего найдено товаров**: 2" in content
            assert "Успешно обработано**: 2" in content
            assert "samokat" in content.lower()
            assert "lavka" in content.lower()
            assert "готовая еда/салаты" in content
            assert "готовая еда/супы" in content
    
    def test_export_empty_results(self):
        """Тест экспорта пустых результатов."""
        results = [
            ScrapingResult(
                shop="samokat",
                items=[],
                total_found=0,
                successful=0,
                failed=0,
                errors=[],
                duration_seconds=10.0
            )
        ]
        
        exported_files = self.exporter.export_results(
            results,
            filename_prefix="empty",
            formats=["csv"]
        )
        
        # При пустых результатах файлы не создаются
        assert exported_files == {}
        assert "csv" not in exported_files or not Path(exported_files["csv"]).exists()
    
    def test_convert_decimals_to_float(self):
        """Тест конвертации Decimal в float."""
        test_data = {
            'price': Decimal('299.50'),
            'nested': {
                'kcal': Decimal('150'),
                'string': 'test'
            },
            'list': [Decimal('10.5'), 'text', 42]
        }
        
        converted = self.exporter._convert_decimals_to_float(test_data)
        
        assert converted['price'] == 299.5
        assert converted['nested']['kcal'] == 150.0
        assert converted['nested']['string'] == 'test'
        assert converted['list'][0] == 10.5
        assert converted['list'][1] == 'text'
        assert converted['list'][2] == 42

"""
Тесты для модуля нормализации данных
"""
import pytest
from src.utils.normalizer import DataNormalizer
from src.sources.base import ScrapedProduct


class TestDataNormalizer:
    """Тесты для класса DataNormalizer"""
    
    def setup_method(self):
        """Настройка перед каждым тестом"""
        self.normalizer = DataNormalizer()
        
    def test_normalize_kcal_per_portion(self):
        """Тест нормализации калорий с порции на 100г"""
        product = ScrapedProduct(
            id="test1",
            name="Тестовый продукт",
            category="Салат",
            kcal_100g=300,  # калории на порцию
            portion_g=150    # масса порции 150г
        )
        
        normalized = self.normalizer.normalize_product(product)
        expected_kcal = (300 / 150) * 100  # 200 ккал на 100г
        
        assert normalized.kcal_100g == pytest.approx(expected_kcal, rel=1e-2)
        
    def test_normalize_nutrition_per_portion(self):
        """Тест нормализации БЖУ с порции на 100г"""
        product = ScrapedProduct(
            id="test2",
            name="Тестовый продукт",
            category="Суп",
            protein_100g=25,  # белки на порцию
            fat_100g=15,       # жиры на порцию
            carb_100g=30,      # углеводы на порцию
            portion_g=200      # масса порции 200г
        )
        
        normalized = self.normalizer.normalize_product(product)
        
        assert normalized.protein_100g == pytest.approx(12.5, rel=1e-2)  # 25/200*100
        assert normalized.fat_100g == pytest.approx(7.5, rel=1e-2)       # 15/200*100
        assert normalized.carb_100g == pytest.approx(15.0, rel=1e-2)     # 30/200*100
        
    def test_normalize_weight_kg_to_g(self):
        """Тест нормализации массы из килограммов в граммы"""
        product = ScrapedProduct(
            id="test3",
            name="Тестовый продукт",
            category="Горячее",
            portion_g=1.5,  # 1.5 кг
            weight_declared_g=1.5
        )
        
        normalized = self.normalizer.normalize_product(product)
        
        assert normalized.portion_g == 1.5  # уже в граммах
        assert normalized.weight_declared_g == 1.5
        
    def test_normalize_price(self):
        """Тест нормализации цены"""
        product = ScrapedProduct(
            id="test4",
            name="Тестовый продукт",
            category="Десерт",
            price=299.99
        )
        
        normalized = self.normalizer.normalize_product(product)
        
        assert normalized.price == 299.99  # цена остается без изменений
        
    def test_calculate_unit_price(self):
        """Тест вычисления цены за 100г"""
        product = ScrapedProduct(
            id="test5",
            name="Тестовый продукт",
            category="Мясо",
            price=450,
            portion_g=300
        )
        
        normalized = self.normalizer.normalize_product(product)
        expected_unit_price = (450 / 300) * 100  # 150 руб за 100г
        
        assert normalized.unit_price == pytest.approx(expected_unit_price, rel=1e-2)
        
    def test_normalize_tags(self):
        """Тест нормализации тегов"""
        product = ScrapedProduct(
            id="test6",
            name="Тестовый продукт",
            category="Салат",
            tags=["ОСТРОЕ", " Веган ", "Хит", "Хит"]  # дубликаты и лишние пробелы
        )
        
        normalized = self.normalizer.normalize_product(product)
        
        expected_tags = ["острое", "веган", "хит"]
        assert normalized.tags == expected_tags
        
    def test_normalize_allergens(self):
        """Тест нормализации аллергенов"""
        product = ScrapedProduct(
            id="test7",
            name="Тестовый продукт",
            category="Выпечка",
            allergens=["ГЛЮТЕН", " МОЛОКО ", "Орехи"]
        )
        
        normalized = self.normalizer.normalize_product(product)
        
        expected_allergens = ["глютен", "молоко", "орехи"]
        assert normalized.allergens == expected_allergens
        
    def test_normalize_composition(self):
        """Тест нормализации состава"""
        product = ScrapedProduct(
            id="test8",
            name="Тестовый продукт",
            category="Салат",
            composition="  Томаты,  огурцы,  лук  "  # лишние пробелы
        )
        
        normalized = self.normalizer.normalize_product(product)
        
        expected_composition = "томаты, огурцы, лук"
        assert normalized.composition == expected_composition
        
    def test_normalize_category(self):
        """Тест нормализации категории"""
        test_cases = [
            ("готовые блюда", "Готовая еда"),
            ("кулинария", "Кулинария"),
            ("салаты", "Салаты"),
            ("супы", "Супы"),
            ("горячие блюда", "Горячие блюда"),
            ("неизвестная категория", "Неизвестная категория")
        ]
        
        for input_category, expected in test_cases:
            product = ScrapedProduct(
                id=f"test_{input_category}",
                name="Тестовый продукт",
                category=input_category
            )
            
            normalized = self.normalizer.normalize_product(product)
            assert normalized.category == expected
            
    def test_normalize_brand(self):
        """Тест нормализации бренда"""
        product = ScrapedProduct(
            id="test9",
            name="Тестовый продукт",
            category="Молочка",
            brand="  НЕСТЛЕ  "  # лишние пробелы
        )
        
        normalized = self.normalizer.normalize_product(product)
        
        assert normalized.brand == "Нестле"  # первая буква заглавная
        
    def test_normalize_image_url(self):
        """Тест нормализации URL изображения"""
        product = ScrapedProduct(
            id="test10",
            name="Тестовый продукт",
            category="Салат",
            image_url="  example.com/image.jpg  "  # лишние пробелы
        )
        
        normalized = self.normalizer.normalize_product(product)
        
        assert normalized.image_url == "https://example.com/image.jpg"
        
    def test_extract_nutrition_from_text(self):
        """Тест извлечения БЖУ из текста"""
        text = "Энергетическая ценность: 250 ккал, белки: 12 г, жиры: 8 г, углеводы: 35 г"
        
        nutrition = self.normalizer.extract_nutrition_from_text(text)
        
        assert nutrition['kcal'] == 250
        assert nutrition['protein'] == 12
        assert nutrition['fat'] == 8
        assert nutrition['carb'] == 35
        
    def test_extract_weight_from_text(self):
        """Тест извлечения массы из текста"""
        test_cases = [
            ("Масса: 250 г", 250),
            ("Вес: 1.5 кг", 1500),
            ("Размер: 500g", 500),
            ("Объем: 2 кг", 2000)
        ]
        
        for text, expected in test_cases:
            weight = self.normalizer.extract_weight_from_text(text)
            assert weight == expected
            
    def test_extract_price_from_text(self):
        """Тест извлечения цены из текста"""
        test_cases = [
            ("Цена: 299 руб", 299),
            ("Стоимость: 1 250 ₽", 1250),
            ("Цена: 99,90", 99.9),
            ("Стоимость: 1,250.50", 1250.5)
        ]
        
        for text, expected in test_cases:
            price = self.normalizer.extract_price_from_text(text)
            assert price == expected
            
    def test_normalize_product_with_none_values(self):
        """Тест нормализации продукта с пустыми значениями"""
        product = ScrapedProduct(
            id="test11",
            name="Тестовый продукт",
            category="Салат",
            kcal_100g=None,
            protein_100g=None,
            fat_100g=None,
            carb_100g=None,
            portion_g=None,
            price=None,
            tags=[],
            allergens=[],
            extra={}
        )
        
        normalized = self.normalizer.normalize_product(product)
        
        # Проверяем, что None значения остаются None
        assert normalized.kcal_100g is None
        assert normalized.protein_100g is None
        assert normalized.fat_100g is None
        assert normalized.carb_100g is None
        assert normalized.portion_g is None
        assert normalized.price is None
        assert normalized.tags == []
        assert normalized.allergens == []
        assert normalized.extra == {}
        
    def test_normalize_product_complete(self):
        """Тест полной нормализации продукта"""
        product = ScrapedProduct(
            id="test12",
            name="  Тестовый продукт  ",
            category="готовые блюда",
            kcal_100g=400,
            protein_100g=20,
            fat_100g=15,
            carb_100g=45,
            portion_g=250,
            price=350,
            shop="samokat",
            tags=["ОСТРОЕ", "Хит"],
            composition="  Мясо, овощи, специи  ",
            url="https://example.com/product",
            image_url="example.com/image.jpg",
            brand="  БРЕНД  ",
            allergens=["ГЛЮТЕН"],
            extra={"feature": "value"}
        )
        
        normalized = self.normalizer.normalize_product(product)
        
        # Проверяем нормализацию
        assert normalized.name == "Тестовый продукт"
        assert normalized.category == "Готовая еда"
        assert normalized.kcal_100g == 160  # (400/250)*100
        assert normalized.protein_100g == 8  # (20/250)*100
        assert normalized.fat_100g == 6      # (15/250)*100
        assert normalized.carb_100g == 18    # (45/250)*100
        assert normalized.portion_g == 250
        assert normalized.price == 350
        assert normalized.unit_price == 140  # (350/250)*100
        assert normalized.tags == ["острое", "хит"]
        assert normalized.composition == "мясо, овощи, специи"
        assert normalized.brand == "Бренд"
        assert normalized.allergens == ["глютен"]
        assert normalized.extra == {"feature": "value"}
        assert normalized.image_url == "https://example.com/image.jpg"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

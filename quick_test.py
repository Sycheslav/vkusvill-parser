#!/usr/bin/env python3
"""
Быстрый тест скрейпера готовой еды
"""
import sys
import os

# Добавляем src в путь для импорта
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    from utils.normalizer import DataNormalizer
    from sources.base import ScrapedProduct
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    print("🔧 Убедитесь, что зависимости установлены:")
    print("   pip3 install -r requirements.txt")
    sys.exit(1)


def test_normalizer():
    """Тест нормализатора данных"""
    print("🧪 Тестируем нормализатор данных...")
    
    normalizer = DataNormalizer()
    
    # Создаем тестовый продукт
    product = ScrapedProduct(
        id="test_product",
        name="  Тестовый салат  ",
        category="готовые блюда",
        kcal_100g=300,  # калории на порцию
        protein_100g=15, # белки на порцию
        fat_100g=10,     # жиры на порцию
        carb_100g=25,    # углеводы на порцию
        portion_g=200,    # масса порции 200г
        price=299.99,
        shop="samokat",
        tags=["ОСТРОЕ", "Хит", "Хит"],  # дубликат
        composition="  Томаты, огурцы, лук  ",
        brand="  ТЕСТОВЫЙ БРЕНД  ",
        allergens=["ГЛЮТЕН", " МОЛОКО "]
    )
    
    print(f"📦 Исходный продукт:")
    print(f"   Название: '{product.name}'")
    print(f"   Категория: '{product.category}'")
    print(f"   Калории: {product.kcal_100g} ккал на {product.portion_g}г")
    print(f"   БЖУ: {product.protein_100g}г белков, {product.fat_100g}г жиров, {product.carb_100g}г углеводов")
    print(f"   Цена: {product.price} руб")
    print(f"   Теги: {product.tags}")
    print(f"   Состав: '{product.composition}'")
    print(f"   Бренд: '{product.brand}'")
    print(f"   Аллергены: {product.allergens}")
    
    # Нормализуем продукт
    normalized = normalizer.normalize_product(product)
    
    print(f"\n✨ Нормализованный продукт:")
    print(f"   Название: '{normalized.name}'")
    print(f"   Категория: '{normalized.category}'")
    print(f"   Калории: {normalized.kcal_100g} ккал на 100г")
    print(f"   БЖУ: {normalized.protein_100g}г белков, {normalized.fat_100g}г жиров, {normalized.carb_100g}г углеводов на 100г")
    print(f"   Цена: {normalized.price} руб")
    print(f"   Цена за 100г: {normalized.unit_price} руб")
    print(f"   Теги: {normalized.tags}")
    print(f"   Состав: '{normalized.composition}'")
    print(f"   Бренд: '{normalized.brand}'")
    print(f"   Аллергены: {normalized.allergens}")
    
    # Проверяем корректность нормализации
    expected_kcal = (300 / 200) * 100  # 150 ккал на 100г
    expected_protein = (15 / 200) * 100  # 7.5г белков на 100г
    expected_fat = (10 / 200) * 100      # 5г жиров на 100г
    expected_carb = (25 / 200) * 100     # 12.5г углеводов на 100г
    expected_unit_price = (299.99 / 200) * 100  # ~150 руб за 100г
    
    print(f"\n✅ Проверка нормализации:")
    print(f"   Калории: {normalized.kcal_100g} ≈ {expected_kcal} ✓")
    print(f"   Белки: {normalized.protein_100g} ≈ {expected_protein} ✓")
    print(f"   Жиры: {normalized.fat_100g} ≈ {expected_fat} ✓")
    print(f"   Углеводы: {normalized.carb_100g} ≈ {expected_carb} ✓")
    print(f"   Цена за 100г: {normalized.unit_price} ≈ {expected_unit_price:.2f} ✓")
    
    print("\n🎉 Тест нормализатора пройден успешно!")


def test_text_extraction():
    """Тест извлечения данных из текста"""
    print("\n🔍 Тестируем извлечение данных из текста...")
    
    normalizer = DataNormalizer()
    
    # Тест извлечения БЖУ
    nutrition_text = "Энергетическая ценность: 180 ккал, белки: 8.5 г, жиры: 6.2 г, углеводы: 22.1 г"
    nutrition = normalizer.extract_nutrition_from_text(nutrition_text)
    
    print(f"📊 Извлечение БЖУ из текста:")
    print(f"   Текст: '{nutrition_text}'")
    print(f"   Результат: {nutrition}")
    
    # Тест извлечения массы
    weight_text = "Масса нетто: 350 г"
    weight = normalizer.extract_weight_from_text(weight_text)
    
    print(f"\n⚖️ Извлечение массы из текста:")
    print(f"   Текст: '{weight_text}'")
    print(f"   Результат: {weight} г")
    
    # Тест извлечения цены
    price_text = "Цена: 1 250 ₽"
    price = normalizer.extract_price_from_text(price_text)
    
    print(f"\n💰 Извлечение цены из текста:")
    print(f"   Текст: '{price_text}'")
    print(f"   Результат: {price} руб")
    
    print("\n🎉 Тест извлечения данных пройден успешно!")


def main():
    """Основная функция"""
    print("🚀 Быстрый тест скрейпера готовой еды")
    print("=" * 50)
    
    try:
        # Тестируем нормализатор
        test_normalizer()
        
        # Тестируем извлечение данных
        test_text_extraction()
        
        print("\n" + "=" * 50)
        print("🎯 Все тесты пройдены успешно!")
        print("✅ Скрипт готов к использованию")
        
        print("\n📋 Для запуска скрейпера используйте:")
        print("   python3 -m src.main --source samokat --city 'Москва' --out data.csv")
        print("   python3 -m src.main --source all --download-images --out foods.sqlite")
        
    except Exception as e:
        print(f"\n❌ Ошибка при тестировании: {e}")
        print("🔧 Проверьте установку зависимостей:")
        print("   pip3 install -r requirements.txt")
        print("   playwright install chromium")
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main())

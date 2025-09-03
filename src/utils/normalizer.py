"""
Модуль для нормализации данных продуктов
"""
import re
import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

# Используем абсолютные импорты
try:
    from src.sources.base import ScrapedProduct
except ImportError:
    try:
        from sources.base import ScrapedProduct
    except ImportError:
        # Для тестирования
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from sources.base import ScrapedProduct


class DataNormalizer:
    """Класс для нормализации данных продуктов"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def normalize_product(self, product: ScrapedProduct) -> ScrapedProduct:
        """Нормализация всех полей продукта"""
        try:
            # Нормализуем числовые поля
            product.kcal_100g = self._normalize_kcal(product.kcal_100g, product.portion_g)
            product.protein_100g = self._normalize_nutrition(product.protein_100g, product.portion_g)
            product.fat_100g = self._normalize_nutrition(product.fat_100g, product.portion_g)
            product.carb_100g = self._normalize_nutrition(product.carb_100g, product.portion_g)
            
            # Нормализуем массу порции
            product.portion_g = self._normalize_weight(product.portion_g)
            product.weight_declared_g = self._normalize_weight(product.weight_declared_g)
            
            # Нормализуем цену
            product.price = self._normalize_price(product.price)
            
            # Вычисляем цену за 100г
            product.unit_price = self._calculate_unit_price(product.price, product.portion_g)
            
            # Нормализуем теги и аллергены
            product.tags = self._normalize_tags(product.tags)
            product.allergens = self._normalize_allergens(product.allergens)
            
            # Нормализуем состав
            product.composition = self._normalize_composition(product.composition)
            
            # Нормализуем категорию
            product.category = self._normalize_category(product.category)
            
            # Нормализуем бренд
            product.brand = self._normalize_brand(product.brand)
            
            # Нормализуем URL изображения
            product.image_url = self._normalize_image_url(product.image_url)
            
            self.logger.debug(f"Продукт {product.id} нормализован")
            return product
            
        except Exception as e:
            self.logger.error(f"Ошибка нормализации продукта {product.id}: {e}")
            return product
            
    def _normalize_kcal(self, kcal: Optional[float], portion_g: Optional[float]) -> Optional[float]:
        """Нормализация калорий к значению на 100г"""
        if kcal is None:
            return None
            
        # Если калории указаны на порцию, а не на 100г
        if portion_g and portion_g > 0:
            # Предполагаем, что калории указаны на порцию, если они больше 500
            if kcal > 500:
                kcal = (kcal / portion_g) * 100
                
        return round(kcal, 1) if kcal else None
        
    def _normalize_nutrition(self, value: Optional[float], portion_g: Optional[float]) -> Optional[float]:
        """Нормализация БЖУ к значению на 100г"""
        if value is None:
            return None
            
        # Если БЖУ указаны на порцию, а не на 100г
        if portion_g and portion_g > 0:
            # Предполагаем, что БЖУ указаны на порцию, если сумма больше 50г
            if value > 50:
                value = (value / portion_g) * 100
                
        return round(value, 1) if value else None
        
    def _normalize_weight(self, weight: Optional[float]) -> Optional[float]:
        """Нормализация массы в граммы"""
        if weight is None:
            return None
            
        # Если вес указан в килограммах (больше 1000), переводим в граммы
        if weight > 1000:
            weight = weight / 1000
            
        return round(weight, 1) if weight else None
        
    def _normalize_price(self, price: Optional[float]) -> Optional[float]:
        """Нормализация цены"""
        if price is None:
            return None
            
        # Округляем до копеек
        return round(price, 2) if price else None
        
    def _calculate_unit_price(self, price: Optional[float], portion_g: Optional[float]) -> Optional[float]:
        """Вычисление цены за 100г"""
        if price is None or portion_g is None or portion_g <= 0:
            return None
            
        unit_price = (price / portion_g) * 100
        return round(unit_price, 2)
        
    def _normalize_tags(self, tags: List[str]) -> List[str]:
        """Нормализация тегов"""
        if not tags:
            return []
            
        normalized_tags = []
        for tag in tags:
            if tag:
                # Приводим к нижнему регистру и убираем лишние пробелы
                normalized_tag = tag.strip().lower()
                if normalized_tag and normalized_tag not in normalized_tags:
                    normalized_tags.append(normalized_tag)
                    
        return normalized_tags
        
    def _normalize_allergens(self, allergens: List[str]) -> List[str]:
        """Нормализация аллергенов"""
        if not allergens:
            return []
            
        normalized_allergens = []
        for allergen in allergens:
            if allergen:
                # Приводим к нижнему регистру и убираем лишние пробелы
                normalized_allergen = allergen.strip().lower()
                if normalized_allergen and normalized_allergen not in normalized_allergens:
                    normalized_allergens.append(normalized_allergen)
                    
        return normalized_allergens
        
    def _normalize_composition(self, composition: str) -> str:
        """Нормализация состава"""
        if not composition:
            return ""
            
        # Убираем лишние пробелы и переносы строк
        composition = re.sub(r'\s+', ' ', composition.strip())
        
        # Приводим к нижнему регистру для единообразия
        composition = composition.lower()
        
        return composition
        
    def _normalize_category(self, category: str) -> str:
        """Нормализация категории"""
        if not category:
            return "Готовая еда"
            
        # Приводим к нижнему регистру
        category = category.strip().lower()
        
        # Стандартизируем названия категорий
        category_mapping = {
            'готов': 'Готовая еда',
            'кулинар': 'Кулинария',
            'салат': 'Салаты',
            'суп': 'Супы',
            'блюд': 'Горячие блюда',
            'еда': 'Готовая еда',
            'кухня': 'Кулинария'
        }
        
        for keyword, standard_name in category_mapping.items():
            if keyword in category:
                return standard_name
                
        return category.capitalize()
        
    def _normalize_brand(self, brand: Optional[str]) -> Optional[str]:
        """Нормализация бренда"""
        if not brand:
            return None
            
        # Убираем лишние пробелы и приводим к правильному регистру
        brand = brand.strip()
        if brand:
            # Первая буква заглавная, остальные строчные
            brand = brand[0].upper() + brand[1:].lower()
            
        return brand
        
    def _normalize_image_url(self, image_url: str) -> str:
        """Нормализация URL изображения"""
        if not image_url:
            return ""
            
        # Убираем лишние пробелы
        image_url = image_url.strip()
        
        # Проверяем, что URL начинается с http
        if image_url and not image_url.startswith(('http://', 'https://')):
            # Если это относительный URL, добавляем базовый домен
            image_url = f"https://{image_url}"
            
        return image_url
        
    def extract_nutrition_from_text(self, text: str) -> Dict[str, Optional[float]]:
        """Извлечение информации о БЖУ из текста"""
        if not text:
            return {}
            
        nutrition = {}
        
        # Ищем калории
        kcal_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:ккал|кал|kcal)', text, re.I)
        if kcal_match:
            nutrition['kcal'] = float(kcal_match.group(1).replace(',', '.'))
            
        # Ищем белки
        protein_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:г|g)\s*(?:бел|протеин|protein)', text, re.I)
        if protein_match:
            nutrition['protein'] = float(protein_match.group(1).replace(',', '.'))
            
        # Ищем жиры
        fat_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:г|g)\s*(?:жир|fat)', text, re.I)
        if fat_match:
            nutrition['fat'] = float(fat_match.group(1).replace(',', '.'))
            
        # Ищем углеводы
        carb_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:г|g)\s*(?:углев|carb)', text, re.I)
        if carb_match:
            nutrition['carb'] = float(carb_match.group(1).replace(',', '.'))
            
        return nutrition
        
    def extract_weight_from_text(self, text: str) -> Optional[float]:
        """Извлечение массы из текста"""
        if not text:
            return None
            
        # Ищем массу в граммах или килограммах
        weight_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:г|g|кг|kg)', text, re.I)
        if weight_match:
            weight = float(weight_match.group(1).replace(',', '.'))
            
            # Если указано в килограммах, переводим в граммы
            if 'кг' in text.lower() or 'kg' in text.lower():
                weight *= 1000
                
            return weight
            
        return None
        
    def extract_price_from_text(self, text: str) -> Optional[float]:
        """Извлечение цены из текста"""
        if not text:
            return None
            
        # Убираем лишние символы и извлекаем число
        price_match = re.search(r'(\d+(?:[.,]\d+)?)', text.replace(' ', ''))
        if price_match:
            return float(price_match.group(1).replace(',', '.'))
            
        return None

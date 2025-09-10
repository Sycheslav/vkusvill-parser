"""Нормализатор данных о питательных веществах."""

import re
from typing import Optional, Dict, Any
from decimal import Decimal, ROUND_HALF_UP
from loguru import logger

from ..models import FoodItem


class NutrientNormalizer:
    """Класс для нормализации данных о питательных веществах."""
    
    def __init__(self):
        """Инициализация нормализатора."""
        # Паттерны для извлечения чисел из текста
        self.number_pattern = re.compile(r'(\d+(?:[.,]\d+)?)')
        
        # Паттерны для очистки единиц измерения
        self.unit_patterns = {
            'kcal': re.compile(r'\b(?:ккал|kcal|кал|cal)\b', re.IGNORECASE),
            'gram': re.compile(r'\b(?:г|g|гр|gram|грамм)\b', re.IGNORECASE),
            'per_100g': re.compile(r'\b(?:на\s*100\s*г|per\s*100\s*g|в\s*100\s*г)\b', re.IGNORECASE),
            'per_portion': re.compile(r'\b(?:на\s*порцию|per\s*portion|в\s*порции)\b', re.IGNORECASE)
        }
        
        # Словарь для нормализации категорий
        self.category_mapping = {
            'готовая еда': 'готовая еда',
            'готовые блюда': 'готовая еда',
            'кулинария': 'готовая еда/кулинария',
            'салаты': 'готовая еда/салаты',
            'супы': 'готовая еда/супы',
            'горячие блюда': 'готовая еда/горячие блюда',
            'вторые блюда': 'готовая еда/вторые блюда',
            'первые блюда': 'готовая еда/первые блюда',
            'закуски': 'готовая еда/закуски',
            'выпечка': 'готовая еда/выпечка',
            'десерты': 'готовая еда/десерты'
        }
    
    def normalize_item(self, item: FoodItem) -> FoodItem:
        """Нормализация данных товара."""
        try:
            # Нормализуем название
            item.name = self.normalize_name(item.name)
            
            # Нормализуем категорию
            item.category = self.normalize_category(item.category)
            
            # Нормализуем нутриенты
            item = self.normalize_nutrients(item)
            
            # Нормализуем состав
            if item.composition:
                item.composition = self.normalize_composition(item.composition)
            
            # Нормализуем теги
            if item.tags:
                item.tags = self.normalize_tags(item.tags)
            
            # Рассчитываем цену за 100г если возможно
            if item.portion_g and item.price:
                item.price_per_100g = self.calculate_price_per_100g(
                    item.price, item.portion_g
                )
            
            return item
            
        except Exception as e:
            logger.warning(f"Failed to normalize item {item.id}: {e}")
            return item
    
    def normalize_name(self, name: str) -> str:
        """Нормализация названия товара."""
        if not name:
            return ""
        
        # Удаляем лишние пробелы
        name = re.sub(r'\s+', ' ', name.strip())
        
        # Удаляем категории из названия
        patterns_to_remove = [
            r'\bготовая\s+еда\b',
            r'\bготовые?\s+блюда?\b',
            r'\bкулинария\b',
            r'\d+\s*г\b',  # вес в названии
            r'\d+\s*мл\b',  # объем в названии
        ]
        
        for pattern in patterns_to_remove:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)
        
        # Удаляем артикулы и коды товара
        name = re.sub(r'\b\d{6,}\b', '', name)  # длинные числа
        name = re.sub(r'\b[A-Z]{2,}\d+\b', '', name)  # коды типа ABC123
        
        # Очищаем от лишних символов и пробелов
        name = re.sub(r'[^\w\s\-\(\)\,\.]', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name
    
    def normalize_category(self, category: str) -> str:
        """Нормализация категории."""
        if not category:
            return "готовая еда"
        
        category_lower = category.lower().strip()
        
        # Ищем точное совпадение
        if category_lower in self.category_mapping:
            return self.category_mapping[category_lower]
        
        # Ищем частичные совпадения
        for key, value in self.category_mapping.items():
            if key in category_lower or category_lower in key:
                return value
        
        # Если не найдено, добавляем префикс
        return f"готовая еда/{category}"
    
    def normalize_nutrients(self, item: FoodItem) -> FoodItem:
        """Нормализация нутриентов."""
        # Если есть данные на порцию и известен вес порции - пересчитываем на 100г
        if item.portion_g and item.portion_g > 0:
            # Проверяем, не указаны ли уже значения на 100г
            if self._are_nutrients_per_100g(item):
                # Нутриенты уже на 100г, рассчитываем на порцию
                pass
            else:
                # Нутриенты на порцию, пересчитываем на 100г
                item = self._convert_nutrients_to_100g(item)
        
        # Округляем значения
        item = self._round_nutrients(item)
        
        return item
    
    def _are_nutrients_per_100g(self, item: FoodItem) -> bool:
        """Определяем, указаны ли нутриенты на 100г или на порцию."""
        # Эвристика: если калорийность слишком высока для порции, 
        # вероятно указана на 100г
        if item.kcal_100g and item.portion_g:
            expected_kcal_per_portion = float(item.kcal_100g) * float(item.portion_g) / 100
            actual_kcal = float(item.kcal_100g)
            
            # Если актуальные калории близки к ожидаемым на порцию,
            # значит они указаны на порцию
            if abs(actual_kcal - expected_kcal_per_portion) < actual_kcal * 0.2:
                return False
        
        return True  # По умолчанию считаем, что на 100г
    
    def _convert_nutrients_to_100g(self, item: FoodItem) -> FoodItem:
        """Конвертация нутриентов с порции на 100г."""
        if not item.portion_g or item.portion_g <= 0:
            return item
        
        conversion_factor = 100 / float(item.portion_g)
        
        if item.kcal_100g:
            item.kcal_100g = Decimal(str(float(item.kcal_100g) * conversion_factor))
        
        if item.protein_100g:
            item.protein_100g = Decimal(str(float(item.protein_100g) * conversion_factor))
        
        if item.fat_100g:
            item.fat_100g = Decimal(str(float(item.fat_100g) * conversion_factor))
        
        if item.carb_100g:
            item.carb_100g = Decimal(str(float(item.carb_100g) * conversion_factor))
        
        return item
    
    def _round_nutrients(self, item: FoodItem) -> FoodItem:
        """Округление нутриентов."""
        # Калории - до целого
        if item.kcal_100g:
            item.kcal_100g = item.kcal_100g.quantize(
                Decimal('1'), rounding=ROUND_HALF_UP
            )
        
        # БЖУ - до 1 знака после запятой
        for attr in ['protein_100g', 'fat_100g', 'carb_100g']:
            value = getattr(item, attr)
            if value:
                setattr(item, attr, value.quantize(
                    Decimal('0.1'), rounding=ROUND_HALF_UP
                ))
        
        return item
    
    def normalize_composition(self, composition: str) -> str:
        """Нормализация состава/ингредиентов."""
        if not composition:
            return ""
        
        # Удаляем HTML теги
        composition = re.sub(r'<[^>]+>', '', composition)
        
        # Удаляем лишние пробелы и переносы строк
        composition = re.sub(r'\s+', ' ', composition.strip())
        
        # Удаляем технические фразы
        tech_phrases = [
            r'состав:?',
            r'ингредиенты:?',
            r'содержит:?',
            r'может содержать следы:?'
        ]
        
        for phrase in tech_phrases:
            composition = re.sub(phrase, '', composition, flags=re.IGNORECASE)
        
        return composition.strip()
    
    def extract_number_from_text(self, text: str) -> Optional[Decimal]:
        """Извлечение числа из текста."""
        if not text:
            return None
        
        # Удаляем единицы измерения
        clean_text = text
        for pattern in self.unit_patterns.values():
            clean_text = pattern.sub('', clean_text)
        
        # Ищем число
        match = self.number_pattern.search(clean_text)
        if match:
            number_str = match.group(1).replace(',', '.')
            try:
                return Decimal(number_str)
            except:
                return None
        
        return None
    
    def calculate_price_per_100g(self, price: Decimal, weight_g: Decimal) -> Decimal:
        """Расчет цены за 100г."""
        if not weight_g or weight_g <= 0:
            return price
        
        price_per_100g = price * 100 / weight_g
        return price_per_100g.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    
    def normalize_tags(self, tags: list) -> list:
        """Нормализация тегов."""
        if not tags:
            return []
        
        normalized_tags = []
        for tag in tags:
            if isinstance(tag, str):
                # Приводим к нижнему регистру и очищаем
                clean_tag = re.sub(r'[^\w\s\-]', '', tag.lower().strip())
                
                # Специальные случаи нормализации
                if clean_tag in ['острое!', 'острое', 'острый', 'острая']:
                    clean_tag = 'острое'
                elif clean_tag in ['вегетарианский', 'вегетарианская', 'веган']:
                    clean_tag = 'вегетарианское'
                elif clean_tag in ['пп', 'диетический', 'диетическая']:
                    clean_tag = 'диетическое'
                
                if clean_tag and len(clean_tag) > 1:
                    normalized_tags.append(clean_tag)
        
        return list(set(normalized_tags))  # Удаляем дубликаты

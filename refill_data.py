#!/usr/bin/env python3
"""
🔧 СКРИПТ ДОБОРА НЕДОСТАЮЩИХ ДАННЫХ
Анализирует CSV файл, находит товары с неполными данными и добирает их.
"""

import asyncio
import csv
import json
import pandas as pd
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin

try:
    from selectolax.parser import HTMLParser
except ImportError:
    HTMLParser = None

import httpx


class DataRefiller:
    """Класс для добора недостающих данных."""
    
    def __init__(self):
        self.BASE_URL = "https://vkusvill.ru"
        
    async def analyze_and_refill(self, csv_file: str):
        """Анализирует CSV и добирает недостающие данные."""
        print(f"🔍 Анализируем файл: {csv_file}")
        
        # Читаем CSV
        df = pd.read_csv(csv_file)
        print(f"📊 Всего товаров в файле: {len(df)}")
        
        # Анализируем качество данных
        incomplete_products = []
        
        for index, row in df.iterrows():
            issues = []
            
            # Проверяем БЖУ
            bju_fields = ['kcal_100g', 'protein_100g', 'fat_100g', 'carb_100g']
            filled_bju = sum(1 for field in bju_fields if pd.notna(row[field]) and str(row[field]).strip())
            
            if filled_bju < 4:
                issues.append(f"БЖУ:{filled_bju}/4")
            
            # Проверяем цену
            if pd.isna(row['price']) or not str(row['price']).strip():
                issues.append("НЕТ_ЦЕНЫ")
            
            # Проверяем состав
            if pd.isna(row['composition']) or not str(row['composition']).strip():
                issues.append("НЕТ_СОСТАВА")
            
            if issues:
                incomplete_products.append({
                    'index': index,
                    'id': row['id'],
                    'name': row['name'][:50],
                    'url': row['url'],
                    'issues': issues
                })
        
        print(f"❌ Товаров с проблемами: {len(incomplete_products)}")
        print(f"✅ Качественных товаров: {len(df) - len(incomplete_products)} ({(len(df) - len(incomplete_products))/len(df)*100:.1f}%)")
        
        if not incomplete_products:
            print("🎉 Все данные уже полные!")
            return
        
        # Показываем статистику проблем
        issue_stats = {}
        for product in incomplete_products:
            for issue in product['issues']:
                issue_stats[issue] = issue_stats.get(issue, 0) + 1
        
        print("\n📈 СТАТИСТИКА ПРОБЛЕМ:")
        for issue, count in issue_stats.items():
            print(f"   • {issue}: {count} товаров")
        
        # Добираем данные
        print(f"\n🔧 Начинаем добор данных для {len(incomplete_products)} товаров...")
        
        async with httpx.AsyncClient(timeout=30) as client:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            }
            client.headers.update(headers)
            
            # Обрабатываем товары батчами
            batch_size = 5
            updated_count = 0
            
            for i in range(0, len(incomplete_products), batch_size):
                batch = incomplete_products[i:i + batch_size]
                print(f"🔧 Добираем {i+1}-{min(i+batch_size, len(incomplete_products))}/{len(incomplete_products)}")
                
                for product in batch:
                    try:
                        # Пытаемся заново извлечь данные
                        updated_data = await self._refill_product_data(client, product['url'], product['issues'])
                        
                        if updated_data:
                            # Обновляем DataFrame
                            for field, value in updated_data.items():
                                if value:  # Только если получили данные
                                    df.at[product['index'], field] = value
                                    updated_count += 1
                            
                            print(f"      ✅ {product['name']}... обновлено полей: {len([v for v in updated_data.values() if v])}")
                        else:
                            print(f"      ❌ {product['name']}... не удалось обновить")
                            
                    except Exception as e:
                        print(f"      ❌ {product['name']}... ошибка: {e}")
                        continue
                
                await asyncio.sleep(1)
        
        # Сохраняем обновленный файл
        output_file = csv_file.replace('.csv', '_refilled.csv')
        df.to_csv(output_file, index=False)
        
        # Финальная статистика
        print(f"\n🏁 ДОБОР ЗАВЕРШЕН")
        print(f"📊 Обновлено полей: {updated_count}")
        print(f"💾 Сохранено в: {output_file}")
        
        # Проверяем итоговое качество
        final_quality = self._calculate_quality(df)
        print(f"⭐ Итоговое качество: {final_quality:.1f}%")
        
    async def _refill_product_data(self, client, url: str, issues: List[str]) -> Dict[str, str]:
        """Добирает недостающие данные для товара."""
        try:
            response = await client.get(url)
            if response.status_code != 200:
                return {}
                
            parser = HTMLParser(response.text)
            page_text = response.text
            
            updated_data = {}
            
            # Добираем цену если нужно
            if "НЕТ_ЦЕНЫ" in issues:
                price = self._extract_price_enhanced(parser, page_text)
                if price:
                    updated_data['price'] = price
            
            # Добираем состав если нужно
            if "НЕТ_СОСТАВА" in issues:
                composition = self._extract_composition_enhanced(parser, page_text)
                if composition:
                    updated_data['composition'] = composition
            
            # Добираем БЖУ если нужно
            if any("БЖУ:" in issue for issue in issues):
                nutrition = self._extract_bju_enhanced(parser, page_text)
                for field, value in nutrition.items():
                    if value:
                        updated_data[field] = value
            
            return updated_data
            
        except Exception:
            return {}
    
    def _extract_price_enhanced(self, parser, page_text: str) -> str:
        """Расширенное извлечение цены."""
        # Метод 1: Все возможные селекторы цены
        price_selectors = [
            '.price', '.product-price', '.cost', '.goods-price', 
            '[class*="price"]', '[data-price]', '.current-price',
            '.js-product-price', '[class*="cost"]'
        ]
        
        for selector in price_selectors:
            elements = parser.css(selector)
            for element in elements:
                price_text = element.text(strip=True)
                numbers = re.findall(r'(\d+(?:[.,]\d+)?)', price_text)
                for num in numbers:
                    try:
                        price_val = float(num.replace(',', '.'))
                        if 10 <= price_val <= 10000:
                            return num.replace(',', '.')
                    except ValueError:
                        continue
        
        # Метод 2: Поиск в атрибутах элементов
        price_attrs = parser.css('[data-price], [value*="price"], [content*="price"]')
        for element in price_attrs:
            for attr in ['data-price', 'value', 'content']:
                attr_value = element.attributes.get(attr, '')
                if attr_value:
                    numbers = re.findall(r'(\d+(?:[.,]\d+)?)', attr_value)
                    for num in numbers:
                        try:
                            price_val = float(num.replace(',', '.'))
                            if 10 <= price_val <= 10000:
                                return num.replace(',', '.')
                        except ValueError:
                            continue
        
        # Метод 3: Поиск в тексте страницы
        text_patterns = [
            r'(\d+(?:[.,]\d+)?)\s*руб',
            r'(\d+(?:[.,]\d+)?)\s*₽',
            r'цена[:\s]*(\d+(?:[.,]\d+)?)',
            r'стоимость[:\s]*(\d+(?:[.,]\d+)?)',
            r'price[:\s]*(\d+(?:[.,]\d+)?)'
        ]
        for pattern in text_patterns:
            matches = re.finditer(pattern, page_text, re.I)
            for match in matches:
                try:
                    price_val = float(match.group(1).replace(',', '.'))
                    if 10 <= price_val <= 10000:
                        return match.group(1).replace(',', '.')
                except ValueError:
                    continue
        
        return ""
    
    def _extract_composition_enhanced(self, parser, page_text: str) -> str:
        """Расширенное извлечение состава."""
        # Метод 1: Поиск по ключевому слову "Состав" с расширенными элементами
        elements = parser.css('div, p, span, td, li, section, article, main')
        for element in elements:
            text = element.text().strip()
            text_lower = text.lower()
            
            if 'состав' in text_lower and len(text) > 15:
                # Исключаем навигационные элементы
                if not any(word in text_lower for word in ['меню', 'каталог', 'корзина', 'навигация', 'вкусвилл', 'доставка']):
                    if text_lower.startswith('состав'):
                        return text[:800]
                    elif len(text) < 800 and 'состав' in text_lower:
                        return text[:500]
        
        # Метод 2: Поиск в мета-тегах
        meta_tags = parser.css('meta[name*="description"], meta[property*="description"]')
        for meta in meta_tags:
            content = meta.attributes.get('content', '')
            if 'состав' in content.lower() and len(content) > 20:
                return content[:500]
        
        # Метод 3: Поиск в JSON-LD структурированных данных
        try:
            scripts = parser.css('script[type="application/ld+json"]')
            for script in scripts:
                try:
                    data = json.loads(script.text())
                    
                    def extract_from_json(obj):
                        if isinstance(obj, dict):
                            for key, value in obj.items():
                                if isinstance(value, str) and 'состав' in value.lower() and len(value) > 20:
                                    return value[:500]
                                elif isinstance(value, (dict, list)):
                                    result = extract_from_json(value)
                                    if result:
                                        return result
                        elif isinstance(obj, list):
                            for item in obj:
                                result = extract_from_json(item)
                                if result:
                                    return result
                        return None
                    
                    composition = extract_from_json(data)
                    if composition:
                        return composition
                        
                except json.JSONDecodeError:
                    continue
        except:
            pass
        
        return ""
    
    def _extract_bju_enhanced(self, parser, page_text: str) -> Dict[str, str]:
        """Расширенное извлечение БЖУ."""
        nutrition = {'kcal_100g': '', 'protein_100g': '', 'fat_100g': '', 'carb_100g': ''}
        
        # Метод 1: Поиск в элементах с расширенными паттернами
        elements = parser.css('div, span, p, td, th, li, section')
        for element in elements:
            text = element.text().lower()
            
            if any(word in text for word in ['ккал', 'белки', 'жиры', 'углеводы', 'калорийность', 'энергетическая']):
                # Калории - множественные паттерны
                if ('ккал' in text or 'калорийность' in text or 'энергетическая' in text) and not nutrition['kcal_100g']:
                    kcal_patterns = [
                        r'(\d+(?:[.,]\d+)?)\s*ккал',
                        r'ккал[:\s]*(\d+(?:[.,]\d+)?)',
                        r'калорийность[:\s]*(\d+(?:[.,]\d+)?)',
                        r'энергетическая\s+ценность[:\s]*(\d+(?:[.,]\d+)?)',
                        r'(\d+(?:[.,]\d+)?)\s+ккал',
                        r'энергия[:\s]*(\d+(?:[.,]\d+)?)'
                    ]\n                    for pattern in kcal_patterns:
                        match = re.search(pattern, text)
                        if match:
                            try:
                                val = float(match.group(1).replace(',', '.'))
                                if 10 <= val <= 900:
                                    nutrition['kcal_100g'] = match.group(1).replace(',', '.')
                                    break
                            except ValueError:
                                continue
                
                # Белки - множественные паттерны
                if 'белк' in text and not nutrition['protein_100g']:
                    protein_patterns = [
                        r'(\d+(?:[.,]\d+)?)\s+белки,\s*г',
                        r'белк[иа][:\s]*(\d+(?:[.,]\d+)?)',
                        r'белок[:\s]*(\d+(?:[.,]\d+)?)',
                        r'протеин[:\s]*(\d+(?:[.,]\d+)?)',
                        r'(\d+(?:[.,]\d+)?)\s*г\s*белк'
                    ]
                    for pattern in protein_patterns:
                        match = re.search(pattern, text)
                        if match:
                            try:
                                val = float(match.group(1).replace(',', '.'))
                                if 0 <= val <= 100:
                                    nutrition['protein_100g'] = match.group(1).replace(',', '.')
                                    break
                            except ValueError:
                                continue
                
                # Жиры - множественные паттерны
                if 'жир' in text and not nutrition['fat_100g']:
                    fat_patterns = [
                        r'(\d+(?:[.,]\d+)?)\s+жиры,\s*г',
                        r'жир[ыа][:\s]*(\d+(?:[.,]\d+)?)',
                        r'жир[:\s]*(\d+(?:[.,]\d+)?)',
                        r'(\d+(?:[.,]\d+)?)\s*г\s*жир'
                    ]
                    for pattern in fat_patterns:
                        match = re.search(pattern, text)
                        if match:
                            try:
                                val = float(match.group(1).replace(',', '.'))
                                if 0 <= val <= 100:
                                    nutrition['fat_100g'] = match.group(1).replace(',', '.')
                                    break
                            except ValueError:
                                continue
                
                # Углеводы - множественные паттерны
                if 'углевод' in text and not nutrition['carb_100g']:
                    carb_patterns = [
                        r'(\d+(?:[.,]\d+)?)\s+углеводы,\s*г',
                        r'углевод[ыа][:\s]*(\d+(?:[.,]\d+)?)',
                        r'углевод[:\s]*(\d+(?:[.,]\d+)?)',
                        r'(\d+(?:[.,]\d+)?)\s*г\s*углевод'
                    ]
                    for pattern in carb_patterns:
                        match = re.search(pattern, text)
                        if match:
                            try:
                                val = float(match.group(1).replace(',', '.'))
                                if 0 <= val <= 100:
                                    nutrition['carb_100g'] = match.group(1).replace(',', '.')
                                    break
                            except ValueError:
                                continue
        
        # Метод 2: Поиск в таблицах
        try:
            tables = parser.css('table')
            for table in tables:
                rows = table.css('tr')
                for row in rows:
                    cells = row.css('td, th')
                    if len(cells) >= 2:
                        header = cells[0].text().lower()
                        value_text = cells[1].text()
                        
                        num_match = re.search(r'(\d+(?:[.,]\d+)?)', value_text)
                        if num_match:
                            value = num_match.group(1).replace(',', '.')
                            
                            if ('ккал' in header or 'калорийность' in header) and not nutrition['kcal_100g']:
                                nutrition['kcal_100g'] = value
                            elif 'белк' in header and not nutrition['protein_100g']:
                                nutrition['protein_100g'] = value
                            elif 'жир' in header and not nutrition['fat_100g']:
                                nutrition['fat_100g'] = value
                            elif 'углевод' in header and not nutrition['carb_100g']:
                                nutrition['carb_100g'] = value
        except:
            pass
        
        return nutrition
    
    def _calculate_quality(self, df) -> float:
        """Вычисляет общее качество данных."""
        total_score = 0
        max_score = 0
        
        for _, row in df.iterrows():
            # БЖУ (4 балла максимум)
            bju_fields = ['kcal_100g', 'protein_100g', 'fat_100g', 'carb_100g']
            bju_score = sum(1 for field in bju_fields if pd.notna(row[field]) and str(row[field]).strip())
            
            # Состав (1 балл)
            composition_score = 1 if pd.notna(row['composition']) and str(row['composition']).strip() else 0
            
            # Цена (1 балл)
            price_score = 1 if pd.notna(row['price']) and str(row['price']).strip() else 0
            
            total_score += bju_score + composition_score + price_score
            max_score += 6  # 4 + 1 + 1
        
        return (total_score / max_score) * 100 if max_score > 0 else 0


async def main():
    """Главная функция добора данных."""
    if len(sys.argv) < 2:
        print("❌ Использование: python refill_data.py <путь_к_csv_файлу>")
        return
    
    csv_file = sys.argv[1]
    
    if not Path(csv_file).exists():
        print(f"❌ Файл не найден: {csv_file}")
        return
    
    print("🔧 СКРИПТ ДОБОРА НЕДОСТАЮЩИХ ДАННЫХ")
    print("=" * 50)
    
    refiller = DataRefiller()
    await refiller.analyze_and_refill(csv_file)


if __name__ == "__main__":
    asyncio.run(main())

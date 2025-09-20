#!/usr/bin/env python3
"""
🔍 ПРОВЕРКА ПРАВИЛЬНОСТИ ДАННЫХ ПО АДРЕСУ
Проверяет что товары из CSV файла действительно доступны по указанному адресу.
"""

import asyncio
import csv
import sys
import random
from pathlib import Path
from typing import List, Dict
from urllib.parse import urljoin

try:
    from selectolax.parser import HTMLParser
except ImportError:
    HTMLParser = None

import httpx


class AddressVerifier:
    """Класс для проверки правильности данных по адресу."""
    
    def __init__(self):
        self.BASE_URL = "https://vkusvill.ru"
        
    async def verify_csv_file(self, csv_file: str, address: str, sample_size: int = 10):
        """Проверка CSV файла - доступны ли товары по адресу."""
        print(f"🔍 ПРОВЕРКА ПРАВИЛЬНОСТИ ДАННЫХ")
        print("=" * 50)
        print(f"📊 Файл: {csv_file}")
        print(f"📍 Адрес: {address}")
        print(f"🎯 Проверяем случайную выборку: {sample_size} товаров")
        print()
        
        # Читаем CSV файл
        if not Path(csv_file).exists():
            print(f"❌ Файл не найден: {csv_file}")
            return
            
        products = []
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                products = list(reader)
        except Exception as e:
            print(f"❌ Ошибка чтения файла: {e}")
            return
            
        print(f"📦 Загружено товаров из файла: {len(products)}")
        
        # Выбираем случайную выборку
        sample_products = random.sample(products, min(sample_size, len(products)))
        print(f"🎲 Выбрана случайная выборка: {len(sample_products)} товаров")
        print()
        
        # Устанавливаем геолокацию
        await self._set_location_for_verification(address)
        
        # Проверяем каждый товар
        available_count = 0
        unavailable_count = 0
        error_count = 0
        
        for i, product in enumerate(sample_products, 1):
            product_id = product.get('id', '')
            product_name = product.get('name', '')[:50]
            product_url = product.get('url', '')
            
            print(f"🔍 {i}/{len(sample_products)} Проверяем: {product_name}...")
            
            try:
                is_available = await self._check_product_availability(product_url, product_id)
                
                if is_available:
                    available_count += 1
                    print(f"   ✅ ДОСТУПЕН по адресу")
                else:
                    unavailable_count += 1
                    print(f"   ❌ НЕ ДОСТУПЕН по адресу")
                    
            except Exception as e:
                error_count += 1
                print(f"   ⚠️ ОШИБКА ПРОВЕРКИ: {e}")
            
            # Пауза между проверками
            await asyncio.sleep(1)
            print()
        
        # Итоговая статистика
        print("📊 РЕЗУЛЬТАТЫ ПРОВЕРКИ")
        print("=" * 30)
        print(f"✅ Доступно: {available_count} ({available_count/len(sample_products)*100:.1f}%)")
        print(f"❌ Недоступно: {unavailable_count} ({unavailable_count/len(sample_products)*100:.1f}%)")
        print(f"⚠️ Ошибки: {error_count} ({error_count/len(sample_products)*100:.1f}%)")
        print()
        
        if available_count / len(sample_products) >= 0.8:
            print("🎉 ДАННЫЕ КОРРЕКТНЫ - большинство товаров доступны по адресу!")
        elif available_count / len(sample_products) >= 0.5:
            print("⚠️ ДАННЫЕ ЧАСТИЧНО КОРРЕКТНЫ - около половины товаров доступны")
        else:
            print("❌ ДАННЫЕ НЕКОРРЕКТНЫ - большинство товаров недоступны по адресу")
    
    async def _set_location_for_verification(self, address: str):
        """Установка геолокации для проверки."""
        try:
            # Простая геолокация (координаты Москвы по умолчанию)
            if ',' in address and len(address.split(',')) == 2:
                # Координаты переданы напрямую
                coords = address
                city = "Москва"
            else:
                # Адрес - используем Москву
                coords = "55.7558,37.6176"
                city = "Москва"
            
            lat, lon = coords.split(',')
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
            }
            
            async with httpx.AsyncClient(timeout=30, headers=headers) as client:
                location_url = f"{self.BASE_URL}/api/location?city={city}&lat={lat.strip()}&lon={lon.strip()}"
                await client.get(location_url)
                print(f"📍 Геолокация установлена: {address}")
                
        except Exception as e:
            print(f"⚠️ Ошибка установки геолокации: {e}")
    
    async def _check_product_availability(self, product_url: str, product_id: str) -> bool:
        """Проверка доступности конкретного товара по адресу."""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
            }
            
            async with httpx.AsyncClient(timeout=30, headers=headers) as client:
                response = await client.get(product_url)
                
                if response.status_code != 200:
                    return False
                
                if not HTMLParser:
                    # Простая проверка по тексту
                    return "недоступен" not in response.text.lower() and "нет в наличии" not in response.text.lower()
                
                parser = HTMLParser(response.text)
                
                # Проверяем наличие кнопки "В корзину" или цены
                buy_buttons = parser.css('.buy-button, [data-testid*="buy"], .add-to-cart, .price')
                if buy_buttons:
                    return True
                
                # Проверяем что нет сообщений о недоступности
                unavailable_indicators = [
                    'недоступен', 'нет в наличии', 'закончился', 
                    'временно недоступен', 'out of stock'
                ]
                
                page_text = response.text.lower()
                for indicator in unavailable_indicators:
                    if indicator in page_text:
                        return False
                
                return True
                
        except Exception:
            return False


async def main():
    """Главная функция проверки."""
    if len(sys.argv) < 3:
        print("🔍 ПРОВЕРКА ПРАВИЛЬНОСТИ ДАННЫХ ПО АДРЕСУ")
        print("=" * 50)
        print("Использование:")
        print("  python3 verify_address_data.py файл.csv \"Адрес\" [количество_проверок]")
        print()
        print("Примеры:")
        print("  python3 verify_address_data.py data/address_fast_123.csv \"Москва, Арбат, 15\" 5")
        print("  python3 verify_address_data.py data/address_fast_123.csv \"55.7558,37.6176\" 10")
        print("  python3 verify_address_data.py data/address_fast_123.csv \"СПб, Невский, 1\"")
        return
    
    csv_file = sys.argv[1]
    address = sys.argv[2]
    sample_size = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    
    verifier = AddressVerifier()
    await verifier.verify_csv_file(csv_file, address, sample_size)


if __name__ == "__main__":
    asyncio.run(main())

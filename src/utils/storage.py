"""
Модуль для хранения и экспорта данных
"""
import os
import json
import csv
import sqlite3
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from ..sources.base import ScrapedProduct


class DataStorage:
    """Класс для хранения и экспорта данных"""
    
    def __init__(self, db_path: str = "data/out/products.db"):
        self.db_path = db_path
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Создаем директорию для базы данных
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Инициализируем базу данных
        self._init_database()
        
    def _init_database(self):
        """Инициализация базы данных SQLite"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Создаем таблицу продуктов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    category TEXT,
                    kcal_100g REAL,
                    protein_100g REAL,
                    fat_100g REAL,
                    carb_100g REAL,
                    portion_g REAL,
                    price REAL,
                    shop TEXT NOT NULL,
                    tags TEXT,
                    composition TEXT,
                    url TEXT,
                    image_url TEXT,
                    image_path TEXT,
                    available BOOLEAN DEFAULT 1,
                    unit_price REAL,
                    brand TEXT,
                    weight_declared_g REAL,
                    energy_kj_100g REAL,
                    allergens TEXT,
                    scraped_at TEXT,
                    extra TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Создаем индексы для быстрого поиска
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_shop ON products(shop)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_category ON products(category)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_updated_at ON products(updated_at)')
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"База данных инициализирована: {self.db_path}")
            
        except Exception as e:
            self.logger.error(f"Ошибка инициализации базы данных: {e}")
            raise
            
    def save_product(self, product: ScrapedProduct) -> bool:
        """Сохранение или обновление продукта в базе данных"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Проверяем, существует ли продукт
            cursor.execute('SELECT id FROM products WHERE id = ?', (product.id,))
            exists = cursor.fetchone()
            
            if exists:
                # Обновляем существующий продукт
                cursor.execute('''
                    UPDATE products SET
                        name = ?, category = ?, kcal_100g = ?, protein_100g = ?,
                        fat_100g = ?, carb_100g = ?, portion_g = ?, price = ?,
                        shop = ?, tags = ?, composition = ?, url = ?, image_url = ?,
                        image_path = ?, available = ?, unit_price = ?, brand = ?,
                        weight_declared_g = ?, energy_kj_100g = ?, allergens = ?,
                        scraped_at = ?, extra = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (
                    product.name, product.category, product.kcal_100g,
                    product.protein_100g, product.fat_100g, product.carb_100g,
                    product.portion_g, product.price, product.shop,
                    json.dumps(product.tags), product.composition, product.url,
                    product.image_url, product.image_path, product.available,
                    product.unit_price, product.brand, product.weight_declared_g,
                    product.energy_kj_100g, json.dumps(product.allergens),
                    product.scraped_at, json.dumps(product.extra), product.id
                ))
                
                self.logger.debug(f"Продукт {product.id} обновлен")
                
            else:
                # Создаем новый продукт
                cursor.execute('''
                    INSERT INTO products (
                        id, name, category, kcal_100g, protein_100g, fat_100g,
                        carb_100g, portion_g, price, shop, tags, composition,
                        url, image_url, image_path, available, unit_price,
                        brand, weight_declared_g, energy_kj_100g, allergens,
                        scraped_at, extra
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    product.id, product.name, product.category, product.kcal_100g,
                    product.protein_100g, product.fat_100g, product.carb_100g,
                    product.portion_g, product.price, product.shop,
                    json.dumps(product.tags), product.composition, product.url,
                    product.image_url, product.image_path, product.available,
                    product.unit_price, product.brand, product.weight_declared_g,
                    product.energy_kj_100g, json.dumps(product.allergens),
                    product.scraped_at, json.dumps(product.extra)
                ))
                
                self.logger.debug(f"Продукт {product.id} создан")
                
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка сохранения продукта {product.id}: {e}")
            return False
            
    def save_products(self, products: List[ScrapedProduct]) -> int:
        """Сохранение списка продуктов"""
        saved_count = 0
        for product in products:
            if self.save_product(product):
                saved_count += 1
                
        self.logger.info(f"Сохранено продуктов: {saved_count}/{len(products)}")
        return saved_count
        
    def get_product(self, product_id: str) -> Optional[ScrapedProduct]:
        """Получение продукта по ID"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM products WHERE id = ?', (product_id,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return self._row_to_product(row)
            return None
            
        except Exception as e:
            self.logger.error(f"Ошибка получения продукта {product_id}: {e}")
            return None
            
    def get_products_by_shop(self, shop: str, limit: int = None) -> List[ScrapedProduct]:
        """Получение продуктов по магазину"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            query = 'SELECT * FROM products WHERE shop = ? ORDER BY updated_at DESC'
            params = [shop]
            
            if limit:
                query += ' LIMIT ?'
                params.append(limit)
                
            cursor.execute(query, params)
            rows = cursor.fetchall()
            conn.close()
            
            return [self._row_to_product(row) for row in rows]
            
        except Exception as e:
            self.logger.error(f"Ошибка получения продуктов магазина {shop}: {e}")
            return []
            
    def get_all_products(self, limit: int = None) -> List[ScrapedProduct]:
        """Получение всех продуктов"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            query = 'SELECT * FROM products ORDER BY updated_at DESC'
            params = []
            
            if limit:
                query += ' LIMIT ?'
                params.append(limit)
                
            cursor.execute(query, params)
            rows = cursor.fetchall()
            conn.close()
            
            return [self._row_to_product(row) for row in rows]
            
        except Exception as e:
            self.logger.error(f"Ошибка получения всех продуктов: {e}")
            return []
            
    def _row_to_product(self, row: tuple) -> ScrapedProduct:
        """Преобразование строки БД в объект продукта"""
        return ScrapedProduct(
            id=row[0],
            name=row[1],
            category=row[2],
            kcal_100g=row[3],
            protein_100g=row[4],
            fat_100g=row[5],
            carb_100g=row[6],
            portion_g=row[7],
            price=row[8],
            shop=row[9],
            tags=json.loads(row[10]) if row[10] else [],
            composition=row[11] or "",
            url=row[12] or "",
            image_url=row[13] or "",
            image_path=row[14],
            available=bool(row[15]),
            unit_price=row[16],
            brand=row[17],
            weight_declared_g=row[18],
            energy_kj_100g=row[19],
            allergens=json.loads(row[20]) if row[20] else [],
            scraped_at=row[21] or "",
            extra=json.loads(row[22]) if row[22] else {}
        )
        
    def export_to_csv(self, file_path: str, products: List[ScrapedProduct] = None) -> bool:
        """Экспорт в CSV файл"""
        try:
            if products is None:
                products = self.get_all_products()
                
            if not products:
                self.logger.warning("Нет продуктов для экспорта")
                return False
                
            # Создаем директорию для файла
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Подготавливаем данные для CSV
            csv_data = []
            for product in products:
                csv_data.append({
                    'id': product.id,
                    'name': product.name,
                    'category': product.category,
                    'kcal_100g': product.kcal_100g,
                    'protein_100g': product.protein_100g,
                    'fat_100g': product.fat_100g,
                    'carb_100g': product.carb_100g,
                    'portion_g': product.portion_g,
                    'price': product.price,
                    'shop': product.shop,
                    'tags': '; '.join(product.tags),
                    'composition': product.composition,
                    'url': product.url,
                    'image_url': product.image_url,
                    'image_path': product.image_path,
                    'available': product.available,
                    'unit_price': product.unit_price,
                    'brand': product.brand,
                    'weight_declared_g': product.weight_declared_g,
                    'energy_kj_100g': product.energy_kj_100g,
                    'allergens': '; '.join(product.allergens),
                    'scraped_at': product.scraped_at,
                    'extra': json.dumps(product.extra, ensure_ascii=False)
                })
                
            # Записываем в CSV
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = csv_data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(csv_data)
                
            self.logger.info(f"Данные экспортированы в CSV: {file_path} ({len(products)} записей)")
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка экспорта в CSV: {e}")
            return False
            
    def export_to_jsonl(self, file_path: str, products: List[ScrapedProduct] = None) -> bool:
        """Экспорт в JSONL файл"""
        try:
            if products is None:
                products = self.get_all_products()
                
            if not products:
                self.logger.warning("Нет продуктов для экспорта")
                return False
                
            # Создаем директорию для файла
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Записываем в JSONL
            with open(file_path, 'w', encoding='utf-8') as jsonlfile:
                for product in products:
                    # Преобразуем в словарь
                    product_dict = {
                        'id': product.id,
                        'name': product.name,
                        'category': product.category,
                        'kcal_100g': product.kcal_100g,
                        'protein_100g': product.protein_100g,
                        'fat_100g': product.fat_100g,
                        'carb_100g': product.carb_100g,
                        'portion_g': product.portion_g,
                        'price': product.price,
                        'shop': product.shop,
                        'tags': product.tags,
                        'composition': product.composition,
                        'url': product.url,
                        'image_url': product.image_url,
                        'image_path': product.image_path,
                        'available': product.available,
                        'unit_price': product.unit_price,
                        'brand': product.brand,
                        'weight_declared_g': product.weight_declared_g,
                        'energy_kj_100g': product.energy_kj_100g,
                        'allergens': product.allergens,
                        'scraped_at': product.scraped_at,
                        'extra': product.extra
                    }
                    
                    jsonlfile.write(json.dumps(product_dict, ensure_ascii=False) + '\n')
                    
            self.logger.info(f"Данные экспортированы в JSONL: {file_path} ({len(products)} записей)")
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка экспорта в JSONL: {e}")
            return False
            
    def export_to_parquet(self, file_path: str, products: List[ScrapedProduct] = None) -> bool:
        """Экспорт в Parquet файл"""
        try:
            if products is None:
                products = self.get_all_products()
                
            if not products:
                self.logger.warning("Нет продуктов для экспорта")
                return False
                
            # Создаем директорию для файла
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Подготавливаем данные для DataFrame
            data = []
            for product in products:
                data.append({
                    'id': product.id,
                    'name': product.name,
                    'category': product.category,
                    'kcal_100g': product.kcal_100g,
                    'protein_100g': product.protein_100g,
                    'fat_100g': product.fat_100g,
                    'carb_100g': product.carb_100g,
                    'portion_g': product.portion_g,
                    'price': product.price,
                    'shop': product.shop,
                    'tags': '; '.join(product.tags),
                    'composition': product.composition,
                    'url': product.url,
                    'image_url': product.image_url,
                    'image_path': product.image_path,
                    'available': product.available,
                    'unit_price': product.unit_price,
                    'brand': product.brand,
                    'weight_declared_g': product.weight_declared_g,
                    'energy_kj_100g': product.energy_kj_100g,
                    'allergens': '; '.join(product.allergens),
                    'scraped_at': product.scraped_at,
                    'extra': json.dumps(product.extra, ensure_ascii=False)
                })
                
            # Создаем DataFrame и сохраняем
            df = pd.DataFrame(data)
            df.to_parquet(file_path, index=False)
            
            self.logger.info(f"Данные экспортированы в Parquet: {file_path} ({len(products)} записей)")
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка экспорта в Parquet: {e}")
            return False
            
    def export_to_sqlite(self, file_path: str, products: List[ScrapedProduct] = None) -> bool:
        """Экспорт в SQLite файл"""
        try:
            if products is None:
                products = self.get_all_products()
                
            if not products:
                self.logger.warning("Нет продуктов для экспорта")
                return False
                
            # Создаем директорию для файла
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Создаем новое соединение с файлом экспорта
            conn = sqlite3.connect(file_path)
            cursor = conn.cursor()
            
            # Создаем таблицу
            cursor.execute('''
                CREATE TABLE products (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    category TEXT,
                    kcal_100g REAL,
                    protein_100g REAL,
                    fat_100g REAL,
                    carb_100g REAL,
                    portion_g REAL,
                    price REAL,
                    shop TEXT NOT NULL,
                    tags TEXT,
                    composition TEXT,
                    url TEXT,
                    image_url TEXT,
                    image_path TEXT,
                    available BOOLEAN DEFAULT 1,
                    unit_price REAL,
                    brand TEXT,
                    weight_declared_g REAL,
                    energy_kj_100g REAL,
                    allergens TEXT,
                    scraped_at TEXT,
                    extra TEXT
                )
            ''')
            
            # Вставляем данные
            for product in products:
                cursor.execute('''
                    INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    product.id, product.name, product.category, product.kcal_100g,
                    product.protein_100g, product.fat_100g, product.carb_100g,
                    product.portion_g, product.price, product.shop,
                    json.dumps(product.tags), product.composition, product.url,
                    product.image_url, product.image_path, product.available,
                    product.unit_price, product.brand, product.weight_declared_g,
                    product.energy_kj_100g, json.dumps(product.allergens),
                    product.scraped_at, json.dumps(product.extra)
                ))
                
            conn.commit()
            conn.close()
            
            self.logger.info(f"Данные экспортированы в SQLite: {file_path} ({len(products)} записей)")
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка экспорта в SQLite: {e}")
            return False
            
    def get_statistics(self) -> Dict[str, Any]:
        """Получение статистики по данным"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Общее количество продуктов
            cursor.execute('SELECT COUNT(*) FROM products')
            total_products = cursor.fetchone()[0]
            
            # Количество по магазинам
            cursor.execute('SELECT shop, COUNT(*) FROM products GROUP BY shop')
            shop_counts = dict(cursor.fetchall())
            
            # Количество по категориям
            cursor.execute('SELECT category, COUNT(*) FROM products GROUP BY category')
            category_counts = dict(cursor.fetchall())
            
            # Последнее обновление
            cursor.execute('SELECT MAX(updated_at) FROM products')
            last_updated = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                'total_products': total_products,
                'shop_counts': shop_counts,
                'category_counts': category_counts,
                'last_updated': last_updated
            }
            
        except Exception as e:
            self.logger.error(f"Ошибка получения статистики: {e}")
            return {}

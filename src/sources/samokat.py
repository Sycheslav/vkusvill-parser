"""
Скрейпер для сервиса Самокат (eda.samokat.ru)
"""
import re
import logging
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin, urlparse
import asyncio

# Используем абсолютные импорты
try:
    from src.sources.base import BaseScraper, ScrapedProduct
except ImportError:
    try:
        from sources.base import BaseScraper, ScrapedProduct
    except ImportError:
        # Для тестирования
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from sources.base import BaseScraper, ScrapedProduct


class SamokatScraper(BaseScraper):
    """Скрейпер для Самоката"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = "https://eda.samokat.ru"
        self.city = config.get('city', 'Москва')
        self.coords = config.get('coords')
        self.session_id = None
        
    async def setup_location(self):
        """Настройка локации для получения доступной витрины"""
        try:
            await self.page.goto(self.base_url)
            await self.random_delay(2, 4)
            
            # Если есть координаты, используем их
            if self.coords:
                lat, lon = map(float, self.coords.split(','))
                await self.page.evaluate(f"""
                    navigator.geolocation.getCurrentPosition = function(success) {{
                        success({{
                            coords: {{
                                latitude: {lat},
                                longitude: {lon}
                            }}
                        }});
                    }};
                """)
            
            # Ждем загрузки и ищем поле ввода адреса
            address_input = await self.page.wait_for_selector('input[placeholder*="адрес"], input[placeholder*="Адрес"]', timeout=10000)
            if address_input:
                await address_input.fill(self.city)
                await self.random_delay(1, 2)
                
                # Ищем и кликаем по первому предложению
                suggestion = await self.page.wait_for_selector('.address-suggestion, .suggestion-item', timeout=5000)
                if suggestion:
                    await suggestion.click()
                    await self.random_delay(2, 4)
                    
            # Ждем загрузки витрины
            await self.page.wait_for_selector('.catalog, .products-grid, [data-testid="catalog"]', timeout=15000)
            self.logger.info(f"Локация настроена: {self.city}")
            
        except Exception as e:
            self.logger.error(f"Ошибка настройки локации: {e}")
            raise
            
    async def get_categories(self) -> List[str]:
        """Получить список доступных категорий готовой еды"""
        try:
            await self.setup_location()
            
            # Ищем категории готовой еды
            category_selectors = [
                'a[href*="готов"], a[href*="кулинар"], a[href*="салат"], a[href*="суп"]',
                '.category-item', '.menu-category', '[data-category]'
            ]
            
            categories = []
            for selector in category_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    for elem in elements:
                        text = await elem.text_content()
                        href = await elem.get_attribute('href')
                        if text and any(keyword in text.lower() for keyword in ['готов', 'кулинар', 'салат', 'суп', 'блюд']):
                            categories.append({
                                'name': text.strip(),
                                'url': urljoin(self.base_url, href) if href else None
                            })
                except Exception as e:
                    self.logger.debug(f"Ошибка при поиске категорий с селектором {selector}: {e}")
                    continue
                    
            # Если категории не найдены, используем стандартные
            if not categories:
                categories = [
                    {'name': 'Готовая еда', 'url': None},
                    {'name': 'Кулинария', 'url': None},
                    {'name': 'Салаты', 'url': None},
                    {'name': 'Супы', 'url': None},
                    {'name': 'Горячие блюда', 'url': None}
                ]
                
            self.logger.info(f"Найдено категорий: {len(categories)}")
            return [cat['name'] for cat in categories]
            
        except Exception as e:
            self.logger.error(f"Ошибка получения категорий: {e}")
            return ['Готовая еда', 'Кулинария', 'Салаты', 'Супы', 'Горячие блюда']
            
    async def scrape_category(self, category: str, limit: int = None) -> List[ScrapedProduct]:
        """Скрапить продукты из указанной категории"""
        try:
            await self.setup_location()
            
            # Ищем товары на странице
            product_selectors = [
                '.product-card', '.item-card', '.catalog-item', '[data-product]',
                '.product', '.item', '.card'
            ]
            
            products = []
            for selector in product_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    if elements:
                        self.logger.info(f"Найдено товаров с селектором {selector}: {len(elements)}")
                        break
                except:
                    continue
            else:
                self.logger.warning("Товары не найдены")
                return []
                
            # Ограничиваем количество товаров
            if limit:
                elements = elements[:limit]
                
            # Скрапим каждый товар
            for i, element in enumerate(elements):
                try:
                    if i % 10 == 0:
                        self.logger.info(f"Обработано товаров: {i}/{len(elements)}")
                        
                    product = await self._extract_product_from_card(element, category)
                    if product:
                        products.append(product)
                        
                    await self.random_delay(0.5, 1.5)
                    
                except Exception as e:
                    self.logger.error(f"Ошибка при обработке товара {i}: {e}")
                    continue
                    
            self.logger.info(f"Успешно обработано товаров: {len(products)}")
            return products
            
        except Exception as e:
            self.logger.error(f"Ошибка скрапинга категории {category}: {e}")
            return []
            
    async def _extract_product_from_card(self, element, category: str) -> Optional[ScrapedProduct]:
        """Извлечь данные продукта из карточки товара"""
        try:
            # Основная информация
            name_elem = await element.query_selector('.product-name, .item-name, .title, h3, h4')
            name = await name_elem.text_content() if name_elem else "Неизвестный товар"
            name = name.strip() if name else "Неизвестный товар"
            
            # Цена
            price_elem = await element.query_selector('.price, .cost, .item-price, [data-price]')
            price_text = await price_elem.text_content() if price_elem else "0"
            price = self._extract_price(price_text)
            
            # URL товара
            link_elem = await element.query_selector('a[href]')
            url = await link_elem.get_attribute('href') if link_elem else ""
            if url and not url.startswith('http'):
                url = urljoin(self.base_url, url)
                
            # Изображение
            img_elem = await element.query_selector('img[src], img[data-src]')
            image_url = await img_elem.get_attribute('src') or await img_elem.get_attribute('data-src') if img_elem else ""
            if image_url and not image_url.startswith('http'):
                image_url = urljoin(self.base_url, image_url)
                
            # ID товара (из URL или data-атрибута)
            product_id = self._extract_product_id(url, element)
            
            # Создаем базовый продукт
            product = ScrapedProduct(
                id=product_id,
                name=name,
                category=category,
                price=price,
                url=url,
                image_url=image_url,
                shop="samokat",
                available=True
            )
            
            # Если есть ссылка на детальную страницу, получаем дополнительную информацию
            if url:
                try:
                    detailed_product = await self.scrape_product_page(url)
                    if detailed_product:
                        # Обновляем базовую информацию детальной
                        product.kcal_100g = detailed_product.kcal_100g
                        product.protein_100g = detailed_product.protein_100g
                        product.fat_100g = detailed_product.fat_100g
                        product.carb_100g = detailed_product.carb_100g
                        product.portion_g = detailed_product.portion_g
                        product.composition = detailed_product.composition
                        product.tags = detailed_product.tags
                        product.brand = detailed_product.brand
                        product.allergens = detailed_product.allergens
                        product.extra = detailed_product.extra
                except Exception as e:
                    self.logger.debug(f"Не удалось получить детальную информацию для {url}: {e}")
                    
            return product
            
        except Exception as e:
            self.logger.error(f"Ошибка извлечения продукта: {e}")
            return None
            
    async def scrape_product_page(self, url: str) -> Optional[ScrapedProduct]:
        """Скрапить детальную страницу продукта"""
        try:
            await self.page.goto(url)
            await self.random_delay(2, 4)
            
            # Ждем загрузки страницы
            await self.page.wait_for_selector('.product-info, .item-details, .product-details', timeout=10000)
            
            # Извлекаем детальную информацию
            product_data = {}
            
            # Калории и БЖУ
            nutrition_selectors = [
                '.nutrition-info', '.calories', '.nutrition', '[data-nutrition]',
                '.kcal', '.protein', '.fat', '.carbohydrates'
            ]
            
            for selector in nutrition_selectors:
                try:
                    elem = await self.page.query_selector(selector)
                    if elem:
                        text = await elem.text_content()
                        if text:
                            # Ищем калории
                            kcal_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:ккал|кал|kcal)', text, re.I)
                            if kcal_match:
                                product_data['kcal_100g'] = float(kcal_match.group(1).replace(',', '.'))
                                
                            # Ищем белки
                            protein_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:г|g)\s*(?:бел|протеин|protein)', text, re.I)
                            if protein_match:
                                product_data['protein_100g'] = float(protein_match.group(1).replace(',', '.'))
                                
                            # Ищем жиры
                            fat_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:г|g)\s*(?:жир|fat)', text, re.I)
                            if fat_match:
                                product_data['fat_100g'] = float(fat_match.group(1).replace(',', '.'))
                                
                            # Ищем углеводы
                            carb_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:г|g)\s*(?:углев|carb)', text, re.I)
                            if carb_match:
                                product_data['carb_100g'] = float(carb_match.group(1).replace(',', '.'))
                except:
                    continue
                    
            # Состав
            composition_selectors = [
                '.composition', '.ingredients', '.ingredient-list', '[data-composition]',
                '.product-composition', '.item-composition'
            ]
            
            for selector in composition_selectors:
                try:
                    elem = await self.page.query_selector(selector)
                    if elem:
                        composition = await elem.text_content()
                        if composition:
                            product_data['composition'] = composition.strip()
                            break
                except:
                    continue
                    
            # Масса порции
            weight_selectors = [
                '.weight', '.portion', '.size', '[data-weight]', '.product-weight',
                '.item-weight', '.weight-info'
            ]
            
            for selector in weight_selectors:
                try:
                    elem = await self.page.query_selector(selector)
                    if elem:
                        weight_text = await elem.text_content()
                        if weight_text:
                            weight_match = re.search(r'(\d+(?:[.,]\d+)?)\s*(?:г|g|кг|kg)', weight_text, re.I)
                            if weight_match:
                                weight = float(weight_match.group(1).replace(',', '.'))
                                if 'кг' in weight_text.lower() or 'kg' in weight_text.lower():
                                    weight *= 1000
                                product_data['portion_g'] = weight
                                break
                except:
                    continue
                    
            # Бренд
            brand_selectors = [
                '.brand', '.manufacturer', '.producer', '[data-brand]',
                '.product-brand', '.item-brand'
            ]
            
            for selector in brand_selectors:
                try:
                    elem = await self.page.query_selector(selector)
                    if elem:
                        brand = await elem.text_content()
                        if brand:
                            product_data['brand'] = brand.strip()
                            break
                except:
                    continue
                    
            # Теги
            tags = []
            tag_selectors = [
                '.tags .tag', '.badges .badge', '.labels .label',
                '[data-tag]', '.product-tags .tag'
            ]
            
            for selector in tag_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    for elem in elements:
                        tag_text = await elem.text_content()
                        if tag_text:
                            tags.append(tag_text.strip())
                except:
                    continue
                    
            product_data['tags'] = tags
            
            # Создаем продукт с детальной информацией
            detailed_product = ScrapedProduct(
                id="",  # Будет заполнено позже
                name="",  # Будет заполнено позже
                category="",  # Будет заполнено позже
                **product_data
            )
            
            return detailed_product
            
        except Exception as e:
            self.logger.error(f"Ошибка скрапинга детальной страницы {url}: {e}")
            return None
            
    def _extract_price(self, price_text: str) -> Optional[float]:
        """Извлечь цену из текста"""
        if not price_text:
            return None
            
        # Убираем лишние символы и извлекаем число
        price_match = re.search(r'(\d+(?:[.,]\d+)?)', price_text.replace(' ', ''))
        if price_match:
            return float(price_match.group(1).replace(',', '.'))
        return None
        
    def _extract_product_id(self, url: str, element) -> str:
        """Извлечь ID продукта"""
        try:
            # Пробуем извлечь из URL
            if url:
                url_parts = urlparse(url).path.split('/')
                for part in url_parts:
                    if part and part.isdigit():
                        return f"samokat:{part}"
                        
            # Пробуем извлечь из data-атрибутов
            data_id = asyncio.run(element.get_attribute('data-id'))
            if data_id:
                return f"samokat:{data_id}"
                
            # Пробуем извлечь из href
            href = asyncio.run(element.get_attribute('href'))
            if href:
                href_parts = href.split('/')
                for part in href_parts:
                    if part and part.isdigit():
                        return f"samokat:{part}"
                        
        except:
            pass
            
        # Генерируем уникальный ID
        import uuid
        return f"samokat:{str(uuid.uuid4())[:8]}"

"""Модели данных для скрейпера готовой еды."""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, validator
from decimal import Decimal


class FoodItem(BaseModel):
    """Модель элемента готовой еды."""
    
    # Обязательные поля
    id: str = Field(..., description="Стабильный ID: {shop}:{native_id}")
    name: str = Field(..., min_length=3, description="Название товара")
    category: str = Field(..., description="Категория товара")
    price: Decimal = Field(..., gt=0, description="Цена в рублях")
    shop: str = Field(..., description="Магазин: samokat|lavka|vkusvill")
    url: str = Field(..., description="Ссылка на карточку товара")
    photo_url: Optional[str] = Field(None, description="Ссылка на фото")
    
    # Нутриенты (на 100г)
    kcal_100g: Optional[Decimal] = Field(None, description="Калории на 100г")
    protein_100g: Optional[Decimal] = Field(None, description="Белки на 100г")
    fat_100g: Optional[Decimal] = Field(None, description="Жиры на 100г")
    carb_100g: Optional[Decimal] = Field(None, description="Углеводы на 100г")
    
    # Дополнительные поля
    portion_g: Optional[Decimal] = Field(None, gt=0, description="Вес порции в граммах")
    tags: List[str] = Field(default_factory=list, description="Теги товара")
    composition: Optional[str] = Field(None, description="Состав/ингредиенты")
    photo_path: Optional[str] = Field(None, description="Локальный путь к фото")
    
    # Дополнительные опциональные поля
    allergens: Optional[str] = Field(None, description="Аллергены")
    shelf_life: Optional[str] = Field(None, description="Срок годности")
    storage: Optional[str] = Field(None, description="Условия хранения")
    brand: Optional[str] = Field(None, description="Бренд/производитель")
    price_per_100g: Optional[Decimal] = Field(None, description="Цена за 100г")
    weight_g: Optional[Decimal] = Field(None, description="Вес товара")
    barcode: Optional[str] = Field(None, description="Штрихкод")
    sku: Optional[str] = Field(None, description="Артикул")
    nutri_source: Optional[str] = Field(None, description="Источник нутриентов")
    rating: Optional[Decimal] = Field(None, description="Рейтинг")
    reviews_count: Optional[int] = Field(None, description="Количество отзывов")
    availability: Optional[bool] = Field(None, description="Доступность")
    discount: Optional[Decimal] = Field(None, description="Скидка")
    old_price: Optional[Decimal] = Field(None, description="Старая цена")
    city: Optional[str] = Field(None, description="Город")
    address: Optional[str] = Field(None, description="Адрес")
    scraped_at: datetime = Field(default_factory=datetime.now, description="Время скрейпинга")
    
    @validator('shop')
    def validate_shop(cls, v):
        """Валидация названия магазина."""
        allowed_shops = ['samokat', 'lavka', 'vkusvill']
        if v not in allowed_shops:
            raise ValueError(f'Shop must be one of: {allowed_shops}')
        return v
    
    @validator('name')
    def validate_name(cls, v):
        """Валидация названия товара."""
        if v.isdigit():
            raise ValueError('Name cannot be purely numeric')
        return v.strip()
    
    def has_complete_nutrients(self) -> bool:
        """Проверка наличия всех нутриентов."""
        return all([
            self.kcal_100g is not None,
            self.protein_100g is not None,
            self.fat_100g is not None,
            self.carb_100g is not None
        ])


class ScrapingResult(BaseModel):
    """Результат скрейпинга."""
    
    shop: str
    items: List[FoodItem]
    total_found: int
    successful: int
    failed: int
    errors: List[str]
    duration_seconds: float
    scraped_at: datetime = Field(default_factory=datetime.now)


class ScrapingConfig(BaseModel):
    """Конфигурация скрейпинга."""
    
    shop: Optional[str] = None
    city: str = "Москва"
    address: Optional[str] = None
    coordinates: Optional[dict] = None
    parallel_workers: int = 3
    download_images: bool = False
    output_formats: List[str] = ["csv"]
    max_retries: int = 3
    request_delay_min: float = 1.0
    request_delay_max: float = 3.0
    headless: bool = True
    proxy_servers: List[str] = Field(default_factory=list)


class ValidationIssue(BaseModel):
    """Проблема валидации."""
    
    url: str
    issue_type: str
    description: str
    stage: str
    timestamp: datetime = Field(default_factory=datetime.now)

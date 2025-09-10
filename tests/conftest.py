"""Конфигурация тестов pytest."""

import pytest
import tempfile
import shutil
from pathlib import Path


@pytest.fixture
def temp_dir():
    """Временная директория для тестов."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
def sample_html_samokat():
    """HTML образец страницы Самокат."""
    return '''
    <html>
    <body>
        <div class="product-card">
            <h3 class="product-name">Салат Цезарь с курицей</h3>
            <div class="product-price">299 ₽</div>
            <a href="/product/caesar-salad-123">
                <img src="/images/caesar-salad.jpg" alt="Салат Цезарь">
            </a>
        </div>
        <div class="product-card">
            <h3 class="product-name">Суп Том Ям</h3>
            <div class="product-price">450 ₽</div>
            <a href="/product/tom-yam-456">
                <img src="/images/tom-yam.jpg" alt="Суп Том Ям">
            </a>
        </div>
    </body>
    </html>
    '''


@pytest.fixture
def sample_html_product_page():
    """HTML образец страницы товара."""
    return '''
    <html>
    <body>
        <h1>Салат Цезарь с курицей</h1>
        <div class="price">299 ₽</div>
        <div class="composition">
            <h3>Состав</h3>
            <p>Салат айсберг, куриная грудка, сыр пармезан, соус цезарь, гренки</p>
        </div>
        <div class="nutrition">
            <h3>Пищевая ценность на 100г</h3>
            <p>Калории: 150 ккал</p>
            <p>Белки: 12,5 г</p>
            <p>Жиры: 8,2 г</p>
            <p>Углеводы: 15,3 г</p>
        </div>
        <div class="weight">Вес: 250 г</div>
        <div class="tags">
            <span>Острое</span>
            <span>Без глютена</span>
        </div>
    </body>
    </html>
    '''


@pytest.fixture
def sample_config():
    """Пример конфигурации."""
    return {
        'browser': {
            'headless': True,
            'timeout': 30000
        },
        'scraping': {
            'parallel_workers': 2,
            'request_delay_min': 0.5,
            'request_delay_max': 1.0,
            'max_retries': 2
        },
        'defaults': {
            'city': 'Москва',
            'address': 'Тестовый адрес, 1'
        },
        'export': {
            'formats': ['csv', 'json'],
            'download_images': False
        }
    }

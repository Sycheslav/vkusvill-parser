#!/usr/bin/env python3
"""
Тестирование пагинации ВкусВилл для понимания как работает "Показать еще"
"""

import asyncio
import httpx
from selectolax.parser import HTMLParser
import json
import re


async def test_vkusvill_pagination():
    """Исследуем как работает пагинация на ВкусВилл."""
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    }
    
    async with httpx.AsyncClient(timeout=30, headers=headers) as client:
        # Загружаем главную страницу готовой еды
        url = "https://vkusvill.ru/goods/gotovaya-eda/"
        print(f"🔍 Исследуем: {url}")
        
        response = await client.get(url)
        print(f"📥 HTTP {response.status_code}, размер: {len(response.text)}")
        
        if response.status_code == 200:
            parser = HTMLParser(response.text)
            
            # Ищем все товары на первой странице
            product_links = parser.css('a[href*="/goods/"][href$=".html"]')
            print(f"🔗 Товаров на первой странице: {len(product_links)}")
            
            # Ищем кнопки "Показать еще" или похожие
            show_more_buttons = parser.css('button, a, [class*="more"], [class*="load"], [class*="show"]')
            print(f"🔘 Потенциальных кнопок загрузки: {len(show_more_buttons)}")
            
            # Анализируем кнопки
            for i, button in enumerate(show_more_buttons[:10]):  # Первые 10 для анализа
                text = button.text().strip().lower()
                classes = button.attributes.get('class', '')
                onclick = button.attributes.get('onclick', '')
                data_attrs = {k: v for k, v in button.attributes.items() if k.startswith('data-')}
                
                if any(keyword in text for keyword in ['показать', 'еще', 'больше', 'загрузить', 'more', 'load']):
                    print(f"   🎯 Кнопка {i+1}: '{text}' | class='{classes}' | onclick='{onclick}' | data={data_attrs}")
            
            # Ищем JavaScript код с пагинацией
            scripts = parser.css('script')
            print(f"📜 JavaScript блоков: {len(scripts)}")
            
            pagination_patterns = [
                r'load.*more',
                r'show.*more', 
                r'pagination',
                r'page.*next',
                r'ajax.*load',
                r'catalog.*load'
            ]
            
            for script in scripts:
                script_text = script.text()
                if script_text:
                    for pattern in pagination_patterns:
                        if re.search(pattern, script_text, re.I):
                            print(f"   📜 Найден JS с пагинацией: {pattern}")
                            # Показываем фрагмент кода
                            lines = script_text.split('\n')
                            for line_num, line in enumerate(lines):
                                if re.search(pattern, line, re.I):
                                    start = max(0, line_num - 2)
                                    end = min(len(lines), line_num + 3)
                                    print(f"      Строки {start}-{end}:")
                                    for i in range(start, end):
                                        marker = ">>> " if i == line_num else "    "
                                        print(f"      {marker}{lines[i].strip()}")
                                    break
                            break
            
            # Ищем данные о пагинации в HTML
            pagination_elements = parser.css('[class*="pag"], [id*="pag"], [data*="pag"]')
            print(f"📄 Элементов пагинации: {len(pagination_elements)}")
            
            for elem in pagination_elements[:5]:
                print(f"   📄 {elem.tag} class='{elem.attributes.get('class', '')}' id='{elem.attributes.get('id', '')}' text='{elem.text()[:50]}'")
            
            # Проверяем есть ли data-атрибуты с информацией о загрузке
            data_elements = parser.css('[data-url], [data-page], [data-offset], [data-limit], [data-category]')
            print(f"📊 Элементов с data-атрибутами: {len(data_elements)}")
            
            for elem in data_elements[:5]:
                data_attrs = {k: v for k, v in elem.attributes.items() if k.startswith('data-')}
                print(f"   📊 {elem.tag}: {data_attrs}")


if __name__ == "__main__":
    asyncio.run(test_vkusvill_pagination())

"""Утилиты для сохранения и экспорта данных."""

import csv
import json
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
from loguru import logger

from ..models import FoodItem, ScrapingResult, ValidationIssue


class DataExporter:
    """Класс для экспорта данных в различные форматы."""
    
    def __init__(self, output_dir: str = "data"):
        """Инициализация экспортера."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def export_results(
        self,
        results: List[ScrapingResult],
        filename_prefix: str = "foods",
        formats: List[str] = ["csv"],
        csv_separator: str = ","
    ) -> Dict[str, str]:
        """Экспорт результатов скрейпинга в указанные форматы."""
        
        # Объединяем все товары
        all_items = []
        for result in results:
            all_items.extend(result.items)
        
        if not all_items:
            logger.warning("No items to export")
            return {}
        
        logger.info(f"Exporting {len(all_items)} items in formats: {formats}")
        
        exported_files = {}
        
        # CSV
        if "csv" in formats:
            csv_path = self._export_csv(all_items, filename_prefix, csv_separator)
            exported_files["csv"] = str(csv_path)
        
        # JSON Lines
        if "json" in formats or "jsonl" in formats:
            json_path = self._export_jsonl(all_items, filename_prefix)
            exported_files["jsonl"] = str(json_path)
        
        # Parquet
        if "parquet" in formats:
            parquet_path = self._export_parquet(all_items, filename_prefix)
            exported_files["parquet"] = str(parquet_path)
        
        # Экспорт отчета
        report_path = self._export_report(results, filename_prefix)
        exported_files["report"] = str(report_path)
        
        # Экспорт проблем валидации
        issues_path = self._export_validation_issues(results, filename_prefix)
        if issues_path:
            exported_files["issues"] = str(issues_path)
        
        return exported_files
    
    def _export_csv(self, items: List[FoodItem], filename_prefix: str, separator: str) -> Path:
        """Экспорт в CSV формат."""
        csv_path = self.output_dir / f"{filename_prefix}.csv"
        
        if not items:
            return csv_path
        
        # Получаем все поля из первого элемента
        fieldnames = list(items[0].dict().keys())
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=separator)
            writer.writeheader()
            
            for item in items:
                row = item.dict()
                # Конвертируем списки в строки
                for key, value in row.items():
                    if isinstance(value, list):
                        row[key] = "; ".join(map(str, value))
                    elif value is None:
                        row[key] = ""
                
                writer.writerow(row)
        
        logger.info(f"Exported {len(items)} items to CSV: {csv_path}")
        return csv_path
    
    def _export_jsonl(self, items: List[FoodItem], filename_prefix: str) -> Path:
        """Экспорт в JSON Lines формат."""
        jsonl_path = self.output_dir / f"{filename_prefix}.jsonl"
        
        with open(jsonl_path, 'w', encoding='utf-8') as f:
            for item in items:
                json_data = item.dict(exclude_none=True)
                # Конвертируем Decimal в float для JSON
                json_data = self._convert_decimals_to_float(json_data)
                f.write(json.dumps(json_data, ensure_ascii=False, default=str) + '\n')
        
        logger.info(f"Exported {len(items)} items to JSONL: {jsonl_path}")
        return jsonl_path
    
    def _export_parquet(self, items: List[FoodItem], filename_prefix: str) -> Path:
        """Экспорт в Parquet формат."""
        parquet_path = self.output_dir / f"{filename_prefix}.parquet"
        
        # Конвертируем в DataFrame
        data = []
        for item in items:
            row = item.dict()
            # Конвертируем списки в строки
            for key, value in row.items():
                if isinstance(value, list):
                    row[key] = "; ".join(map(str, value))
            data.append(row)
        
        df = pd.DataFrame(data)
        
        # Сохраняем в Parquet
        df.to_parquet(parquet_path, index=False, engine='pyarrow')
        
        logger.info(f"Exported {len(items)} items to Parquet: {parquet_path}")
        return parquet_path
    
    def _export_report(self, results: List[ScrapingResult], filename_prefix: str) -> Path:
        """Экспорт отчета о скрейпинге."""
        report_path = self.output_dir / f"{filename_prefix}_report.md"
        
        total_items = sum(result.successful for result in results)
        total_found = sum(result.total_found for result in results)
        total_failed = sum(result.failed for result in results)
        total_duration = sum(result.duration_seconds for result in results)
        
        # Статистика по магазинам
        shop_stats = {}
        for result in results:
            shop_stats[result.shop] = {
                'found': result.total_found,
                'successful': result.successful,
                'failed': result.failed,
                'success_rate': result.successful / result.total_found if result.total_found > 0 else 0,
                'duration': result.duration_seconds
            }
        
        # Статистика по категориям
        category_stats = {}
        all_items = []
        for result in results:
            all_items.extend(result.items)
        
        for item in all_items:
            if item.category not in category_stats:
                category_stats[item.category] = {'count': 0, 'avg_price': 0, 'prices': []}
            category_stats[item.category]['count'] += 1
            category_stats[item.category]['prices'].append(float(item.price))
        
        # Рассчитываем средние цены
        for category, stats in category_stats.items():
            if stats['prices']:
                stats['avg_price'] = sum(stats['prices']) / len(stats['prices'])
        
        # Средние цены за 100г по магазинам
        shop_price_stats = {}
        for item in all_items:
            if item.price_per_100g:
                if item.shop not in shop_price_stats:
                    shop_price_stats[item.shop] = []
                shop_price_stats[item.shop].append(float(item.price_per_100g))
        
        for shop, prices in shop_price_stats.items():
            if prices:
                shop_price_stats[shop] = sum(prices) / len(prices)
        
        # Генерируем отчет
        report_content = f"""# Отчет о скрейпинге готовой еды

## Общая статистика
- **Всего найдено товаров**: {total_found}
- **Успешно обработано**: {total_items}
- **Не удалось обработать**: {total_failed}
- **Процент успеха**: {(total_items / total_found * 100):.1f}%
- **Общее время выполнения**: {total_duration:.1f} сек

## Статистика по магазинам
"""
        
        for shop, stats in shop_stats.items():
            report_content += f"""
### {shop.title()}
- Найдено товаров: {stats['found']}
- Успешно обработано: {stats['successful']}
- Не удалось обработать: {stats['failed']}
- Процент успеха: {(stats['success_rate'] * 100):.1f}%
- Время выполнения: {stats['duration']:.1f} сек
- Средняя цена за 100г: {shop_price_stats.get(shop, 0):.2f} руб
"""
        
        report_content += f"""
## Топ категорий по количеству товаров
"""
        
        sorted_categories = sorted(category_stats.items(), key=lambda x: x[1]['count'], reverse=True)
        for category, stats in sorted_categories[:10]:
            report_content += f"- **{category}**: {stats['count']} товаров (средняя цена: {stats['avg_price']:.2f} руб)\n"
        
        # Статистика полноты нутриентов
        complete_nutrients = sum(1 for item in all_items if item.has_complete_nutrients())
        nutrients_completeness = complete_nutrients / len(all_items) if all_items else 0
        
        report_content += f"""
## Качество данных
- **Товаров с полными нутриентами**: {complete_nutrients} из {len(all_items)} ({nutrients_completeness * 100:.1f}%)
- **Товаров с фото**: {sum(1 for item in all_items if item.photo_url)} из {len(all_items)}
- **Товаров с составом**: {sum(1 for item in all_items if item.composition)} из {len(all_items)}
"""
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"Report exported to: {report_path}")
        return report_path
    
    def _export_validation_issues(self, results: List[ScrapingResult], filename_prefix: str) -> Path:
        """Экспорт проблем валидации."""
        issues_path = self.output_dir / f"{filename_prefix}_issues.csv"
        
        all_issues = []
        for result in results:
            # Собираем проблемы из результатов (если они есть)
            for error in result.errors:
                all_issues.append({
                    'shop': result.shop,
                    'url': '',
                    'issue_type': 'general_error',
                    'description': error,
                    'stage': 'processing',
                    'timestamp': result.scraped_at.isoformat()
                })
        
        if not all_issues:
            return None
        
        with open(issues_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['shop', 'url', 'issue_type', 'description', 'stage', 'timestamp']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_issues)
        
        logger.info(f"Exported {len(all_issues)} validation issues to: {issues_path}")
        return issues_path
    
    def _convert_decimals_to_float(self, obj: Any) -> Any:
        """Рекурсивное преобразование Decimal в float для JSON."""
        if isinstance(obj, dict):
            return {key: self._convert_decimals_to_float(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_decimals_to_float(item) for item in obj]
        elif hasattr(obj, '__dict__'):
            return self._convert_decimals_to_float(obj.__dict__)
        else:
            # Конвертируем Decimal в float
            if hasattr(obj, 'quantize'):  # Это Decimal
                return float(obj)
            return obj

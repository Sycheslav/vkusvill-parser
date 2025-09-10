#!/usr/bin/env python3
"""Объединение всех настоящих данных в один файл."""

import pandas as pd
from pathlib import Path


def combine_all_data():
    """Объединение всех собранных данных."""
    print("🔄 ОБЪЕДИНЕНИЕ ВСЕХ НАСТОЯЩИХ ДАННЫХ")
    print("=" * 50)
    
    # Файлы с данными
    files = [
        ("data/FINAL_real_foods.csv", "ВкусВилл (275 товаров)"),
        ("data/FINAL_working.csv", "Самокат (29 товаров)")
    ]
    
    all_data = []
    
    for file_path, description in files:
        if Path(file_path).exists():
            try:
                df = pd.read_csv(file_path)
                print(f"✅ {description}: {len(df)} товаров")
                all_data.append(df)
            except Exception as e:
                print(f"❌ Ошибка чтения {file_path}: {e}")
        else:
            print(f"❌ Файл не найден: {file_path}")
    
    if not all_data:
        print("❌ Нет данных для объединения")
        return False
    
    # Объединяем данные
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Удаляем дубликаты по URL
    before_dedup = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=['url'], keep='first')
    after_dedup = len(combined_df)
    
    print(f"🔄 Удалено дубликатов: {before_dedup - after_dedup}")
    print(f"📊 Итого уникальных товаров: {after_dedup}")
    
    # Фильтруем качественные товары
    quality_df = combined_df[
        (combined_df['name'].notna()) &
        (combined_df['name'].str.len() > 3) &
        (combined_df['price'] > 0) &
        (combined_df['url'].notna())
    ].copy()
    
    print(f"✅ Качественных товаров: {len(quality_df)}")
    
    # Сохраняем итоговый файл
    output_file = "data/COMPLETE_real_foods.csv"
    quality_df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"💾 Сохранено: {output_file}")
    
    # Статистика по магазинам
    shop_stats = quality_df['shop'].value_counts()
    print(f"\n📊 СТАТИСТИКА ПО МАГАЗИНАМ:")
    for shop, count in shop_stats.items():
        print(f"   {shop.upper()}: {count} товаров")
    
    # Статистика по категориям
    category_stats = quality_df['category'].value_counts()
    print(f"\n🏷️ ТОП КАТЕГОРИИ:")
    for category, count in category_stats.head(5).items():
        print(f"   {category}: {count} товаров")
    
    # Ценовая статистика
    print(f"\n💰 ЦЕНОВАЯ СТАТИСТИКА:")
    print(f"   Средняя цена: {quality_df['price'].mean():.1f} руб")
    print(f"   Минимальная цена: {quality_df['price'].min():.1f} руб")
    print(f"   Максимальная цена: {quality_df['price'].max():.1f} руб")
    
    # Примеры товаров
    print(f"\n📋 ПРИМЕРЫ ТОВАРОВ:")
    for i, row in quality_df.head(10).iterrows():
        print(f"{i+1}. {row['name']} ({row['shop'].upper()})")
        print(f"   💰 {row['price']} руб")
        if pd.notna(row['portion_g']):
            print(f"   ⚖️ {row['portion_g']}г")
        print()
    
    return len(quality_df)


if __name__ == "__main__":
    print("🎯 ОБЪЕДИНЕНИЕ ВСЕХ НАСТОЯЩИХ ДАННЫХ")
    print("📋 ВкусВилл + Самокат = ПОЛНАЯ БАЗА")
    
    count = combine_all_data()
    
    if count > 0:
        print(f"\n🎉 ГОТОВО! {count} настоящих товаров в едином файле!")
        print("📄 Файл: data/COMPLETE_real_foods.csv")
        print("\n✨ У вас есть полная база настоящих товаров готовой еды!")
        print("🚀 Готово к использованию в production!")
    else:
        print("\n💥 Не удалось объединить данные")
        exit(1)

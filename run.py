#!/usr/bin/env python3
"""Главный скрипт запуска парсера готовой еды."""

import subprocess
import sys
from pathlib import Path


def show_menu():
    """Показать главное меню."""
    print("🚀 ПАРСЕР ГОТОВОЙ ЕДЫ")
    print("=" * 50)
    print("1. 📦 Запустить полный парсинг (все магазины)")
    print("2. 🔍 Диагностика Самоката") 
    print("3. 🔍 Диагностика Лавки")
    print("4. 📊 Показать готовые данные")
    print("5. 🧹 Очистить временные файлы")
    print("6. ⚙️ Запустить CLI парсер")
    print("0. ❌ Выход")
    print("-" * 50)


def run_full_scraper():
    """Запуск полного парсера."""
    print("🚀 ЗАПУСК ПОЛНОГО ПАРСЕРА")
    print("-" * 30)
    
    cmd = ["python3", "scripts/final_working_scraper.py"]
    
    try:
        subprocess.run(cmd, check=True)
        print("✅ Парсинг завершен!")
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка парсинга: {e}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")


def run_cli_scraper():
    """Запуск CLI парсера."""
    print("⚙️ CLI ПАРСЕР")
    print("-" * 30)
    
    print("Доступные команды:")
    print("python3 -m app scrape --shop vkusvill --city 'Москва' --out data/vkusvill.csv")
    print("python3 -m app scrape --shop samokat --city 'Москва' --out data/samokat.csv")
    print("python3 -m app scrape --shop lavka --city 'Москва' --out data/lavka.csv")
    print("python3 -m app scrape --shop all --city 'Москва' --out data/all_shops.csv")
    
    cmd_input = input("\nВведите команду или нажмите Enter для примера: ").strip()
    
    if not cmd_input:
        cmd_input = "python3 -m app scrape --shop vkusvill --city 'Москва' --out data/cli_test.csv"
    
    try:
        subprocess.run(cmd_input.split(), check=True)
        print("✅ CLI команда выполнена!")
    except Exception as e:
        print(f"❌ Ошибка CLI: {e}")


def show_data():
    """Показать готовые данные."""
    print("📊 ГОТОВЫЕ ДАННЫЕ")
    print("-" * 30)
    
    data_files = list(Path("data").glob("*.csv"))
    
    if not data_files:
        print("❌ CSV файлы не найдены")
        return
    
    print("📄 Доступные файлы:")
    for i, file in enumerate(data_files, 1):
        try:
            with open(file, 'r') as f:
                lines = sum(1 for line in f) - 1
            size_kb = file.stat().st_size / 1024
            
            print(f"{i}. {file.name}")
            print(f"   📊 Товаров: {lines}")
            print(f"   💾 Размер: {size_kb:.1f} KB")
            print()
        except Exception as e:
            print(f"{i}. {file.name} - ошибка: {e}")
    
    # Рекомендуем лучшие файлы
    best_files = [f for f in data_files if any(word in f.name.lower() for word in ['complete', 'final', 'real'])]
    
    if best_files:
        print("💡 РЕКОМЕНДУЕМЫЕ ФАЙЛЫ:")
        for file in best_files:
            print(f"   📄 {file.name}")


def cleanup():
    """Очистка временных файлов."""
    print("🧹 ОЧИСТКА")
    print("-" * 30)
    
    # Удаляем временные файлы
    temp_patterns = [
        "*.json",
        "data/*test*",
        "data/*debug*",
        "logs/*.log"
    ]
    
    removed = 0
    for pattern in temp_patterns:
        files = list(Path(".").glob(pattern))
        for file in files:
            try:
                file.unlink()
                print(f"🗑️ Удален: {file}")
                removed += 1
            except:
                pass
    
    print(f"✅ Удалено файлов: {removed}")


def main():
    """Главная функция."""
    while True:
        print("\n" + "="*50)
        show_menu()
        
        try:
            choice = input("👉 Выберите опцию: ").strip()
            
            if choice == "0":
                print("👋 До свидания!")
                break
            elif choice == "1":
                run_full_scraper()
            elif choice == "2":
                subprocess.run(["python3", "scripts/debug_samokat.py"])
            elif choice == "3":
                subprocess.run(["python3", "scripts/debug_lavka_real.py"])
            elif choice == "4":
                show_data()
            elif choice == "5":
                cleanup()
            elif choice == "6":
                run_cli_scraper()
            else:
                print("❌ Неверная опция")
                
        except KeyboardInterrupt:
            print("\n👋 Прервано пользователем")
            break
        except Exception as e:
            print(f"❌ Ошибка: {e}")


if __name__ == "__main__":
    print("🎯 ПАРСЕР ГОТОВОЙ ЕДЫ - ГЛАВНОЕ МЕНЮ")
    
    # Показываем текущий статус
    print("\n📊 ТЕКУЩИЙ СТАТУС:")
    
    if Path("data/COMPLETE_real_foods.csv").exists():
        print("✅ Есть готовые данные: data/COMPLETE_real_foods.csv")
    elif Path("data/FINAL_real_foods.csv").exists():
        print("✅ Есть готовые данные: data/FINAL_real_foods.csv")
    else:
        print("⚠️ Готовых данных нет - запустите парсинг")
    
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 Программа завершена")
        sys.exit(0)

#!/usr/bin/env python3
"""Главный скрипт запуска парсера готовой еды."""

import asyncio
import sys
import subprocess
from pathlib import Path


def show_menu():
    """Показать меню опций."""
    print("🚀 ПАРСЕР ГОТОВОЙ ЕДЫ")
    print("=" * 40)
    print("1. 📦 Парсить ВкусВилл (работает)")
    print("2. 🔍 Диагностика Самоката")
    print("3. 🔍 Диагностика Лавки")
    print("4. 📊 Показать готовые данные")
    print("5. 🧹 Очистить временные файлы")
    print("0. ❌ Выход")
    print("-" * 40)


async def run_vkusvill():
    """Запуск парсинга ВкусВилл."""
    print("📦 ЗАПУСК ПАРСИНГА ВКУСВИЛЛ")
    print("-" * 30)
    
    # Используем основной CLI
    import subprocess
    
    cmd = [
        "python3", "-m", "app", "scrape",
        "--shop", "vkusvill",
        "--city", "Москва", 
        "--address", "Красная площадь, 1",
        "--out", "data/vkusvill_latest.csv",
        "--format", "csv", "json",
        "--log-level", "INFO"
    ]
    
    print("🔄 Команда:", " ".join(cmd))
    print("⏳ Запуск...")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            print("✅ Парсинг завершен успешно!")
            print("🔍 Проверьте файл: data/vkusvill_latest.csv")
        else:
            print("❌ Ошибка парсинга:")
            print(result.stderr)
            
    except subprocess.TimeoutExpired:
        print("⏰ Парсинг превысил 5 минут - остановлен")
    except Exception as e:
        print(f"❌ Ошибка запуска: {e}")


def show_existing_data():
    """Показать существующие данные."""
    print("📊 ГОТОВЫЕ ДАННЫЕ")
    print("-" * 30)
    
    data_dir = Path("data")
    if not data_dir.exists():
        print("❌ Директория data/ не найдена")
        return
    
    csv_files = list(data_dir.glob("*.csv"))
    
    if not csv_files:
        print("❌ CSV файлы не найдены")
        return
    
    print("📄 Найденные файлы:")
    for i, file in enumerate(csv_files, 1):
        try:
            # Подсчитываем строки
            with open(file, 'r') as f:
                line_count = sum(1 for line in f) - 1  # -1 для заголовка
            
            file_size = file.stat().st_size / 1024  # KB
            print(f"{i}. {file.name}")
            print(f"   📊 Товаров: {line_count}")
            print(f"   💾 Размер: {file_size:.1f} KB")
            print()
            
        except Exception as e:
            print(f"{i}. {file.name} - ошибка чтения: {e}")
    
    # Рекомендуем лучший файл
    best_files = [f for f in csv_files if "FINAL" in f.name or "real" in f.name]
    if best_files:
        print(f"💡 РЕКОМЕНДУЕМЫЙ ФАЙЛ: {best_files[0].name}")


def cleanup_temp_files():
    """Очистка временных файлов."""
    print("🧹 ОЧИСТКА ВРЕМЕННЫХ ФАЙЛОВ")
    print("-" * 30)
    
    temp_patterns = [
        "debug_*.py",
        "test_*.py", 
        "quick_*.py",
        "*_session.json",
        "data/demo_*",
        "data/test_*",
        "data/quick_*"
    ]
    
    removed_count = 0
    
    for pattern in temp_patterns:
        try:
            files = list(Path(".").glob(pattern))
            for file in files:
                try:
                    file.unlink()
                    print(f"🗑️ Удален: {file}")
                    removed_count += 1
                except Exception as e:
                    print(f"❌ Не удалось удалить {file}: {e}")
        except Exception as e:
            print(f"❌ Ошибка поиска {pattern}: {e}")
    
    print(f"\n✅ Удалено файлов: {removed_count}")


async def main():
    """Главная функция."""
    while True:
        print("\n" + "="*50)
        show_menu()
        
        try:
            choice = input("👉 Выберите опцию (0-5): ").strip()
            
            if choice == "0":
                print("👋 До свидания!")
                break
                
            elif choice == "1":
                await run_vkusvill()
                
            elif choice == "2":
                print("🔍 Запуск диагностики Самоката...")
                subprocess.run(["python3", "debug_samokat.py"])
                
            elif choice == "3":
                print("🔍 Запуск диагностики Лавки...")
                subprocess.run(["python3", "debug_lavka_real.py"])
                
            elif choice == "4":
                show_existing_data()
                
            elif choice == "5":
                cleanup_temp_files()
                
            else:
                print("❌ Неверная опция. Попробуйте снова.")
                
        except KeyboardInterrupt:
            print("\n👋 Прервано пользователем")
            break
        except Exception as e:
            print(f"❌ Ошибка: {e}")


if __name__ == "__main__":
    print("🎯 ПАРСЕР ГОТОВОЙ ЕДЫ")
    print("📋 Главное меню управления")
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Программа завершена")
        sys.exit(0)

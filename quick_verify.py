#!/usr/bin/env python3
"""
⚡ БЫСТРАЯ ПРОВЕРКА ПОСЛЕДНЕГО ФАЙЛА
Автоматически находит последний CSV файл и проверяет его правильность.
"""

import asyncio
import sys
from pathlib import Path
from verify_address_data import AddressVerifier


async def main():
    """Быстрая проверка последнего файла."""
    # Находим последний файл address_fast
    data_dir = Path("data")
    address_files = list(data_dir.glob("address_fast_*.csv"))
    
    if not address_files:
        print("❌ Файлы address_fast_*.csv не найдены")
        return
    
    # Берем последний файл
    latest_file = str(sorted(address_files)[-1])
    
    # Адрес по умолчанию или из аргументов
    address = sys.argv[1] if len(sys.argv) > 1 else "Москва, Тверская, 12"
    sample_size = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    
    print(f"⚡ БЫСТРАЯ ПРОВЕРКА")
    print(f"📁 Последний файл: {latest_file}")
    print(f"📍 Адрес: {address}")
    print(f"🎯 Выборка: {sample_size} товаров")
    print()
    
    verifier = AddressVerifier()
    await verifier.verify_csv_file(latest_file, address, sample_size)


if __name__ == "__main__":
    asyncio.run(main())

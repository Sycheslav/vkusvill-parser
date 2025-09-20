#!/usr/bin/env python3
"""
🤖 ПРОСТОЙ TELEGRAM БОТ ДЛЯ ПАРСИНГА ВКУСВИЛЛ
Только быстрый парсинг по адресу.
"""

import asyncio
import logging
import os
import sys
import time
from pathlib import Path

# Telegram Bot API
try:
    from telegram import Update
    from telegram.ext import Application, CommandHandler, ContextTypes
except ImportError:
    print("❌ Установите python-telegram-bot: pip install python-telegram-bot==20.3")
    sys.exit(1)

import subprocess


class VkusvillSimpleBot:
    """Простой Telegram бот для парсинга ВкусВилл."""
    
    def __init__(self, token: str):
        self.token = token
        self.app = Application.builder().token(token).build()
        self._setup_handlers()
    
    def _setup_handlers(self):
        """Настройка обработчиков команд."""
        self.app.add_handler(CommandHandler("start", self.start_command))
        self.app.add_handler(CommandHandler("help", self.help_command))
        self.app.add_handler(CommandHandler("parse", self.parse_command))
        self.app.add_handler(CommandHandler("status", self.status_command))
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start."""
        welcome_text = """
🤖 **ВкусВилл Парсер Бот**

⚡ `/parse адрес количество` - Парсинг товаров
📊 `/status` - Статус системы  
❓ `/help` - Помощь

**Примеры:**
• `/parse "Москва, Арбат, 15" 1500`
• `/parse "55.7558,37.6176" 1000`

Парсер проверяет доступность товаров по адресу.
        """
        await update.message.reply_text(welcome_text, parse_mode='Markdown')
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /help."""
        help_text = """
📖 **ПОМОЩЬ**

⚡ **Парсер** `/parse`
• Формат: `/parse "Адрес" количество`
• Время: 5-15 секунд
• Качество: 95%+ БЖУ данных

📍 **Адреса:**
• `"Москва, Красная площадь, 1"`
• `"55.7558,37.6176"` (координаты)

⚠️ **Ограничения:**
• Реальная доступность ~100-200 товаров по адресу
• Максимум: 2000 товаров
        """
        await update.message.reply_text(help_text, parse_mode='Markdown')
    
    async def parse_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда парсинга /parse."""
        try:
            # Парсинг аргументов
            if len(context.args) < 2:
                await update.message.reply_text(
                    "❌ Формат: `/parse \"Адрес\" количество`\n\n"
                    "Примеры:\n"
                    "• `/parse \"Москва, Арбат, 15\" 1500`\n"
                    "• `/parse \"55.7558,37.6176\" 1000`",
                    parse_mode='Markdown'
                )
                return
            
            address = context.args[0]
            try:
                limit = int(context.args[1])
                if limit > 2000:
                    limit = 2000
                    await update.message.reply_text(f"⚠️ Лимит уменьшен до {limit}")
            except ValueError:
                await update.message.reply_text("❌ Количество должно быть числом!")
                return
            
            # Отправляем сообщение о начале
            status_msg = await update.message.reply_text(
                f"⚡ **ПАРСИНГ ЗАПУЩЕН**\n\n"
                f"📍 Адрес: `{address}`\n"
                f"🎯 Запрошено: {limit}\n"
                f"🔄 Обработка...",
                parse_mode='Markdown'
            )
            
            # Запуск парсера
            start_time = time.time()
            result = await self._run_parser(address, limit)
            end_time = time.time()
            
            if result['success']:
                # Успешный результат
                await status_msg.edit_text(
                    f"✅ **ПАРСИНГ ЗАВЕРШЕН**\n\n"
                    f"📍 Адрес: `{address}`\n"
                    f"🎯 Запрошено: {limit}\n"
                    f"📦 Получено: **{result['count']}**\n"
                    f"📊 С БЖУ: **{result['bju_count']} ({result['bju_percent']:.1f}%)**\n"
                    f"⏱️ Время: **{end_time - start_time:.1f} сек**",
                    parse_mode='Markdown'
                )
                
                # Отправляем файл
                if result.get('csv_file') and Path(result['csv_file']).exists():
                    with open(result['csv_file'], 'rb') as f:
                        await update.message.reply_document(
                            document=f,
                            filename=f"vkusvill_{int(time.time())}.csv",
                            caption=f"📊 {result['count']} товаров по адресу"
                        )
            else:
                # Ошибка
                await status_msg.edit_text(
                    f"❌ **ОШИБКА**\n\n"
                    f"📍 Адрес: `{address}`\n"
                    f"⚠️ {result['error']}\n\n"
                    f"Попробуйте другой адрес или меньшее количество.",
                    parse_mode='Markdown'
                )
                
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка: {e}")
    
    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда статуса /status."""
        try:
            # Проверяем файлы
            data_dir = Path("data")
            csv_files = list(data_dir.glob("*.csv")) if data_dir.exists() else []
            heavy_files = list(data_dir.glob("moscow_improved_*.csv")) if data_dir.exists() else []
            latest_heavy = sorted(heavy_files)[-1] if heavy_files else None
            
            status_text = f"""
📊 **СТАТУС**

🗃️ **База данных:**
• Файлов: {len(csv_files)}
• База: `{latest_heavy.name if latest_heavy else 'Нет'}`
• Товаров в базе: {self._count_lines(latest_heavy) if latest_heavy else 0}

⚡ **Парсер:**
• Статус: {'✅ Готов' if latest_heavy else '❌ Нет базы'}
• Режим: Быстрый (по адресу)

🤖 **Бот:** v1.0
            """
            
            await update.message.reply_text(status_text, parse_mode='Markdown')
            
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка статуса: {e}")
    
    def _count_lines(self, file_path) -> int:
        """Подсчет строк в файле."""
        if not file_path or not file_path.exists():
            return 0
        try:
            with open(file_path, 'r') as f:
                return sum(1 for _ in f) - 1  # -1 для заголовка
        except:
            return 0
    
    async def _run_parser(self, address: str, limit: int) -> dict:
        """Запуск парсера."""
        try:
            cmd = [sys.executable, "address.py", address, str(limit)]
            
            # Запуск процесса
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=Path.cwd()
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                # Парсинг успешен
                output = stdout.decode('utf-8')
                lines = output.split('\n')
                
                count = 0
                bju_count = 0
                csv_file = ""
                
                for line in lines:
                    if "Всего товаров:" in line:
                        count = int(line.split(':')[1].strip())
                    elif "С БЖУ данными:" in line:
                        bju_text = line.split(':')[1].strip()
                        bju_count = int(bju_text.split()[0])
                    elif "CSV:" in line:
                        csv_file = line.split(':', 1)[1].strip()
                
                bju_percent = (bju_count / count * 100) if count > 0 else 0
                
                return {
                    'success': True,
                    'count': count,
                    'bju_count': bju_count,
                    'bju_percent': bju_percent,
                    'csv_file': csv_file
                }
            else:
                error = stderr.decode('utf-8') or stdout.decode('utf-8')
                return {'success': False, 'error': error[:200]}
                
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def run(self):
        """Запуск бота."""
        print("🤖 Запуск простого Telegram бота...")
        print("📍 Нажмите Ctrl+C для остановки")
        self.app.run_polling()


def main():
    """Главная функция."""
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    
    if not token:
        print("❌ Установите TELEGRAM_BOT_TOKEN")
        print("💡 Получите токен у @BotFather")
        print("export TELEGRAM_BOT_TOKEN='your_token'")
        return
    
    # Настройка логирования
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.WARNING  # Меньше логов
    )
    
    # Создание и запуск бота
    bot = VkusvillSimpleBot(token)
    
    try:
        bot.run()
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен")
    except Exception as e:
        print(f"❌ Ошибка: {e}")


if __name__ == "__main__":
    main()

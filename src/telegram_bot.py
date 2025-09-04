"""
Telegram бот для управления парсером готовой еды
"""
import asyncio
import os
import logging
from typing import Dict, Any, Optional
from pathlib import Path
import json
import tempfile

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)

from src.main import FoodScraper, load_config
from src.utils.logger import setup_logger


class FoodScraperBot:
    """Telegram бот для управления парсером готовой еды"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.bot_token = config.get('telegram_bot_token')
        self.allowed_users = config.get('telegram_allowed_users', [])
        
        # Настройка логирования
        self.logger = setup_logger('telegram_bot', 'INFO', config.get('log_file'))
        
        # Инициализация бота
        self.application = Application.builder().token(self.bot_token).build()
        self._setup_handlers()
        
        # Кэш активных задач
        self.active_tasks = {}
        
    def _setup_handlers(self):
        """Настройка обработчиков команд"""
        # Основные команды
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("status", self.status_command))
        
        # Команды парсера
        self.application.add_handler(CommandHandler("scrape", self.scrape_command))
        self.application.add_handler(CommandHandler("sources", self.sources_command))
        self.application.add_handler(CommandHandler("categories", self.categories_command))
        
        # Обработка callback кнопок
        self.application.add_handler(CallbackQueryHandler(self.button_callback))
        
        # Обработка текстовых сообщений
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text))
        
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /start"""
        if not self._is_user_allowed(update.effective_user.id):
            await update.message.reply_text("❌ У вас нет доступа к этому боту.")
            return
            
        welcome_text = """
🤖 **Бот-парсер готовой еды**

Доступные команды:
/start - Начать работу
/help - Справка по командам
/scrape - Запустить парсинг
/sources - Выбрать источники
/categories - Выбрать категории
/status - Статус текущих задач

Выберите действие или используйте /help для подробной справки.
        """
        
        keyboard = [
            [InlineKeyboardButton("🚀 Запустить парсинг", callback_data="scrape_menu")],
            [InlineKeyboardButton("📊 Выбрать источники", callback_data="sources_menu")],
            [InlineKeyboardButton("🏷️ Выбрать категории", callback_data="categories_menu")],
            [InlineKeyboardButton("📈 Статус", callback_data="status")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(welcome_text, reply_markup=reply_markup, parse_mode='Markdown')
        
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /help"""
        if not self._is_user_allowed(update.effective_user.id):
            await update.message.reply_text("❌ У вас нет доступа к этому боту.")
            return
            
        help_text = """
📚 **Справка по командам**

**Основные команды:**
/start - Начать работу с ботом
/help - Показать эту справку
/status - Показать статус текущих задач

**Парсинг:**
/scrape - Запустить парсинг с текущими настройками
/sources - Выбрать источники для парсинга
/categories - Выбрать категории продуктов

**Примеры использования:**
• `/scrape` - запуск парсинга Самоката в Москве
• `/sources samokat lavka` - парсинг Самоката и Лавки
• `/categories готовые блюда салаты` - парсинг конкретных категорий

**Поддерживаемые источники:**
• samokat - Самокат
• lavka - Яндекс.Лавка  
• vkusvill - ВкусВилл

**Форматы экспорта:**
• CSV - таблица Excel
• JSONL - структурированные данные
• SQLite - база данных
        """
        
        await update.message.reply_text(help_text, parse_mode='Markdown')
        
    async def scrape_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /scrape"""
        if not self._is_user_allowed(update.effective_user.id):
            await update.message.reply_text("❌ У вас нет доступа к этому боту.")
            return
            
        # Парсим аргументы команды
        args = context.args if context.args else []
        
        # Создаем конфигурацию для парсинга
        scrape_config = self.config.copy()
        
        # Обрабатываем аргументы
        if args:
            # Источники
            if 'samokat' in args or 'lavka' in args or 'vkusvill' in args:
                sources = [arg for arg in args if arg in ['samokat', 'lavka', 'vkusvill']]
                scrape_config['sources'] = sources
                
            # Категории
            categories = [arg for arg in args if arg not in ['samokat', 'lavka', 'vkusvill']]
            if categories:
                scrape_config['categories'] = categories
                
        # Запускаем парсинг
        await self._start_scraping(update, context, scrape_config)
        
    async def sources_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /sources"""
        if not self._is_user_allowed(update.effective_user.id):
            await update.message.reply_text("❌ У вас нет доступа к этому боту.")
            return
            
        keyboard = [
            [InlineKeyboardButton("Самокат", callback_data="source_samokat")],
            [InlineKeyboardButton("Яндекс.Лавка", callback_data="source_lavka")],
            [InlineKeyboardButton("ВкусВилл", callback_data="source_vkusvill")],
            [InlineKeyboardButton("Все источники", callback_data="source_all")],
            [InlineKeyboardButton("Назад", callback_data="main_menu")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "🔍 **Выберите источники для парсинга:**\n\n"
            "• Самокат - готовые блюда и кулинария\n"
            "• Яндекс.Лавка - продукты и готовая еда\n"
            "• ВкусВилл - фермерские продукты и готовые блюда",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
    async def categories_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /categories"""
        if not self._is_user_allowed(update.effective_user.id):
            await update.message.reply_text("❌ У вас нет доступа к этому боту.")
            return
            
        keyboard = [
            [InlineKeyboardButton("Готовые блюда", callback_data="cat_ready_food")],
            [InlineKeyboardButton("Салаты", callback_data="cat_salads")],
            [InlineKeyboardButton("Супы", callback_data="cat_soups")],
            [InlineKeyboardButton("Кулинария", callback_data="cat_cooking")],
            [InlineKeyboardButton("Все категории", callback_data="cat_all")],
            [InlineKeyboardButton("Назад", callback_data="main_menu")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "🏷️ **Выберите категории продуктов:**\n\n"
            "• Готовые блюда - основные блюда\n"
            "• Салаты - свежие и готовые салаты\n"
            "• Супы - горячие и холодные супы\n"
            "• Кулинария - выпечка и десерты",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /status"""
        if not self._is_user_allowed(update.effective_user.id):
            await update.message.reply_text("❌ У вас нет доступа к этому боту.")
            return
            
        user_id = update.effective_user.id
        
        if user_id in self.active_tasks:
            task_info = self.active_tasks[user_id]
            status_text = f"""
📊 **Статус задачи:**

🆔 ID: {task_info['task_id']}
📅 Время запуска: {task_info['start_time']}
🔍 Источники: {', '.join(task_info['sources'])}
🏷️ Категории: {', '.join(task_info['categories']) if task_info['categories'] else 'Все'}
⏱️ Статус: {task_info['status']}
📈 Прогресс: {task_info.get('progress', 'Неизвестно')}
            """
        else:
            status_text = "📊 **Нет активных задач**\n\nИспользуйте /scrape для запуска парсинга."
            
        await update.message.reply_text(status_text, parse_mode='Markdown')
        
    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка нажатий на inline кнопки"""
        if not self._is_user_allowed(update.effective_user.id):
            await update.callback_query.answer("❌ У вас нет доступа к этому боту.")
            return
            
        query = update.callback_query
        await query.answer()
        
        data = query.data
        
        if data == "main_menu":
            await self._show_main_menu(query)
        elif data == "scrape_menu":
            await self._show_scrape_menu(query)
        elif data.startswith("source_"):
            await self._handle_source_selection(query, data)
        elif data.startswith("cat_"):
            await self._handle_category_selection(query, data)
        elif data == "start_scraping":
            await self._start_scraping_from_menu(query, context)
        elif data == "status":
            await self._show_status(query)
            
    async def _show_main_menu(self, query):
        """Показать главное меню"""
        keyboard = [
            [InlineKeyboardButton("🚀 Запустить парсинг", callback_data="scrape_menu")],
            [InlineKeyboardButton("📊 Выбрать источники", callback_data="sources_menu")],
            [InlineKeyboardButton("🏷️ Выбрать категории", callback_data="categories_menu")],
            [InlineKeyboardButton("📈 Статус", callback_data="status")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "🤖 **Главное меню**\n\nВыберите действие:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
    async def _show_scrape_menu(self, query):
        """Показать меню парсинга"""
        keyboard = [
            [InlineKeyboardButton("🚀 Запустить парсинг", callback_data="start_scraping")],
            [InlineKeyboardButton("📊 Выбрать источники", callback_data="sources_menu")],
            [InlineKeyboardButton("🏷️ Выбрать категории", callback_data="categories_menu")],
            [InlineKeyboardButton("Назад", callback_data="main_menu")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "🚀 **Меню парсинга**\n\n"
            "Текущие настройки:\n"
            f"📍 Город: {self.config.get('city', 'Москва')}\n"
            f"🔍 Источники: {', '.join(self.config.get('sources', ['samokat']))}\n"
            f"🏷️ Категории: {', '.join(self.config.get('categories', [])) or 'Все'}\n\n"
            "Выберите действие:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
    async def _handle_source_selection(self, query, data):
        """Обработка выбора источников"""
        source = data.replace("source_", "")
        
        if source == "all":
            self.config['sources'] = ['samokat', 'lavka', 'vkusvill']
        else:
            if source not in self.config.get('sources', []):
                self.config['sources'] = [source]
            else:
                self.config['sources'].remove(source)
                
        # Показываем обновленное меню
        await self._show_scrape_menu(query)
        
    async def _handle_category_selection(self, query, data):
        """Обработка выбора категорий"""
        category = data.replace("cat_", "")
        
        if category == "all":
            self.config['categories'] = []
        else:
            category_map = {
                "ready_food": "готовые блюда",
                "salads": "салаты", 
                "soups": "супы",
                "cooking": "кулинария"
            }
            
            if category in category_map:
                cat_name = category_map[category]
                if cat_name not in self.config.get('categories', []):
                    self.config['categories'] = [cat_name]
                else:
                    self.config['categories'].remove(cat_name)
                    
        # Показываем обновленное меню
        await self._show_scrape_menu(query)
        
    async def _start_scraping_from_menu(self, query, context):
        """Запуск парсинга из меню"""
        await query.edit_message_text("🚀 **Запуск парсинга...**\n\nПодождите, это может занять несколько минут.")
        
        # Запускаем парсинг
        await self._start_scraping(query, context, self.config)
        
    async def _start_scraping(self, update, context, scrape_config):
        """Запуск процесса парсинга"""
        user_id = update.effective_user.id if hasattr(update, 'effective_user') else update.from_user.id
        
        # Создаем задачу
        task_id = f"task_{user_id}_{int(asyncio.get_event_loop().time())}"
        
        task_info = {
            'task_id': task_id,
            'start_time': asyncio.get_event_loop().time(),
            'sources': scrape_config.get('sources', ['samokat']),
            'categories': scrape_config.get('categories', []),
            'status': 'Запуск...',
            'progress': '0%'
        }
        
        self.active_tasks[user_id] = task_info
        
        # Запускаем парсинг в фоне
        asyncio.create_task(self._run_scraping_task(update, context, scrape_config, task_info))
        
    async def _run_scraping_task(self, update, context, scrape_config, task_info):
        """Выполнение задачи парсинга"""
        user_id = update.effective_user.id if hasattr(update, 'effective_user') else update.from_user.id
        
        try:
            # Обновляем статус
            task_info['status'] = 'Выполняется...'
            task_info['progress'] = '10%'
            
            # Создаем скрейпер
            scraper = FoodScraper(scrape_config)
            
            # Обновляем статус
            task_info['status'] = 'Парсинг данных...'
            task_info['progress'] = '30%'
            
            # Запускаем парсинг
            result = await scraper.run()
            
            # Обновляем статус
            task_info['status'] = 'Экспорт данных...'
            task_info['progress'] = '80%'
            
            if result:
                # Экспортируем данные
                export_files = await self._export_data(scraper, user_id)
                
                task_info['status'] = 'Завершено'
                task_info['progress'] = '100%'
                
                # Отправляем результаты
                await self._send_results(update, context, export_files, task_info)
                
            else:
                task_info['status'] = 'Ошибка'
                await update.message.reply_text("❌ **Ошибка парсинга**\n\nПроверьте логи для деталей.")
                
        except Exception as e:
            task_info['status'] = f'Ошибка: {str(e)}'
            await update.message.reply_text(f"❌ **Критическая ошибка:**\n\n{str(e)}")
            self.logger.error(f"Ошибка в задаче парсинга: {e}")
            
        finally:
            # Удаляем задачу из активных
            if user_id in self.active_tasks:
                del self.active_tasks[user_id]
                
    async def _export_data(self, scraper, user_id):
        """Экспорт данных в различные форматы"""
        export_files = {}
        
        try:
            # Экспорт в CSV
            csv_file = f"data/out/products_{user_id}.csv"
            await scraper.export_csv(csv_file)
            export_files['csv'] = csv_file
            
            # Экспорт в JSONL
            jsonl_file = f"data/out/products_{user_id}.jsonl"
            await scraper.export_jsonl(jsonl_file)
            export_files['jsonl'] = jsonl_file
            
            # Экспорт в SQLite
            sqlite_file = f"data/out/products_{user_id}.db"
            await scraper.export_sqlite(sqlite_file)
            export_files['sqlite'] = sqlite_file
            
        except Exception as e:
            self.logger.error(f"Ошибка экспорта данных: {e}")
            
        return export_files
        
    async def _send_results(self, update, context, export_files, task_info):
        """Отправка результатов парсинга"""
        results_text = f"""
✅ **Парсинг завершен успешно!**

📊 **Результаты:**
• Источники: {', '.join(task_info['sources'])}
• Категории: {', '.join(task_info['categories']) if task_info['categories'] else 'Все'}
• Время выполнения: {int(asyncio.get_event_loop().time() - task_info['start_time'])} сек

📁 **Файлы для скачивания:**
        """
        
        # Отправляем текстовое сообщение
        await update.message.reply_text(results_text, parse_mode='Markdown')
        
        # Отправляем файлы
        for format_name, file_path in export_files.items():
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'rb') as f:
                        await context.bot.send_document(
                            chat_id=update.effective_chat.id,
                            document=f,
                            filename=f"products_{format_name}.{format_name}",
                            caption=f"📁 Данные в формате {format_name.upper()}"
                        )
                except Exception as e:
                    self.logger.error(f"Ошибка отправки файла {file_path}: {e}")
                    
    async def handle_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка текстовых сообщений"""
        if not self._is_user_allowed(update.effective_user.id):
            await update.message.reply_text("❌ У вас нет доступа к этому боту.")
            return
            
        text = update.message.text.lower()
        
        if any(word in text for word in ['помощь', 'help', 'справка']):
            await self.help_command(update, context)
        elif any(word in text for word in ['статус', 'status']):
            await self.status_command(update, context)
        elif any(word in text for word in ['парсинг', 'scrape', 'запуск']):
            await self.scrape_command(update, context)
        else:
            await update.message.reply_text(
                "💡 Используйте команды или меню для управления ботом.\n"
                "Напишите /help для справки."
            )
            
    def _is_user_allowed(self, user_id: int) -> bool:
        """Проверка доступа пользователя"""
        if not self.allowed_users:
            return True  # Если список пуст, доступ для всех
        return user_id in self.allowed_users
        
    async def run(self):
        """Запуск бота"""
        self.logger.info("Запуск Telegram бота...")
        
        # Запускаем бота
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()
        
        self.logger.info("Telegram бот запущен и ожидает сообщения")
        
        # Держим бота запущенным
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            self.logger.info("Получен сигнал остановки")
        finally:
            await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()
            
    def stop(self):
        """Остановка бота"""
        self.logger.info("Остановка Telegram бота...")
        asyncio.create_task(self.application.shutdown())

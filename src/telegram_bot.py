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
        
        # Инициализация бота с увеличенными таймаутами
        self.application = (
            Application.builder()
            .token(self.bot_token)
            .connect_timeout(120.0)  # 120 секунд на подключение
            .read_timeout(120.0)     # 120 секунд на чтение
            .write_timeout(120.0)    # 120 секунд на запись
            .build()
        )
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
        self.application.add_handler(CommandHandler("scrape_all", self.scrape_all_command))
        self.application.add_handler(CommandHandler("test_samokat", self.test_samokat_command))
        self.application.add_handler(CommandHandler("test_lavka", self.test_lavka_command))
        self.application.add_handler(CommandHandler("test_vkusvill", self.test_vkusvill_command))
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
🤖 *Бот-парсер готовой еды*

Доступные команды:
/start - Начать работу
/help - Справка по командам
/scrape\\_all - Запустить парсинг всех источников (рекомендуется)
/scrape - Запустить парсинг с настройками
/sources - Выбрать источники
/categories - Выбрать категории
/status - Статус текущих задач

Выберите действие или используйте /help для подробной справки.
        """
        
        keyboard = [
            [InlineKeyboardButton("🚀 Парсинг всех источников", callback_data="scrape_all")],
            [InlineKeyboardButton("⚙️ Настройки парсинга", callback_data="scrape_menu")],
            [InlineKeyboardButton("🧪 Тестирование источников", callback_data="test_sources")],
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
📚 *Справка по командам*

*Основные команды:*
/start - Начать работу с ботом
/help - Показать эту справку
/status - Показать статус текущих задач

*Парсинг:*
/scrape\\_all - Запустить парсинг всех источников (рекомендуется)
/scrape - Запустить парсинг с текущими настройками
/sources - Выбрать источники для парсинга
/categories - Выбрать категории продуктов

*Тестирование отдельных источников:*
/test\\_samokat - Тестирование Самоката
/test\\_lavka - Тестирование Яндекс.Лавки
/test\\_vkusvill - Тестирование ВкусВилла

*Примеры использования:*
• /scrape\\_all - запуск парсинга всех источников (рекомендуется)
• /scrape - запуск парсинга с текущими настройками
• /test\\_samokat - быстрое тестирование Самоката
• /test\\_lavka - быстрое тестирование Лавки
• /test\\_vkusvill - быстрое тестирование ВкусВилла
• /sources samokat lavka - парсинг Самоката и Лавки
• /categories готовые блюда салаты - парсинг конкретных категорий

*Поддерживаемые источники:*
• samokat - Самокат
• lavka - Яндекс.Лавка  
• vkusvill - ВкусВилл

*Форматы экспорта:*
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
        
    async def scrape_all_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /scrape_all - парсинг всех источников"""
        if not self._is_user_allowed(update.effective_user.id):
            await update.message.reply_text("❌ У вас нет доступа к этому боту.")
            return
            
        # Создаем конфигурацию для парсинга всех источников с оптимизированными настройками
        scrape_config = self.config.copy()
        scrape_config['sources'] = ['samokat', 'lavka', 'vkusvill']
        scrape_config['limit'] = 500  # Увеличиваем лимит для быстрого парсинга
        scrape_config['headless'] = True  # Скрытый браузер для ускорения
        scrape_config['max_concurrent'] = 4  # Увеличиваем параллельность
        
        await update.message.reply_text("🚀 *Запуск парсинга всех источников*\n\n" +
                                       "📊 *Источники:* Самокат, Яндекс.Лавка, ВкусВилл\n" +
                                       "🏷️ *Категории:* Автоопределение\n" +
                                       "📦 *Лимит:* До 500 товаров на категорию\n" +
                                       "⏳ *Время:* Ожидайте, это может занять 5-10 минут")
        
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
            "🔍 *Выберите источники для парсинга:*\n\n"
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
            "🏷️ *Выберите категории продуктов:*\n\n"
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
📊 *Статус задачи:*

🆔 ID: {task_info['task_id']}
📅 Время запуска: {task_info['start_time']}
🔍 Источники: {', '.join(task_info['sources'])}
🏷️ Категории: {', '.join(task_info['categories']) if task_info['categories'] else 'Все'}
⏱️ Статус: {task_info['status']}
📈 Прогресс: {task_info.get('progress', 'Неизвестно')}
            """
        else:
            status_text = "📊 *Нет активных задач*\n\nИспользуйте /scrape для запуска парсинга."
            
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
        elif data == "scrape_all":
            await self._start_scraping_all_from_menu(query, context)
        elif data == "test_sources":
            await self._show_test_sources_menu(query)
        elif data.startswith("test_"):
            await self._handle_test_source(query, context, data)
        elif data == "status":
            await self._show_status(query)
            
    async def _show_main_menu(self, query):
        """Показать главное меню"""
        keyboard = [
            [InlineKeyboardButton("🚀 Парсинг всех источников", callback_data="scrape_all")],
            [InlineKeyboardButton("⚙️ Настройки парсинга", callback_data="scrape_menu")],
            [InlineKeyboardButton("🧪 Тестирование источников", callback_data="test_sources")],
            [InlineKeyboardButton("📊 Выбрать источники", callback_data="sources_menu")],
            [InlineKeyboardButton("🏷️ Выбрать категории", callback_data="categories_menu")],
            [InlineKeyboardButton("📈 Статус", callback_data="status")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "🤖 *Главное меню*\n\nВыберите действие:",
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
            "⚙️ *Настройки парсинга*\n\n"
            "Текущие настройки:\n"
            f"📍 Город: {self.config.get('city', 'Москва')}\n"
            f"🔍 Источники: {', '.join(self.config.get('sources', ['samokat', 'lavka', 'vkusvill']))}\n"
            f"🏷️ Категории: {', '.join(self.config.get('categories', [])) or 'Автоопределение'}\n\n"
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
        
    async def _show_test_sources_menu(self, query):
        """Показать меню тестирования источников"""
        keyboard = [
            [InlineKeyboardButton("🧪 Тест Самоката", callback_data="test_samokat")],
            [InlineKeyboardButton("🧪 Тест Яндекс.Лавки", callback_data="test_lavka")],
            [InlineKeyboardButton("🧪 Тест ВкусВилла", callback_data="test_vkusvill")],
            [InlineKeyboardButton("Назад", callback_data="main_menu")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "🧪 *Тестирование источников*\n\n"
            "Выберите источник для тестирования:\n\n"
            "• *Самокат* - готовые блюда и кулинария\n"
            "• *Яндекс.Лавка* - продукты и готовая еда\n"
            "• *ВкусВилл* - фермерские продукты и готовые блюда\n\n"
            "Тестирование займет 3-5 минут на источник.",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
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
        
    async def _handle_test_source(self, query, context, data):
        """Обработка тестирования отдельного источника"""
        source = data.replace("test_", "")
        
        if source == "samokat":
            await self._start_test_scraping(query, context, ['samokat'], "Самокат")
        elif source == "lavka":
            await self._start_test_scraping(query, context, ['lavka'], "Яндекс.Лавка")
        elif source == "vkusvill":
            await self._start_test_scraping(query, context, ['vkusvill'], "ВкусВилл")
        else:
            await query.answer("❌ Неизвестный источник для тестирования")
            
    async def _start_test_scraping(self, query, context, sources, source_name):
        """Запуск тестового парсинга"""
        await query.edit_message_text(f"🧪 *Тестирование {source_name}...*\n\n" +
                                    f"🔍 *Источник:* {source_name}\n" +
                                    f"🏷️ *Категории:* Автоопределение\n" +
                                    f"⏳ *Время:* Ожидайте, это может занять 3-5 минут")
        
        # Создаем конфигурацию для тестирования
        scrape_config = self.config.copy()
        scrape_config['sources'] = sources
        
        # Запускаем парсинг
        await self._start_scraping(query, context, scrape_config)
        
    async def _start_scraping_from_menu(self, query, context):
        """Запуск парсинга из меню"""
        await query.edit_message_text("🚀 *Запуск парсинга...*\n\nПодождите, это может занять несколько минут.")
        
        # Запускаем парсинг
        await self._start_scraping(query, context, self.config)
        
    async def _start_scraping_all_from_menu(self, query, context):
        """Запуск парсинга всех источников из меню"""
        await query.edit_message_text("🚀 *Запуск парсинга всех источников...*\n\n" +
                                    "📊 *Источники:* Самокат, Яндекс.Лавка, ВкусВилл\n" +
                                    "🏷️ *Категории:* Автоопределение\n" +
                                    "📦 *Лимит:* До 500 товаров на категорию\n" +
                                    "⏳ *Время:* Ожидайте, это может занять 5-10 минут")
        
        # Создаем конфигурацию для всех источников с оптимизированными настройками
        scrape_config = self.config.copy()
        scrape_config['sources'] = ['samokat', 'lavka', 'vkusvill']
        scrape_config['limit'] = 500  # Увеличиваем лимит для быстрого парсинга
        scrape_config['headless'] = True  # Скрытый браузер для ускорения
        scrape_config['max_concurrent'] = 4  # Увеличиваем параллельность
        
        # Запускаем парсинг
        await self._start_scraping(query, context, scrape_config)
        
    async def test_samokat_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /test_samokat - тестирование Самоката"""
        if not self._is_user_allowed(update.effective_user.id):
            await update.message.reply_text("❌ У вас нет доступа к этому боту.")
            return
            
        scrape_config = self.config.copy()
        scrape_config['sources'] = ['samokat']
        
        await update.message.reply_text("🧪 *Тестирование Самоката*\n\n" +
                                       "🔍 *Источник:* Самокат\n" +
                                       "🏷️ *Категории:* Автоопределение\n" +
                                       "📦 *Лимит:* До 500 товаров на категорию\n" +
                                       "⏳ *Время:* Ожидайте, это может занять 2-3 минуты")
        
        # Добавляем оптимизированные настройки
        scrape_config['limit'] = 500
        scrape_config['headless'] = True
        scrape_config['max_concurrent'] = 4
        
        await self._start_scraping(update, context, scrape_config)
        
    async def test_lavka_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /test_lavka - тестирование Лавки"""
        if not self._is_user_allowed(update.effective_user.id):
            await update.message.reply_text("❌ У вас нет доступа к этому боту.")
            return
            
        scrape_config = self.config.copy()
        scrape_config['sources'] = ['lavka']
        
        await update.message.reply_text("🧪 *Тестирование Яндекс.Лавки*\n\n" +
                                       "🔍 *Источник:* Яндекс.Лавка\n" +
                                       "🏷️ *Категории:* Автоопределение\n" +
                                       "📦 *Лимит:* До 500 товаров на категорию\n" +
                                       "⏳ *Время:* Ожидайте, это может занять 2-3 минуты")
        
        # Добавляем оптимизированные настройки
        scrape_config['limit'] = 500
        scrape_config['headless'] = True
        scrape_config['max_concurrent'] = 4
        
        await self._start_scraping(update, context, scrape_config)
        
    async def test_vkusvill_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /test_vkusvill - тестирование ВкусВилла"""
        if not self._is_user_allowed(update.effective_user.id):
            await update.message.reply_text("❌ У вас нет доступа к этому боту.")
            return
            
        scrape_config = self.config.copy()
        scrape_config['sources'] = ['vkusvill']
        
        await update.message.reply_text("🧪 *Тестирование ВкусВилла*\n\n" +
                                       "🔍 *Источник:* ВкусВилл\n" +
                                       "🏷️ *Категории:* Автоопределение\n" +
                                       "📦 *Лимит:* До 500 товаров на категорию\n" +
                                       "⏳ *Время:* Ожидайте, это может занять 2-3 минуты")
        
        # Добавляем оптимизированные настройки
        scrape_config['limit'] = 500
        scrape_config['headless'] = True
        scrape_config['max_concurrent'] = 4
        
        await self._start_scraping(update, context, scrape_config)
        
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
        message = update.message if hasattr(update, 'message') else update
        
        try:
            # Создаем скрейпер
            try:
                scraper = FoodScraper(scrape_config)
            except Exception as e:
                await message.reply_text(f"❌ *Ошибка инициализации скрейпера:* {str(e)}")
                self.logger.error(f"Ошибка инициализации скрейпера: {e}")
                return
                
            # Запускаем парсинг с упрощенным логированием
            result = await self._run_scraping_with_progress(scraper, message, task_info)
            
            if result:
                # Экспортируем данные
                export_files = await self._export_data(scraper, user_id)
                
                # Отправляем результаты
                await self._send_results(message, context, export_files, task_info)
                
            else:
                task_info['status'] = 'Ошибка'
                await message.reply_text("❌ *Парсинг завершен с ошибками*\n\nПроверьте логи для деталей")
                
        except Exception as e:
            task_info['status'] = f'Ошибка: {str(e)}'
            await message.reply_text(f"❌ *Критическая ошибка:*\n\n{str(e)}")
            self.logger.error(f"Ошибка в задаче парсинга: {e}")
            
        finally:
            # Удаляем задачу из активных
            if user_id in self.active_tasks:
                del self.active_tasks[user_id]
                
    async def _run_scraping_with_progress(self, scraper, message, task_info):
        """Запуск парсинга с упрощенным логированием (3-5 сообщений)"""
        try:
            self.logger.info("_run_scraping_with_progress вызван")
            
            # Получаем список источников
            sources = list(scraper.scrapers.keys())
            total_sources = len(sources)
            self.logger.info(f"Найдено источников: {total_sources}, список: {sources}")
            
            if total_sources == 0:
                self.logger.error("Не найдено ни одного источника для парсинга")
                await message.reply_text("❌ *Ошибка:* Не найдено ни одного источника для парсинга")
                return False
                
            # Проверяем, что все скрейперы инициализированы
            for source_name in sources:
                self.logger.info(f"Проверяем скрейпер {source_name}: {scraper.scrapers[source_name]}")
                if not scraper.scrapers[source_name]:
                    self.logger.error(f"Скрейпер {source_name} не инициализирован")
                    await message.reply_text(f"❌ *Ошибка:* Скрейпер {source_name} не инициализирован")
                    return False
                
            # Сообщение 1: Начало парсинга
            self.logger.info("Отправляем сообщение о начале парсинга")
            await message.reply_text(f"🚀 *Начинаем парсинг {total_sources} источников*\n" +
                                   f"🔍 *Источники:* {', '.join(sources)}")
            
            all_products = {}
            total_products = 0
            self.logger.info("Начинаем парсинг источников")
            
            # Парсим каждый источник по очереди
            for i, (source_name, scraper_instance) in enumerate(scraper.scrapers.items()):
                try:
                    self.logger.info(f"Начинаем парсинг источника {source_name} ({i+1}/{total_sources})")
                    
                    # Сообщение 2: Парсинг источника
                    await message.reply_text(f"🔍 *Парсинг {source_name}...*")
                    
                    # Получаем категории для источника
                    categories = task_info.get('categories', [])
                    self.logger.info(f"Категории из task_info: {categories}")
                    
                    if not categories:
                        try:
                            self.logger.info(f"Получаем категории для {source_name}")
                            source_categories = await scraper_instance.get_categories()
                            self.logger.info(f"Получены категории для {source_name}: {source_categories}")
                            
                            # Фильтруем только категории готовой еды
                            filtered_categories = [cat for cat in source_categories if any(
                                keyword in cat.lower() for keyword in 
                                ['готов', 'кулинар', 'салат', 'суп', 'блюд', 'еда', 'кухня', 'кулинар']
                            )]
                            categories = filtered_categories[:5]  # Берем первые 5 категорий
                            self.logger.info(f"Отфильтрованные категории для {source_name}: {categories}")
                        except Exception as e:
                            self.logger.warning(f"Не удалось получить категории для {source_name}: {e}")
                            categories = ['Готовая еда', 'Кулинария', 'Салаты', 'Супы', 'Горячие блюда']
                            self.logger.info(f"Используем стандартные категории для {source_name}: {categories}")
                    
                    # Парсим каждую категорию (без лишних сообщений)
                    source_products = []
                    self.logger.info(f"Начинаем парсинг {len(categories)} категорий для {source_name}")
                    
                    for j, category in enumerate(categories):
                        try:
                            self.logger.info(f"Парсим категорию {category} ({j+1}/{len(categories)}) для {source_name}")
                            category_products = await scraper_instance.scrape_category(category, scraper.config.get('limit'))
                            self.logger.info(f"Получено {len(category_products)} товаров из категории {category} для {source_name}")
                            source_products.extend(category_products)
                        except Exception as e:
                            self.logger.error(f"Ошибка в категории {category} для {source_name}: {e}")
                            continue
                    
                    # Сохраняем продукты источника
                    self.logger.info(f"Всего получено {len(source_products)} товаров для {source_name}")
                    all_products[source_name] = source_products
                    total_products += len(source_products)
                    
                    # Сообщение 3: Результаты по источнику
                    await message.reply_text(f"✅ *{source_name} завершен*\n" +
                                           f"📦 *Найдено продуктов:* {len(source_products)}")
                    
                except Exception as e:
                    await message.reply_text(f"❌ *Ошибка в {source_name}:* {str(e)}")
                    all_products[source_name] = []
                    continue
            
            # Сохраняем все продукты в базу данных
            self.logger.info(f"Всего собрано продуктов: {total_products}")
            
            if total_products > 0:
                # Сообщение 4: Сохранение
                self.logger.info("Начинаем сохранение продуктов в БД")
                await message.reply_text(f"💾 *Сохранение {total_products} продуктов в БД...*")
                
                try:
                    saved_count = await scraper.save_products(all_products)
                    self.logger.info(f"Сохранено в БД: {saved_count} продуктов")
                except Exception as e:
                    self.logger.error(f"Ошибка сохранения в БД: {e}")
                    saved_count = 0
                
                # Сообщение 5: Завершение
                await message.reply_text(f"🎉 *Парсинг завершен успешно!*\n" +
                                       f"📊 *Всего продуктов:* {total_products}\n" +
                                       f"💾 *Сохранено в БД:* {saved_count}")
                
                return True
            else:
                self.logger.warning("Не найдено ни одного продукта")
                await message.reply_text("❌ *Не найдено ни одного продукта*\n\n" +
                                       "Проверьте логи для деталей")
                return False
                
        except Exception as e:
            await message.reply_text(f"❌ *Критическая ошибка:* {str(e)}")
            self.logger.error(f"Ошибка в _run_scraping_with_progress: {e}")
            return False
                
    async def _export_data(self, scraper, user_id):
        """Экспорт данных в различные форматы"""
        export_files = {}
        
        try:
            # Получаем все продукты из базы данных
            try:
                all_products = scraper.storage.get_all_products()
            except Exception as e:
                self.logger.error(f"Ошибка получения продуктов из БД: {e}")
                return export_files
            
            if not all_products:
                self.logger.warning("Нет продуктов для экспорта")
                return export_files
            
            # Экспорт в CSV
            try:
                csv_file = f"data/out/products_{user_id}.csv"
                success = scraper.storage.export_to_csv(csv_file, all_products)
                if success:
                    export_files['csv'] = csv_file
            except Exception as e:
                self.logger.error(f"Ошибка экспорта в CSV: {e}")
            
            # Экспорт в JSONL
            try:
                jsonl_file = f"data/out/products_{user_id}.jsonl"
                success = scraper.storage.export_to_jsonl(jsonl_file, all_products)
                if success:
                    export_files['jsonl'] = jsonl_file
            except Exception as e:
                self.logger.error(f"Ошибка экспорта в JSONL: {e}")
            
            # Экспорт в SQLite
            try:
                sqlite_file = f"data/out/products_{user_id}.db"
                success = scraper.storage.export_to_sqlite(sqlite_file, all_products)
                if success:
                    export_files['sqlite'] = sqlite_file
            except Exception as e:
                self.logger.error(f"Ошибка экспорта в SQLite: {e}")
            
        except Exception as e:
            self.logger.error(f"Ошибка экспорта данных: {e}")
            
        return export_files
        
    async def _send_results(self, update, context, export_files, task_info):
        """Отправка результатов парсинга"""
        results_text = f"""
✅ *Парсинг завершен успешно!*

📊 *Результаты:*
• Источники: {', '.join(task_info['sources'])}
• Категории: {', '.join(task_info['categories']) if task_info['categories'] else 'Все'}
• Время выполнения: {int(asyncio.get_event_loop().time() - task_info['start_time'])} сек

📁 *Файлы для скачивания:*
        """
        
        # Определяем, как отправлять сообщения (обычное сообщение или callback)
        if hasattr(update, 'message'):
            # Обычное сообщение
            message = update.message
            chat_id = update.effective_chat.id
        else:
            # Callback
            message = update
            chat_id = message.chat.id if hasattr(message, 'chat') else None
        
        # Отправляем текстовое сообщение
        try:
            await message.reply_text(results_text, parse_mode='Markdown')
        except Exception as e:
            self.logger.error(f"Ошибка отправки текстового сообщения: {e}")
            # Пытаемся отправить через context.bot
            if chat_id:
                await context.bot.send_message(chat_id=chat_id, text=results_text, parse_mode='Markdown')
        
        # Отправляем файлы
        for format_name, file_path in export_files.items():
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'rb') as f:
                        await context.bot.send_document(
                            chat_id=chat_id or (message.chat.id if hasattr(message, 'chat') else None),
                            document=f,
                            filename=f"products_{format_name}.{format_name}",
                            caption=f"📁 Данные в формате {format_name.upper()}"
                        )
                except Exception as e:
                    self.logger.error(f"Ошибка отправки файла {file_path}: {e}")
                    try:
                        await message.reply_text(f"⚠️ *Ошибка отправки файла {format_name}:* {str(e)}")
                    except:
                        if chat_id:
                            await context.bot.send_message(chat_id=chat_id, text=f"⚠️ Ошибка отправки файла {format_name}: {str(e)}")
                    
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
        elif any(word in text for word in ['все источники', 'все', 'all sources']):
            await self.scrape_all_command(update, context)
        elif any(word in text for word in ['тест самокат', 'test samokat']):
            await self.test_samokat_command(update, context)
        elif any(word in text for word in ['тест лавка', 'test lavka']):
            await self.test_lavka_command(update, context)
        elif any(word in text for word in ['тест вкусвилл', 'test vkusvill']):
            await self.test_vkusvill_command(update, context)
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
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Инициализируем приложение
                await self.application.initialize()
                await self.application.start()
                
                # Запускаем updater с увеличенными таймаутами
                await self.application.updater.start_polling(
                    timeout=120,  # 120 секунд на получение обновлений
                    drop_pending_updates=True,  # Игнорируем старые сообщения
                    allowed_updates=["message", "callback_query"],  # Разрешаем только нужные типы обновлений
                    bootstrap_retries=3,  # Повторные попытки при запуске
                    read_timeout=120,  # Таймаут чтения
                    write_timeout=120   # Таймаут записи
                )
                
                self.logger.info("Telegram бот запущен и ожидает сообщения")
                
                # Держим бота запущенным
                try:
                    await asyncio.Event().wait()
                except KeyboardInterrupt:
                    self.logger.info("Получен сигнал остановки")
                break  # Если дошли сюда, значит все работает
                
            except Exception as e:
                retry_count += 1
                self.logger.error(f"Ошибка запуска бота (попытка {retry_count}/{max_retries}): {e}")
                
                if retry_count < max_retries:
                    self.logger.info(f"Повторная попытка через 5 секунд...")
                    await asyncio.sleep(5)
                else:
                    self.logger.error("Превышено максимальное количество попыток запуска")
                    raise
            finally:
                try:
                    await self.application.updater.stop()
                    await self.application.stop()
                    await self.application.shutdown()
                except:
                    pass
            
    def stop(self):
        """Остановка бота"""
        self.logger.info("Остановка Telegram бота...")
        asyncio.create_task(self.application.shutdown())

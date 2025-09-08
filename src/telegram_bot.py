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
        self.application.add_handler(CommandHandler("scrape_address", self.scrape_address_command))
        self.application.add_handler(CommandHandler("test_samokat", self.test_samokat_command))
        self.application.add_handler(CommandHandler("test_lavka", self.test_lavka_command))
        self.application.add_handler(CommandHandler("test_vkusvill", self.test_vkusvill_command))
        self.application.add_handler(CommandHandler("samokat", self.samokat_command))
        self.application.add_handler(CommandHandler("lavka", self.lavka_command))
        self.application.add_handler(CommandHandler("sources", self.sources_command))
        self.application.add_handler(CommandHandler("categories", self.categories_command))
        
        # Обработка callback кнопок
        self.application.add_handler(CallbackQueryHandler(self.button_callback))
        
        # Обработка текстовых сообщений
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text))
        
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /start"""
            
        welcome_text = """
🤖 Бот-парсер готовой еды

Доступные команды:
/start - Начать работу
/help - Справка по командам
/scrape_all - Запустить парсинг Самоката и Вкусвилла (рекомендуется)
/scrape_address - Парсинг по адресу доставки
/scrape - Запустить парсинг с настройками
/sources - Выбрать источники
/categories - Выбрать категории
/status - Статус текущих задач

Выберите действие или используйте /help для подробной справки.
        """
        
        keyboard = [
            [InlineKeyboardButton("🚀 Парсинг Самоката и Вкусвилла", callback_data="scrape_all")],
            [InlineKeyboardButton("📍 Парсинг по адресу", callback_data="scrape_address")],
            [InlineKeyboardButton("⚙️ Настройки парсинга", callback_data="scrape_menu")],
            [InlineKeyboardButton("🧪 Тестирование источников", callback_data="test_sources")],
            [InlineKeyboardButton("🔍 Отладка Самоката", callback_data="debug_samokat")],
            [InlineKeyboardButton("🔍 Отладка Лавки", callback_data="debug_lavka")],
            [InlineKeyboardButton("📊 Выбрать источники", callback_data="sources_menu")],
            [InlineKeyboardButton("🏷️ Выбрать категории", callback_data="categories_menu")],
            [InlineKeyboardButton("📈 Статус", callback_data="status")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(welcome_text, reply_markup=reply_markup, parse_mode='Markdown')
        
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /help"""
            
        help_text = """
📚 Справка по командам

Основные команды:
/start - Начать работу с ботом
/help - Показать эту справку
/status - Показать статус текущих задач

Парсинг:
/scrape_all - Запустить парсинг Самоката и Вкусвилла (рекомендуется)
/scrape - Запустить парсинг с текущими настройками
/sources - Выбрать источники для парсинга
/categories - Выбрать категории продуктов

Тестирование отдельных источников:
/test_samokat - Тестирование Самоката
/test_lavka - Тестирование Яндекс.Лавки
/test_vkusvill - Тестирование ВкусВилла

Отладка парсеров (с подробным логированием):
/samokat - Отладка парсера Самоката
/lavka - Отладка парсера Яндекс.Лавки

Примеры использования:
• /scrape_all - запуск парсинга Самоката и Вкусвилла (рекомендуется)
• /scrape - запуск парсинга с текущими настройками
• /test_samokat - быстрое тестирование Самоката
• /test_lavka - быстрое тестирование Лавки
• /test_vkusvill - быстрое тестирование ВкусВилла
• /sources samokat lavka - парсинг Самоката и Лавки
• /categories готовые блюда салаты - парсинг конкретных категорий

Поддерживаемые источники:
• samokat - Самокат
• lavka - Яндекс.Лавка  
• vkusvill - ВкусВилл

Форматы экспорта:
• CSV - таблица Excel
• JSONL - структурированные данные
• SQLite - база данных
        """
        
        await update.message.reply_text(help_text, parse_mode='Markdown')
        
    async def scrape_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /scrape"""
            
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
        """Обработка команды /scrape_all - парсинг Самоката и Вкусвилла для Москвы"""
            
        # Создаем конфигурацию для быстрого парсинга только Самоката и Вкусвилла
        scrape_config = self.config.copy()
        scrape_config['sources'] = ['samokat', 'vkusvill']  # Убираем Яндекс Лавку
        scrape_config['city'] = 'Москва'  # Принудительно Москва
        scrape_config['limit'] = 200  # Увеличиваем лимит для получения 1000-2000 товаров на сервис
        scrape_config['headless'] = True
        scrape_config['max_concurrent'] = 2  # Уменьшаем параллельность до 2 источников
        scrape_config['throttle_min'] = 0.1  # Минимальные задержки для максимальной скорости
        scrape_config['throttle_max'] = 0.3
        
        await update.message.reply_text("🚀 Запуск парсинга Самоката и Вкусвилла для Москвы\n\n" +
                                       "📊 Источники: Самокат, ВкусВилл\n" +
                                       "🏙️ Город: Москва\n" +
                                       "🏷️ Категории: 10+ категорий (автоопределение)\n" +
                                       "📦 Лимит: До 200 товаров на категорию\n" +
                                       "🎯 Цель: 1000-2000 товаров на сервис\n" +
                                       "⚡ Режим: Максимальная скорость\n" +
                                       "⏳ Время: Ожидайте, это займет 4-6 минут")
        
        # Запускаем парсинг
        await self._start_scraping(update, context, scrape_config)
        
    async def scrape_address_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /scrape_address - парсинг по адресу доставки"""
        # Парсим аргументы команды
        args = context.args if context.args else []
        
        if not args:
            await update.message.reply_text(
                "📍 Парсинг по адресу доставки\n\n"
                "Использование: /scrape_address <адрес>\n\n"
                "Примеры:\n"
                "• /scrape_address Москва, ул. Тверская, 1\n"
                "• /scrape_address Санкт-Петербург, Невский проспект, 28\n"
                "• /scrape_address Екатеринбург, ул. Ленина, 5\n\n"
                "Бот найдет товары, доступные для доставки по указанному адресу.",
                parse_mode='Markdown'
            )
            return
        
        # Объединяем все аргументы в один адрес
        address = " ".join(args)
        
        await update.message.reply_text(
            f"📍 Парсинг по адресу: {address}\n\n"
            "🔍 Ищу товары, доступные для доставки по указанному адресу...\n"
            "⏱️ Это может занять несколько минут.",
            parse_mode='Markdown'
        )
        
        # Создаем конфигурацию для парсинга по адресу
        scrape_config = self.config.copy()
        scrape_config['sources'] = ['samokat', 'lavka', 'vkusvill']
        scrape_config['limit'] = 150  # Увеличиваем лимит для получения большего количества товаров
        scrape_config['headless'] = True
        scrape_config['max_concurrent'] = 3  # Параллельный парсинг
        scrape_config['throttle_min'] = 0.1  # Минимальные задержки для максимальной скорости
        scrape_config['throttle_max'] = 0.3
        
        # Запускаем парсинг по адресу
        await self._start_scraping_by_address(update, context, scrape_config, address)
        
    async def sources_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /sources"""
            
        keyboard = [
            [InlineKeyboardButton("Самокат", callback_data="source_samokat")],
            [InlineKeyboardButton("Яндекс.Лавка", callback_data="source_lavka")],
            [InlineKeyboardButton("ВкусВилл", callback_data="source_vkusvill")],
            [InlineKeyboardButton("Все источники", callback_data="source_all")],
            [InlineKeyboardButton("Назад", callback_data="main_menu")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "🔍 Выберите источники для парсинга:\n\n"
            "• Самокат - готовые блюда и кулинария\n"
            "• Яндекс.Лавка - продукты и готовая еда\n"
            "• ВкусВилл - фермерские продукты и готовые блюда",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
    async def categories_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /categories"""
            
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
        elif data.startswith("debug_"):
            await self._handle_debug_source(query, context, data)
        elif data == "status":
            await self._show_status(query)
            
    async def _show_main_menu(self, query):
        """Показать главное меню"""
        keyboard = [
            [InlineKeyboardButton("🚀 Парсинг Самоката и Вкусвилла", callback_data="scrape_all")],
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
            
    async def _handle_debug_source(self, query, context, data):
        """Обработка отладочных команд"""
        source = data.replace("debug_", "")
        
        if source == "samokat":
            await self._start_debug_scraping(query, context, ['samokat'], "Самокат")
        elif source == "lavka":
            await self._start_debug_scraping(query, context, ['lavka'], "Яндекс.Лавка")
        else:
            await query.answer("❌ Неизвестный источник для отладки")
            
    async def _start_debug_scraping(self, query, context, sources, source_name):
        """Запуск отладочного парсинга"""
        await query.edit_message_text(f"🔍 *Отладка {source_name}...*\n\n" +
                                    f"🔍 *Источник:* {source_name}\n" +
                                    f"🏷️ *Категории:* Автоопределение\n" +
                                    f"📦 *Лимит:* 100 товаров (для отладки)\n" +
                                    f"🖥️ *Режим:* С отображением браузера\n" +
                                    f"📝 *Логирование:* Подробное\n" +
                                    f"⏳ *Время:* Ожидайте, это может занять 3-5 минут")
        
        # Создаем конфигурацию для отладки
        scrape_config = self.config.copy()
        scrape_config['sources'] = sources
        scrape_config['limit'] = 100
        scrape_config['headless'] = False
        scrape_config['max_concurrent'] = 1
        
        # Запускаем отладку
        await self._start_scraping_with_debug(query, context, scrape_config, source_name)
            
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
        """Запуск парсинга Самоката и Вкусвилла из меню"""
        await query.edit_message_text("🚀 *Запуск парсинга Самоката и Вкусвилла...*\n\n" +
                                    "📊 *Источники:* Самокат, ВкусВилл\n" +
                                    "🏷️ *Категории:* Автоопределение\n" +
                                    "📦 *Лимит:* До 500 товаров на категорию\n" +
                                    "⏳ *Время:* Ожидайте, это может занять 4-8 минут")
        
        # Создаем конфигурацию для Самоката и Вкусвилла с оптимизированными настройками
        scrape_config = self.config.copy()
        scrape_config['sources'] = ['samokat', 'vkusvill']  # Убираем Яндекс Лавку
        scrape_config['limit'] = 500  # Увеличиваем лимит для быстрого парсинга
        scrape_config['headless'] = True  # Скрытый браузер для ускорения
        scrape_config['max_concurrent'] = 2  # Уменьшаем параллельность до 2 источников
        
        # Запускаем парсинг
        await self._start_scraping(query, context, scrape_config)
        
    async def test_samokat_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /test_samokat - тестирование Самоката"""
            
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
        
    async def samokat_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /samokat - парсинг только Самоката с подробным логированием"""
        self.logger.info("Команда /samokat вызвана")
        
        scrape_config = self.config.copy()
        scrape_config['sources'] = ['samokat']
        scrape_config['limit'] = 100  # Ограничиваем для отладки
        scrape_config['headless'] = False  # Показываем браузер для отладки
        scrape_config['max_concurrent'] = 1  # Один поток для отладки
        
        await update.message.reply_text("🔍 *Парсинг Самоката с отладкой*\n\n" +
                                       "🔍 *Источник:* Самокат\n" +
                                       "🏷️ *Категории:* Автоопределение\n" +
                                       "📦 *Лимит:* 100 товаров (для отладки)\n" +
                                       "🖥️ *Режим:* С отображением браузера\n" +
                                       "📝 *Логирование:* Подробное\n" +
                                       "⏳ *Время:* Ожидайте, это может занять 3-5 минут")
        
        # Запускаем парсинг с подробным логированием
        await self._start_scraping_with_debug(update, context, scrape_config, "Самокат")
        
    async def lavka_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /lavka - парсинг только Яндекс Лавки с подробным логированием"""
        self.logger.info("Команда /lavka вызвана")
        
        scrape_config = self.config.copy()
        scrape_config['sources'] = ['lavka']
        scrape_config['limit'] = 100  # Ограничиваем для отладки
        scrape_config['headless'] = False  # Показываем браузер для отладки
        scrape_config['max_concurrent'] = 1  # Один поток для отладки
        
        await update.message.reply_text("🔍 *Парсинг Яндекс Лавки с отладкой*\n\n" +
                                       "🔍 *Источник:* Яндекс.Лавка\n" +
                                       "🏷️ *Категории:* Автоопределение\n" +
                                       "📦 *Лимит:* 100 товаров (для отладки)\n" +
                                       "🖥️ *Режим:* С отображением браузера\n" +
                                       "📝 *Логирование:* Подробное\n" +
                                       "⏳ *Время:* Ожидайте, это может занять 3-5 минут")
        
        # Запускаем парсинг с подробным логированием
        await self._start_scraping_with_debug(update, context, scrape_config, "Яндекс.Лавка")
        
    async def _start_scraping_with_debug(self, update, context, scrape_config, source_name):
        """Запуск парсинга с подробным логированием для отладки"""
        # Определяем тип update (обычное сообщение или callback)
        if hasattr(update, 'message'):
            user_id = update.effective_user.id
            message = update.message
        else:
            user_id = update.from_user.id
            message = update
        
        # Создаем задачу
        task_id = f"debug_{source_name.lower()}_{user_id}_{int(asyncio.get_event_loop().time())}"
        
        task_info = {
            'task_id': task_id,
            'start_time': asyncio.get_event_loop().time(),
            'sources': scrape_config.get('sources', [source_name.lower()]),
            'categories': scrape_config.get('categories', []),
            'status': f'Отладка {source_name}...',
            'progress': '0%',
            'debug_mode': True
        }
        
        self.active_tasks[user_id] = task_info
        
        # Запускаем парсинг в фоне
        asyncio.create_task(self._run_debug_scraping_task(update, context, scrape_config, task_info, source_name))
        
    async def _run_debug_scraping_task(self, update, context, scrape_config, task_info, source_name):
        """Выполнение задачи парсинга с подробным логированием"""
        # Определяем тип update (обычное сообщение или callback)
        if hasattr(update, 'message'):
            user_id = update.effective_user.id
            message = update.message
        else:
            user_id = update.from_user.id
            message = update
        
        try:
            self.logger.info(f"=== НАЧАЛО ОТЛАДКИ {source_name.upper()} ===")
            self.logger.info(f"Конфигурация для отладки: {scrape_config}")
            
            # Создаем скрейпер
            try:
                self.logger.info(f"Создаем FoodScraper с конфигом: {scrape_config}")
                scraper = FoodScraper(scrape_config)
                self.logger.info(f"Скрейпер {source_name} создан успешно")
                await message.reply_text(f"✅ *Скрейпер {source_name} создан успешно*")
            except Exception as e:
                error_msg = f"❌ *Ошибка создания скрейпера {source_name}:* {str(e)}"
                await message.reply_text(error_msg)
                self.logger.error(f"Ошибка создания скрейпера {source_name}: {e}")
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                return
                
            # Проверяем инициализацию скрейперов
            try:
                sources = list(scraper.scrapers.keys())
                self.logger.info(f"Доступные источники: {sources}")
                await message.reply_text(f"📊 *Доступные источники:* {', '.join(sources)}")
                
                # Проверяем, есть ли источник в списке (ищем по ключу, а не по названию)
                source_key = source_name.lower().replace('яндекс.лавка', 'lavka').replace('самокат', 'samokat')
                self.logger.info(f"Ищем источник '{source_name}' с ключом '{source_key}' в списке {sources}")
                if source_key not in sources:
                    error_msg = f"❌ *Источник {source_name} не найден в доступных источниках*"
                    await message.reply_text(error_msg)
                    self.logger.error(f"Источник {source_name} (ключ: {source_key}) не найден в {sources}")
                    return
                    
                scraper_instance = scraper.scrapers[source_key]
                if not scraper_instance:
                    error_msg = f"❌ *Скрейпер {source_name} не инициализирован*"
                    await message.reply_text(error_msg)
                    self.logger.error(f"Скрейпер {source_name} (ключ: {source_key}) не инициализирован")
                    return
                    
                self.logger.info(f"Скрейпер {source_name} инициализирован: {type(scraper_instance).__name__}")
                await message.reply_text(f"✅ *Скрейпер {source_name} инициализирован*")
                
            except Exception as e:
                error_msg = f"❌ *Ошибка проверки скрейпера {source_name}:* {str(e)}"
                await message.reply_text(error_msg)
                self.logger.error(f"Ошибка проверки скрейпера {source_name}: {e}")
                return
            
            # Получаем категории
            try:
                self.logger.info(f"Получаем категории для {source_name}")
                await message.reply_text(f"🔍 *Получение категорий для {source_name}...*")
                
                categories = await scraper_instance.get_categories()
                self.logger.info(f"Получены категории для {source_name}: {categories}")
                await message.reply_text(f"📋 *Категории {source_name}:*\n" + "\n".join([f"• {cat}" for cat in categories[:5]]))
                
                if not categories:
                    error_msg = f"❌ *Не удалось получить категории для {source_name}*"
                    await message.reply_text(error_msg)
                    self.logger.error(f"Не удалось получить категории для {source_name}")
                    return
                    
            except Exception as e:
                error_msg = f"❌ *Ошибка получения категорий {source_name}:* {str(e)}"
                await message.reply_text(error_msg)
                self.logger.error(f"Ошибка получения категорий {source_name}: {e}")
                return
            
            # Парсим первую категорию для тестирования
            try:
                test_category = categories[0]
                self.logger.info(f"Тестируем парсинг категории '{test_category}' для {source_name}")
                await message.reply_text(f"🧪 *Тестируем категорию:* {test_category}")
                
                # Инициализируем браузер для скрейпера
                self.logger.info(f"Инициализируем браузер для {source_name}")
                await message.reply_text(f"🌐 *Инициализация браузера для {source_name}...*")
                
                async with scraper_instance:
                    self.logger.info(f"Браузер инициализирован для {source_name}")
                    await message.reply_text(f"✅ *Браузер готов для {source_name}*")
                    
                    # Запускаем парсинг с подробным логированием
                    products = await self._debug_scrape_category(scraper_instance, test_category, message, source_name)
                
                if products:
                    self.logger.info(f"Успешно получено {len(products)} товаров из {source_name}")
                    await message.reply_text(f"✅ *Успешно получено {len(products)} товаров из {source_name}*")
                    
                    # Показываем первые 3 товара для проверки
                    sample_products = products[:3]
                    for i, product in enumerate(sample_products, 1):
                        product_info = f"📦 *Товар {i}:*\n" + \
                                     f"• Название: {product.name}\n" + \
                                     f"• Цена: {product.price} руб.\n" + \
                                     f"• Категория: {product.category}\n" + \
                                     f"• URL: {product.url[:50]}..." if product.url else "• URL: не указан"
                        await message.reply_text(product_info)
                    
                    # Сохраняем результаты в файл и отправляем
                    await self._save_and_send_debug_results(products, source_name, message, context)
                else:
                    error_msg = f"❌ *Не удалось получить товары из {source_name}*"
                    await message.reply_text(error_msg)
                    self.logger.error(f"Не удалось получить товары из {source_name}")
                    
            except Exception as e:
                error_msg = f"❌ *Ошибка парсинга {source_name}:* {str(e)}"
                await message.reply_text(error_msg)
                self.logger.error(f"Ошибка парсинга {source_name}: {e}")
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                
        except Exception as e:
            error_msg = f"❌ *Критическая ошибка в {source_name}:* {str(e)}"
            await message.reply_text(error_msg)
            self.logger.error(f"Критическая ошибка в {source_name}: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            
        finally:
            # Удаляем задачу из активных
            if user_id in self.active_tasks:
                del self.active_tasks[user_id]
            self.logger.info(f"=== КОНЕЦ ОТЛАДКИ {source_name.upper()} ===")
            
    async def _debug_scrape_category(self, scraper_instance, category, message, source_name):
        """Отладочный парсинг категории с подробным логированием"""
        try:
            self.logger.info(f"Начинаем отладочный парсинг категории '{category}' для {source_name}")
            
            # Браузер уже инициализирован в контекстном менеджере
            self.logger.info(f"Браузер готов для парсинга {source_name}")
            await message.reply_text(f"🔄 *Начинаем парсинг категории '{category}' для {source_name}...*")
            
            # Настраиваем локацию
            try:
                self.logger.info(f"Настраиваем локацию для {source_name}")
                await message.reply_text(f"📍 *Настройка локации для {source_name}...*")
                await scraper_instance.setup_location()
                self.logger.info(f"Локация настроена для {source_name}")
                await message.reply_text(f"✅ *Локация настроена для {source_name}*")
            except Exception as e:
                self.logger.warning(f"Ошибка настройки локации для {source_name}: {e}")
                await message.reply_text(f"⚠️ *Ошибка настройки локации для {source_name}:* {str(e)}")
            
            # Запускаем парсинг категории
            try:
                self.logger.info(f"Запускаем парсинг категории '{category}' для {source_name}")
                
                products = await scraper_instance.scrape_category(category, 10)  # Ограничиваем до 10 для отладки
                
                self.logger.info(f"Парсинг завершен для {source_name}. Получено товаров: {len(products)}")
                await message.reply_text(f"✅ *Парсинг завершен для {source_name}. Получено товаров: {len(products)}*")
                
                return products
                
            except Exception as e:
                self.logger.error(f"Ошибка парсинга категории '{category}' для {source_name}: {e}")
                await message.reply_text(f"❌ *Ошибка парсинга категории '{category}' для {source_name}:* {str(e)}")
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                return []
                
        except Exception as e:
            self.logger.error(f"Критическая ошибка в _debug_scrape_category для {source_name}: {e}")
            await message.reply_text(f"❌ *Критическая ошибка в парсинге {source_name}:* {str(e)}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return []
            
    async def _save_and_send_debug_results(self, products, source_name, message, context):
        """Сохранение и отправка результатов отладки"""
        try:
            import json
            import csv
            from datetime import datetime
            
            # Создаем имя файла с временной меткой
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            source_key = source_name.lower().replace('яндекс.лавка', 'lavka').replace('самокат', 'samokat')
            filename = f"debug_{source_key}_{timestamp}"
            
            # Сохраняем в JSON
            json_file = f"data/out/{filename}.json"
            os.makedirs(os.path.dirname(json_file), exist_ok=True)
            
            products_data = []
            for product in products:
                product_dict = {
                    'id': product.id,
                    'name': product.name,
                    'category': product.category,
                    'price': product.price,
                    'url': product.url,
                    'shop': product.shop,
                    'composition': product.composition,
                    'portion_g': product.portion_g,
                    'kcal_100g': product.kcal_100g,
                    'protein_100g': product.protein_100g,
                    'fat_100g': product.fat_100g,
                    'carb_100g': product.carb_100g
                }
                products_data.append(product_dict)
            
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(products_data, f, ensure_ascii=False, indent=2)
            
            # Сохраняем в CSV
            csv_file = f"data/out/{filename}.csv"
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                if products_data:
                    writer = csv.DictWriter(f, fieldnames=products_data[0].keys())
                    writer.writeheader()
                    writer.writerows(products_data)
            
            # Отправляем файлы
            await message.reply_text(f"📁 *Сохранение результатов отладки {source_name}...*")
            
            # Отправляем JSON файл
            try:
                with open(json_file, 'rb') as f:
                    await context.bot.send_document(
                        chat_id=message.chat.id,
                        document=f,
                        filename=f"{filename}.json",
                        caption=f"📊 *Результаты отладки {source_name} (JSON)*\n"
                               f"📦 Товаров: {len(products)}\n"
                               f"📅 Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
                    )
            except Exception as e:
                self.logger.error(f"Ошибка отправки JSON файла: {e}")
            
            # Отправляем CSV файл
            try:
                with open(csv_file, 'rb') as f:
                    await context.bot.send_document(
                        chat_id=message.chat.id,
                        document=f,
                        filename=f"{filename}.csv",
                        caption=f"📊 *Результаты отладки {source_name} (CSV)*\n"
                               f"📦 Товаров: {len(products)}\n"
                               f"📅 Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
                    )
            except Exception as e:
                self.logger.error(f"Ошибка отправки CSV файла: {e}")
            
            await message.reply_text(f"✅ *Файлы с результатами отладки {source_name} отправлены!*")
            
        except Exception as e:
            self.logger.error(f"Ошибка сохранения результатов отладки: {e}")
            await message.reply_text(f"❌ *Ошибка сохранения результатов:* {str(e)}")
        
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
        
    async def _start_scraping_by_address(self, update, context, scrape_config, address):
        """Запуск процесса парсинга по адресу"""
        user_id = update.effective_user.id if hasattr(update, 'effective_user') else update.from_user.id
        
        # Создаем задачу
        task_id = f"address_task_{user_id}_{int(asyncio.get_event_loop().time())}"
        
        task_info = {
            'task_id': task_id,
            'start_time': asyncio.get_event_loop().time(),
            'sources': scrape_config.get('sources', ['samokat']),
            'categories': scrape_config.get('categories', []),
            'status': f'Парсинг по адресу: {address}',
            'progress': '0%',
            'address': address
        }
        
        self.active_tasks[user_id] = task_info
        
        # Запускаем парсинг по адресу в фоне
        asyncio.create_task(self._run_scraping_by_address_task(update, context, scrape_config, task_info, address))
        
    async def _run_scraping_by_address_task(self, update, context, scrape_config, task_info, address):
        """Выполнение задачи парсинга по адресу"""
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
                
            # Запускаем парсинг по адресу
            result = await self._run_scraping_by_address_with_progress(scraper, message, task_info, address)
            
            if result:
                # Экспортируем данные
                export_files = await self._export_data(scraper, user_id)
                
                # Отправляем результаты
                await self._send_results(message, context, export_files, task_info)
                
            else:
                task_info['status'] = 'Ошибка'
                await message.reply_text("❌ *Парсинг по адресу завершен с ошибками*\n\nПроверьте логи для деталей")
                
        except Exception as e:
            task_info['status'] = f'Ошибка: {str(e)}'
            await message.reply_text(f"❌ *Критическая ошибка:*\n\n{str(e)}")
            self.logger.error(f"Ошибка в задаче парсинга по адресу: {e}")
            
        finally:
            # Удаляем задачу из активных
            if user_id in self.active_tasks:
                del self.active_tasks[user_id]
                
    async def _run_scraping_by_address_with_progress(self, scraper, message, task_info, address):
        """Запуск парсинга по адресу с прогрессом"""
        try:
            self.logger.info(f"_run_scraping_by_address_with_progress вызван для адреса: {address}")
            
            # Обновляем статус
            task_info['status'] = f'Парсинг по адресу: {address}'
            task_info['progress'] = '10%'
            
            # Запускаем парсинг по адресу
            all_products = await scraper.scrape_by_address(address)
            
            if not all_products:
                await message.reply_text(f"❌ *Не удалось найти товары для адреса:* {address}")
                return False
                
            # Сохраняем продукты
            total_saved = await scraper.save_products(all_products)
            
            if total_saved > 0:
                task_info['status'] = f'Найдено {total_saved} товаров для адреса: {address}'
                task_info['progress'] = '100%'
                return True
            else:
                await message.reply_text(f"❌ *Не удалось сохранить товары для адреса:* {address}")
                return False
                
        except Exception as e:
            self.logger.error(f"Ошибка в _run_scraping_by_address_with_progress: {e}")
            return False
        
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

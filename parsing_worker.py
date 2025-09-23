#!/usr/bin/env python3
"""
parsing_worker.py - Воркер для обработки задач парсинга из Redis очереди
Получает задачи от RationBot, выполняет парсинг и возвращает результаты.
"""
import asyncio
import json
import logging
import sys
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

# Добавляем текущую директорию в путь для импортов
sys.path.append(str(Path(__file__).parent))

# Импорты из существующего парсера
from address import VkusvillFastParser, AntiBotClient, get_location_from_address

# Redis клиент
import redis.asyncio as aioredis

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ParsingWorker:
    """Воркер для обработки задач парсинга через Redis."""

    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis = None
        self.parsing_queue = "parsing_queue"
        self.results_queue_prefix = "results:"
        self.antibot_client = None
        self.parser = None
        self.base_csv_path = Path("data/moscow_improved_1758362624.csv")
        self.base_df = None

        # Статистика
        self.stats = {
            "tasks_processed": 0,
            "tasks_success": 0,
            "tasks_error": 0,
            "total_time": 0,
            "start_time": datetime.now().isoformat()
        }

    async def connect(self):
        """Подключение к Redis и инициализация парсера."""
        try:
            self.redis = await aioredis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
            logger.info("✅ Подключено к Redis")

            # Инициализация парсера
            self.antibot_client = AntiBotClient(concurrency=10, timeout=30)
            self.parser = VkusvillFastParser(self.antibot_client)

            # Загрузка базовой таблицы
            if self.base_csv_path.exists():
                logger.info(f"📚 Загрузка базовой таблицы: {self.base_csv_path}")
                self.base_df = pd.read_csv(self.base_csv_path)
                logger.info(f"   Загружено {len(self.base_df)} продуктов")

                # Конвертируем в словарь для быстрого доступа
                self.parser.heavy_data = {}
                for _, row in self.base_df.iterrows():
                    self.parser.heavy_data[row['id']] = row.to_dict()
            else:
                logger.warning(f"⚠️ Базовая таблица не найдена: {self.base_csv_path}")

        except Exception as e:
            logger.error(f"❌ Ошибка подключения: {e}")
            raise

    async def disconnect(self):
        """Отключение от Redis."""
        if self.redis:
            await self.redis.close()
        if self.antibot_client:
            await self.antibot_client.close()

    async def send_heartbeat(self):
        """Отправка heartbeat для мониторинга."""
        while True:
            try:
                await self.redis.set(
                    "parser:heartbeat",
                    datetime.now().isoformat(),
                    ex=60  # TTL 60 секунд
                )

                # Обновляем статистику
                avg_time = (self.stats["total_time"] / self.stats["tasks_processed"]
                            if self.stats["tasks_processed"] > 0 else 0)

                stats_data = {
                    **self.stats,
                    "avg_time": avg_time,
                    "uptime": (datetime.now() - datetime.fromisoformat(self.stats["start_time"])).total_seconds()
                }

                await self.redis.set(
                    "parser:stats",
                    json.dumps(stats_data),
                    ex=3600  # TTL 1 час
                )

                await asyncio.sleep(30)  # Heartbeat каждые 30 секунд

            except Exception as e:
                logger.error(f"Ошибка heartbeat: {e}")
                await asyncio.sleep(30)

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Обработка одной задачи парсинга."""
        task_id = task.get("task_id")
        user_id = task.get("user_id")
        mode = task.get("mode", "fast")

        logger.info(f"📥 Обработка задачи {task_id} для пользователя {user_id}")

        start_time = time.time()

        try:
            # Проверяем, не отменена ли задача
            cancelled = await self.redis.sismember("cancelled_tasks", task_id)
            if cancelled:
                logger.info(f"🚫 Задача {task_id} была отменена")
                return None

            if mode == "full":
                # Полный парсинг
                result_df = await self.run_full_parsing()
            else:
                # Быстрый парсинг по геолокации
                result_df = await self.run_fast_parsing(task)

            # Формируем результат
            result = {
                "status": "success",
                "data": result_df.to_dict('records') if result_df is not None else [],
                "error_message": None
            }

            self.stats["tasks_success"] += 1
            logger.info(f"✅ Задача {task_id} выполнена успешно")

        except Exception as e:
            logger.error(f"❌ Ошибка обработки задачи {task_id}: {e}")

            result = {
                "status": "error",
                "data": None,
                "error_message": str(e)
            }

            self.stats["tasks_error"] += 1

        finally:
            # Обновляем статистику
            self.stats["tasks_processed"] += 1
            self.stats["total_time"] += time.time() - start_time

        return result

    async def run_fast_parsing(self, task: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """Быстрый парсинг по геолокации."""
        coordinates = task.get("coordinates", {})
        lat = coordinates.get("lat", 55.7558)
        lon = coordinates.get("lon", 37.6176)
        address = task.get("address", f"{lat},{lon}")

        logger.info(f"🗺️ Быстрый парсинг для: {address}")

        try:
            # Используем существующий метод быстрого парсинга
            city = "Москва"  # По умолчанию
            coords = f"{lat},{lon}"

            # Запускаем парсинг (получаем ID доступных продуктов)
            products = await self.parser.scrape_fast(
                city=city,
                coords=coords,
                address=address,
                limit=1500  # Максимум продуктов
            )

            if products:
                # Конвертируем в DataFrame
                df = pd.DataFrame(products)
                logger.info(f"   Найдено {len(df)} доступных продуктов")
                return df
            else:
                logger.warning("   Продукты не найдены")
                # Возвращаем базовый набор из основной таблицы
                if self.base_df is not None:
                    return self.base_df.head(100)
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Ошибка быстрого парсинга: {e}")
            # В случае ошибки возвращаем данные из базовой таблицы
            if self.base_df is not None:
                return self.base_df.head(100)
            raise

    async def run_full_parsing(self) -> Optional[pd.DataFrame]:
        """Полный парсинг всех продуктов."""
        logger.info("🔄 Запуск полного парсинга...")

        try:
            # Импортируем полный парсер
            from moscow_improved import VkusvillHeavyParser

            # Создаем новый парсер для полного сканирования
            heavy_parser = VkusvillHeavyParser(self.antibot_client)

            # Запускаем полный парсинг
            products = await heavy_parser.scrape_heavy(limit=1500)

            if products:
                # Сохраняем результаты
                df = pd.DataFrame(products)

                # Сохраняем в файл
                timestamp = int(time.time())
                new_csv_path = Path(f"data/moscow_improved_{timestamp}.csv")
                new_csv_path.parent.mkdir(exist_ok=True)

                df.to_csv(new_csv_path, index=False, encoding='utf-8')
                logger.info(f"💾 Сохранено в {new_csv_path}")

                # Обновляем базовую таблицу
                self.base_df = df
                self.base_csv_path = new_csv_path

                # Обновляем кэш парсера
                self.parser.heavy_data = {}
                for _, row in df.iterrows():
                    self.parser.heavy_data[row['id']] = row.to_dict()

                return df
            else:
                logger.warning("Полный парсинг не вернул результатов")
                return self.base_df

        except Exception as e:
            logger.error(f"Ошибка полного парсинга: {e}")
            raise

    async def run(self):
        """Основной цикл обработки задач."""
        logger.info("🚀 Воркер парсера запущен")

        # Запускаем heartbeat в фоне
        asyncio.create_task(self.send_heartbeat())

        while True:
            try:
                # Блокирующее чтение из очереди
                result = await self.redis.brpop(self.parsing_queue, timeout=5)

                if result:
                    _, task_json = result
                    task = json.loads(task_json)

                    # Обрабатываем задачу
                    result = await self.process_task(task)

                    if result:
                        # Сохраняем результат
                        task_id = task.get("task_id")
                        result_key = f"{self.results_queue_prefix}{task_id}"

                        await self.redis.set(
                            result_key,
                            json.dumps(result),
                            ex=300  # TTL 5 минут
                        )

                        logger.info(f"📤 Результат сохранен: {result_key}")

            except asyncio.TimeoutError:
                # Таймаут - нормально, продолжаем
                continue

            except Exception as e:
                logger.error(f"Ошибка в основном цикле: {e}")
                await asyncio.sleep(5)


async def main():
    """Точка входа воркера."""
    import os
    from dotenv import load_dotenv

    load_dotenv()

    # Получаем URL Redis из переменных окружения
    redis_url = os.getenv("REDIS_PUBLIC_URL", "redis://localhost:6379")

    logger.info("=" * 50)
    logger.info("🚀 VKUSVILL PARSER WORKER")
    logger.info(f"📍 Redis: {redis_url}")
    logger.info("=" * 50)

    worker = ParsingWorker(redis_url)

    try:
        await worker.connect()
        await worker.run()
    except KeyboardInterrupt:
        logger.info("⏹️ Остановка воркера...")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
    finally:
        await worker.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
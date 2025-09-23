#!/usr/bin/env python3
"""
api_server.py - API сервер для админских команд парсера
Предоставляет HTTP endpoints для управления парсером.
"""
import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Создание FastAPI приложения
app = FastAPI(
    title="VkusVill Parser API",
    description="API для управления парсером ВкусВилл",
    version="1.0.0"
)

# Redis клиент
redis_client = None


async def get_redis():
    """Получение Redis клиента."""
    global redis_client
    if not redis_client:
        redis_url = os.getenv("REDIS_PUBLIC_URL", "redis://localhost:6379")
        redis_client = await aioredis.from_url(
            redis_url,
            encoding="utf-8",
            decode_responses=True
        )
    return redis_client


@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске."""
    logger.info("🚀 API сервер запущен")
    await get_redis()


@app.on_event("shutdown")
async def shutdown_event():
    """Очистка при остановке."""
    global redis_client
    if redis_client:
        await redis_client.close()
    logger.info("🛑 API сервер остановлен")


@app.get("/")
async def root():
    """Корневой endpoint."""
    return {
        "service": "VkusVill Parser API",
        "status": "running",
        "endpoints": {
            "health": "/admin/health",
            "parse_full": "/admin/parse_full",
            "queue_status": "/admin/queue_status"
        }
    }


@app.get("/admin/health")
async def health_check():
    """Проверка здоровья сервиса."""
    try:
        redis = await get_redis()

        # Проверяем Redis
        await redis.ping()

        # Проверяем heartbeat воркера
        heartbeat = await redis.get("parser:heartbeat")
        worker_alive = False

        if heartbeat:
            last_beat = datetime.fromisoformat(heartbeat)
            time_diff = (datetime.now() - last_beat).total_seconds()
            worker_alive = time_diff < 60  # Считаем живым если heartbeat < 60 сек

        # Проверяем наличие базовой таблицы
        data_files = list(Path("data").glob("moscow_improved_*.csv"))
        has_data = len(data_files) > 0

        return {
            "status": "healthy" if (worker_alive and has_data) else "degraded",
            "timestamp": datetime.now().isoformat(),
            "components": {
                "redis": "connected",
                "worker": "alive" if worker_alive else "dead",
                "data": "available" if has_data else "missing"
            }
        }

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "error": str(e)
            }
        )


@app.post("/admin/parse_full")
async def trigger_full_parsing():
    """Запуск полного парсинга."""
    try:
        redis = await get_redis()

        # Создаем задачу на полный парсинг
        task = {
            "task_id": f"admin_full_{int(datetime.now().timestamp())}",
            "user_id": 0,
            "mode": "full",
            "admin_command": True,
            "timestamp": datetime.now().isoformat()
        }

        # Добавляем в очередь с высоким приоритетом
        await redis.rpush("parsing_queue", json.dumps(task))

        logger.info(f"🔄 Запущен полный парсинг: {task['task_id']}")

        return {
            "status": "success",
            "message": "Full parsing triggered",
            "task_id": task["task_id"]
        }

    except Exception as e:
        logger.error(f"Failed to trigger full parsing: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admin/queue_status")
async def queue_status():
    """Статус очереди задач."""
    try:
        redis = await get_redis()

        # Размер очереди
        queue_size = await redis.llen("parsing_queue")

        # Статистика воркера
        stats_json = await redis.get("parser:stats")
        stats = json.loads(stats_json) if stats_json else {}

        # Последние задачи (если есть история)
        recent_tasks = []

        return {
            "queue_size": queue_size,
            "worker_stats": stats,
            "recent_tasks": recent_tasks,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Failed to get queue status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/admin/clear_queue")
async def clear_queue():
    """Очистка очереди задач."""
    try:
        redis = await get_redis()

        # Получаем размер очереди
        queue_size = await redis.llen("parsing_queue")

        # Очищаем очередь
        await redis.delete("parsing_queue")

        logger.info(f"🧹 Очередь очищена: {queue_size} задач")

        return {
            "status": "success",
            "cleared": queue_size,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Failed to clear queue: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Запуск сервера
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("API_PORT", "8080"))

    uvicorn.run(
        "api_server:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info"
    )
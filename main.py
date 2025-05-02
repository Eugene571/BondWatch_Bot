# main.py
import asyncio
import io
import logging
import sys
import os
from datetime import datetime, time

from telegram.ext import Application, ContextTypes
from config import TELEGRAM_TOKEN
from bot.handlers import register_handlers
from database.db import init_db
from notification import check_and_notify_all
from bonds_get.nightly_sync import perform_nightly_sync

# Настройка кодировки и логирования
sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding='utf-8', errors='ignore')
sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding='utf-8', errors='ignore')
sys.stdout.reconfigure(encoding='utf-8')
os.environ["PYTHONIOENCODING"] = "utf-8"

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG,
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


async def main():
    # Инициализация БД
    logging.info("Initializing database...")
    await init_db()

    # Создание приложения бота
    logging.info("Starting bot...")
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Регистрация обработчиков
    register_handlers(app)
    app.add_error_handler(error_handler)
    await check_and_notify_all(app)
    # Запуск фоновых задач
    app.job_queue.run_daily(
        lambda ctx: asyncio.create_task(check_and_notify_all(ctx.application)),
        time(hour=9, minute=0)
    )
    app.job_queue.run_daily(
        lambda ctx: asyncio.create_task(perform_nightly_sync()),
        time(hour=14, minute=29)
    )
    # Запуск бота
    logging.info("Bot started...")
    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    # Бесконечный цикл
    while True:
        await asyncio.sleep(3600)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Exception while handling update:", exc_info=context.error)


if __name__ == "__main__":
    asyncio.run(main())

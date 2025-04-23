# main.py

import io
import logging
from telegram.ext import Application
from telegram.ext import ContextTypes
from config import TELEGRAM_TOKEN
from bot.handlers import register_handlers
from database.db import init_db
from apscheduler.schedulers.background import BackgroundScheduler
import sys
import os
sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding='utf-8', errors='ignore')
sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding='utf-8', errors='ignore')

sys.stdout.reconfigure(encoding='utf-8')
os.environ["PYTHONIOENCODING"] = "utf-8"

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,  # Можно оставить INFO вместо DEBUG, чтобы файл не забивался
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()  # <== Удалено: теперь не логирует в консоль
    ]
)


# Обработчик ошибок
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.error("Exception while handling update:", exc_info=context.error)


# Основная точка входа
def main():
    logging.info("Initializing database...")
    init_db()

    logging.info("Starting bot...")
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    register_handlers(app)
    app.add_error_handler(error_handler)

    # Настройка APScheduler с использованием job queue
    scheduler = BackgroundScheduler()

    # Добавляем задачу для проверки и уведомления о событиях
#    scheduler.add_job(check_and_notify, 'interval', seconds=30, args=[app.bot])

    # Добавляем задачу для обновления данных облигаций раз в сутки
#
    # Запускаем планировщик
    scheduler.start()

    logging.info("Bot started...")
    app.run_polling()  # теперь без asyncio.run()


if __name__ == "__main__":
    main()



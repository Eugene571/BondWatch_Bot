# main.py
import asyncio
import io
import logging
import sys
import os
from datetime import datetime, time, timedelta

from aiohttp import web
from sqlalchemy import select
from telegram.ext import Application, ContextTypes
from yookassa import Webhook, Payment, Configuration

from config import TELEGRAM_TOKEN
from bot.handlers import register_handlers
from database.db import init_db, get_session, Subscription
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
        logging.FileHandler("bot.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)  # Используйте sys.stdout вместо sys.stderr
    ]
)
logger = logging.getLogger(__name__)


async def yookassa_webhook(request: web.Request):
    event_json = await request.json()
    logger.info(f"Получено уведомление: {event_json}")  # Логирование входящих данных

    # Добавить проверку типа события
    if event_json['event'] != 'payment.succeeded':
        logger.warning(f"Игнорируем событие: {event_json['event']}")
        return web.Response(status=200)  # Игнорируем другие события

    try:
        signature = request.headers.get('X-Content-Signature')
        if not Webhook().verify(event_json, signature):
            logger.error("Неверная подпись вебхука!")
            return web.Response(status=403)
    except Exception as e:
        logger.error(f"Ошибка проверки подписи: {str(e)}")
        return web.Response(status=400)

        # Получение платежа
    try:
        payment_id = event_json['object']['id']
        payment = await Payment.find_one_async(payment_id)
        user_id = payment.metadata.get('user_id')
        plan = payment.metadata.get('plan', 'basic')
        logger.info(f"Обработка платежа {payment_id} для user_id={user_id}")
    except KeyError as e:
        logger.error(f"Отсутствуют ключи в данных: {str(e)}")
        return web.Response(status=400)

        # Обновление подписки
    async with get_session() as session:
        subscription = await session.scalar(
            select(Subscription)
            .where(Subscription.user_id == user_id)
        )
        if not subscription:
            logger.error(f"Подписка не найдена: user_id={user_id}")
            return web.Response(status=404)

        subscription.is_subscribed = True
        subscription.plan = plan
        subscription.subscription_end = datetime.now() + timedelta(days=30)
        subscription.pending_payment_id = None
        await session.commit()
        logger.info(f"Подписка обновлена: user_id={user_id}")

        # Отправка уведомления пользователю
    try:
        await request.app['bot'].send_message(
            chat_id=user_id,
            text=f"✅ Платеж подтвержден! Тариф «{plan}» активен до {subscription.subscription_end.strftime('%d.%m.%Y')}"
        )
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления: {str(e)}")

    return web.Response(status=200)


async def start_web(app: web.Application):
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    logging.info("Webhook server started on port 8080")


async def main():
    # Инициализация БД
    logging.info("Initializing database...")
    await init_db()

    # Создание приложения бота
    logging.info("Starting bot...")
    app_bot = Application.builder().token(TELEGRAM_TOKEN).build()

    # Настройка веб-приложения
    app_web = web.Application()
    Configuration.account_id = os.getenv("YOOKASSA_SHOP_ID")
    Configuration.secret_key = os.getenv("YOOKASSA_SECRET_KEY")

    # Регистрация обработчиков и ошибок
    register_handlers(app_bot)
    app_bot.add_error_handler(error_handler)

    app_web.add_routes([web.post('/yookassa-webhook', yookassa_webhook)])
    app_web['bot'] = app_bot.bot

    # Фоновые задачи (настройка job_queue)
    app_bot.job_queue.run_daily(
        lambda ctx: asyncio.create_task(check_and_notify_all(ctx.application)),
        time(hour=9, minute=0)
    )
    app_bot.job_queue.run_daily(
        lambda ctx: asyncio.create_task(perform_nightly_sync()),
        time(hour=0, minute=5)
    )

    # Инициализация и запуск бота
    await app_bot.initialize()
    await app_bot.start()

    # Запуск веб-сервера
    await start_web(app_web)

    try:
        # Бесконечный цикл для поддержания работы приложения
        await asyncio.gather(
            app_bot.updater.start_polling(drop_pending_updates=True),
            asyncio.sleep(float('inf'))  # Чтобы main не завершался
        )
    except asyncio.CancelledError:
        pass
    finally:
        # Корректное завершение работы
        await app_bot.stop()
        await app_bot.shutdown()


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Exception while handling update:", exc_info=context.error)


if __name__ == "__main__":
    asyncio.run(main())

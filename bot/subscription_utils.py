# bot/subscription_utils.py
import logging
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select, func
from telegram.ext import ContextTypes

from database.db import get_session
from database.db import UserTracking, Subscription


async def check_tracking_limit(user_id: int) -> bool:
    """
    Проверяет лимит отслеживаемых облигаций в зависимости от тарифа.
    Возвращает True, если пользователь может добавить новую облигацию.
    """
    async with get_session() as session:
        try:
            # Получаем подписку пользователя
            subscription = await session.scalar(
                select(Subscription).filter_by(user_id=user_id))

            # Считаем текущее количество облигаций
            tracking_count = await session.scalar(
                select(func.count(UserTracking.id))
                .where(UserTracking.user_id == user_id)
            )

            # Если подписки нет (например, новый пользователь)
            if not subscription:
                return tracking_count < 1  # Лимит free-тарифа

            # Определяем лимит в зависимости от тарифа
            match subscription.plan:
                case 'free':
                    return tracking_count < 1
                case 'basic':
                    return tracking_count < 10
                case 'optimal':
                    return tracking_count < 20
                case 'pro':
                    return True  # Без ограничений
                case _:
                    logging.error(f"Unknown subscription plan: {subscription.plan} for user {user_id}")
                    return False

        except Exception as e:
            logging.error(f"Error checking tracking limit: {str(e)}")
            return False


async def update_subscription_status(user_id: int):
    async with get_session() as session:
        subscription = await session.scalar(
            select(Subscription).where(Subscription.user_id == user_id)
        )

        if subscription and subscription.subscription_end < datetime.now():
            subscription.is_subscribed = False
            await session.commit()


async def check_subscriptions(context: ContextTypes.DEFAULT_TYPE):
    try:
        async with get_session() as session:
            # Получаем подписки с истекающим сроком
            result = await session.execute(
                select(Subscription)
                .where(
                    Subscription.is_subscribed == True,
                    Subscription.subscription_end < datetime.now()
                )
            )
            expired_subs = result.scalars().all()

            if not expired_subs:
                return

            for sub in expired_subs:
                sub.is_subscribed = False
                session.add(sub)

                try:
                    await context.bot.send_message(
                        chat_id=sub.user_id,
                        text="⚠️ Ваша подписка истекла! Для продления используйте /upgrade"
                    )
                except Exception as e:
                    logging.error(f"Sub expiration notify error [user={sub.user_id}]: {str(e)}")

            await session.commit()
            logging.info(f"Отключено {len(expired_subs)} просроченных подписок")

    except Exception as e:
        logging.critical(f"CRITICAL ERROR in subscription check: {str(e)}")


def setup_scheduler(dp):
    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_subscriptions, 'cron', hour=0, args=[dp])
    scheduler.start()
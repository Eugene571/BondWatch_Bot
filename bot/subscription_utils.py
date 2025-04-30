# bot/subscription_utils.py
from sqlalchemy import select, func
from database.db import get_session
from database.db import UserTracking, Subscription


async def check_tracking_limit(user_id: int) -> bool:
    """
    Проверяет лимит отслеживаемых облигаций в зависимости от тарифа.
    Возвращает True, если пользователь может добавить новую облигацию.
    """
    async with get_session() as session:
        # Получаем подписку пользователя
        subscription = await session.scalar(
            select(Subscription).filter_by(user_id=user_id)
        )

        # Считаем текущее количество облигаций
        tracking_count = await session.scalar(
            select(func.count(UserTracking.id))
            .where(UserTracking.user_id == user_id)
        )

        # Определяем лимит
        if subscription.plan == 'free':
            # Если подписки нет, доступен только демо-режим (1 облигация)
            return tracking_count < 1
        elif subscription.plan == "basic":
            return tracking_count < 10
        elif subscription.plan == "optimal":
            return tracking_count < 20
        elif subscription.plan == "pro":
            return True  # Без ограничений
        else:
            return False  # Некорректный тариф
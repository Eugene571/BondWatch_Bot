# notification.py
import logging
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import select

from config import TELEGRAM_TOKEN
from database.db import get_session, BondsDatabase, User, UserNotification, UserTracking
from telegram import Bot
from telegram.ext import Application
import asyncio


async def check_and_notify_all(app: Application):
    async with get_session() as session:
        today = datetime.utcnow().date()  # Используем UTC для единообразия
        logging.info(f"Starting check_and_notify_all for {today}")

        try:
            # Загрузка облигаций и пользователей
            bonds = await session.scalars(select(BondsDatabase))
            users = await session.scalars(select(User))

            for bond in bonds:
                logging.debug(f"Processing bond ISIN: {bond.isin}")

                # Проверка даты погашения
                if bond.maturity_date:
                    maturity_within_7_days = bond.maturity_date <= today + timedelta(days=7)
                    logging.debug(
                        f"Maturity check: {bond.maturity_date} <= {today + timedelta(days=7)} = {maturity_within_7_days}")

                    if maturity_within_7_days and bond.maturity_date > today:
                        logging.info(f"Bond {bond.isin} maturity within 7 days")
                        users = await session.scalars(select(User))
                        for user in users:
                            tracking_result = await session.execute(
                                select(UserTracking).filter_by(user_id=user.tg_id, isin=bond.isin)
                            )
                            user_tracking = tracking_result.scalar()
                            if user_tracking:
                                logging.info(f"Notifying user {user.tg_id} about maturity")
                                await notify_user_about_event(
                                    app=app,
                                    bond=bond,
                                    user=user,
                                    user_tracking=user_tracking,
                                    user_id=user.tg_id,
                                    event_type="maturity",
                                    event_date=bond.maturity_date
                                )

                # Проверка следующего купона (асинхронная версия)
                if bond.next_coupon_date:
                    coupon_check = bond.next_coupon_date == today + timedelta(days=1)
                    logging.debug(
                        f"Coupon check: {bond.next_coupon_date} == {today + timedelta(days=1)} = {coupon_check}")

                    if coupon_check:
                        users = await session.scalars(select(User))
                        for user in users:
                            tracking_result = await session.execute(
                                select(UserTracking).filter_by(user_id=user.tg_id, isin=bond.isin)
                            )
                            user_tracking = tracking_result.scalar()
                            if user_tracking:
                                logging.info(f"Notifying user {user.tg_id} about coupon")
                                await notify_user_about_event(
                                    app=app,
                                    bond=bond,
                                    user=user,
                                    user_tracking=user_tracking,
                                    user_id=user.tg_id,
                                    event_type="coupon",
                                    event_date=bond.next_coupon_date

                                )

                # Проверка амортизации (асинхронная версия)
                if bond.amortization_date:
                    amortization_check = bond.amortization_date == today + timedelta(days=1)
                    logging.debug(
                        f"Amortization check: {bond.amortization_date} == {today + timedelta(days=1)} = {amortization_check}")

                    if amortization_check:
                        users = await session.scalars(select(User))
                        for user in users:
                            tracking_result = await session.execute(
                                select(UserTracking).filter_by(user_id=user.tg_id, isin=bond.isin)
                            )
                            user_tracking = tracking_result.scalar()
                            if user_tracking:
                                logging.info(f"Notifying user {user.tg_id} about amortization")
                                await notify_user_about_event(
                                    app=app,
                                    bond=bond,
                                    user=user,
                                    user_tracking=user_tracking,
                                    user_id=user.tg_id,
                                    event_type="amortization",
                                    event_date=bond.amortization_date,
                                )
                # Обработка оферты (offer)
                if bond.offer_date:
                    days_left = (bond.offer_date - today).days
                    logging.debug(f"Offer check: {bond.isin} days_left={days_left}")
                    if 1 <= days_left <= 14:
                        logging.debug(f"Processing offer for {bond.isin}, days left: {days_left}")
                        users = await session.scalars(select(User))
                        for user in users:
                            logging.debug(f"Checking user_tracking for user {user.tg_id} and bond {bond.isin}")
                            tracking = await session.execute(
                                select(UserTracking).filter_by(user_id=user.tg_id, isin=bond.isin)
                            )
                            user_tracking = tracking.scalar()
                            if user_tracking:
                                logging.debug(f"User {user.tg_id} is tracking {bond.isin}")
                                await notify_user_about_event(
                                    app=app,
                                    bond=bond,
                                    user=user,
                                    user_tracking=user_tracking,
                                    user_id=user.tg_id,
                                    event_type="offer",
                                    event_date=bond.offer_date,
                                    days_left=days_left,
                                )
        except Exception as e:
            logging.error(f"Critical error in check_and_notify_all: {e}", exc_info=True)


async def manual_send_notifications(app: Application):
    with get_session() as session:
        users = session.query(User).all()  # Получаем всех пользователей
        today = datetime.now().date()

        for user in users:
            # Логика проверки событий для каждого пользователя
            for bond in session.query(BondsDatabase).all():
                user_tracking = session.query(UserTracking).filter(
                    UserTracking.user_id == user.tg_id,
                    UserTracking.isin == bond.isin
                ).first()
                if user_tracking:
                    # Проверка даты погашения, купонов и амортизации
                    if bond.maturity_date and bond.maturity_date <= today + timedelta(days=7):
                        await notify_user_about_event(
                            app=app,
                            bond=bond,  # Объект облигации
                            user=user,  # Объект пользователя
                            user_tracking=user_tracking,  # Объект UserTracking
                            user_id=user.tg_id,  # ID пользователя
                            event_type="maturity",
                            event_date=bond.maturity_date,
                        )
                    if bond.next_coupon_date and bond.next_coupon_date == today + timedelta(days=1):
                        await notify_user_about_event(
                            app=app,
                            bond=bond,  # Объект облигации
                            user=user,  # Объект пользователя
                            user_tracking=user_tracking,  # Объект UserTracking
                            user_id=user.tg_id,  # ID пользователя
                            event_type="coupon",
                            event_date=bond.maturity_date,
                        )
                    if bond.amortization_date and bond.amortization_date == today + timedelta(days=1):
                        await notify_user_about_event(
                            app=app,
                            bond=bond,  # Объект облигации
                            user=user,  # Объект пользователя
                            user_tracking=user_tracking,  # Объект UserTracking
                            user_id=user.tg_id,  # ID пользователя
                            event_type="amortization",
                            event_date=bond.maturity_date,
                        )


async def async_send_notification(context):
    user_id = context.job.data['user_id']
    message = context.job.data['message']
    if user_id is None or message is None:
        logging.error(f"Invalid data for notification: user_id={user_id}, message={message}")
        return
    bot: Bot = context.bot
    try:
        await bot.send_message(chat_id=user_id, text=message)
        logging.info(f"Notification sent to user {user_id}: {message}")
        await asyncio.sleep(0.05)  # Небольшая задержка для соблюдения rate limits
    except Exception as e:
        logging.exception(f"Error sending notification to user {user_id}: {e}")


async def notify_user_about_event(
        app: Application,
        bond: BondsDatabase,
        user: User,
        user_tracking: UserTracking,
        user_id: int,
        event_type: str,
        event_date: datetime,
        days_left: Optional[int] = None,
):
    try:
        async with get_session() as session:
            bond_isin = bond.isin
            logging.debug(f"Attempting to notify user {user_id} about {event_type}")
            stmt = select(UserNotification).where(
                UserNotification.user_id == user_id,
                UserNotification.bond_isin == bond_isin,  # <-- Важно!
                UserNotification.event_type == event_type,
                UserNotification.event_date == event_date
            )
            result = await session.execute(stmt)
            notification = result.scalar_one_or_none()

            if not notification:
                message = ""
                quantity = user_tracking.quantity if user_tracking else 0

                # Формирование сообщения с учетом типа события
                if event_type == "coupon":
                    coupon_value = bond.next_coupon_value or 0
                    total = coupon_value * quantity
                    message = (
                        f"Привет! {user.full_name}, по вашей облигации {bond.name} (ISIN: {bond_isin})\n"
                        f"📅 Выплата купона {event_date.strftime('%d.%m.%Y')}\n"
                        f"💰 Сумма к получению: {total:.2f} руб."
                    )

                elif event_type == "maturity":
                    message = (
                        f"Привет! {user.full_name}, облигация {bond.name} (ISIN: {bond_isin})\n"
                        f"🏁 Погашение {event_date.strftime('%d.%m.%Y')}\n"
                        "Рекомендуем подготовиться к получению номинала."
                    )

                elif event_type == "amortization":
                    amort_value = bond.amortization_value or 0
                    total = amort_value * quantity
                    message = (
                        f"Привет! {user.full_name}, по вашей облигации {bond.name} (ISIN: {bond_isin})\n"
                        f"📉 Частичное погашение {event_date.strftime('%d.%m.%Y')}\n"
                        f"💰 Сумма к получению: {total:.2f} руб."
                    )

                elif event_type == "offer":
                    logging.debug(f"Forming offer message. Days left: {days_left}")
                    # Добавьте проверку days_left
                    if days_left is None:
                        logging.error("Days_left is None for offer event!")
                        return

                    def get_days_word(d: int) -> str:
                        # Добавьте логирование
                        try:
                            if 11 <= d <= 14:
                                return "дней"
                            last = d % 10
                            return {1: "день", 2: "дня", 3: "дня", 4: "дня"}.get(last, "дней")
                        except Exception as e:
                            logging.error(f"Error in get_days_word: {e}")
                            return "дней"

                    days_word = get_days_word(days_left) if days_left else "дней"
                    message = (
                        f"Привет! {user.full_name}, по вашей облигации {bond.name} (ISIN: {bond_isin})\n"
                        f"⏳ До оферты осталось {days_left} {days_word} ({event_date.strftime('%d.%m.%Y')})\n\n"
                        "⚠️ Важные заметки:\n"
                        "• Сроки подачи заявок отличаются у разных брокеров\n"
                        "• Проверьте условия оферты в официальных документах\n"
                        "• Уточните дедлайн у вашего брокера заранее"
                    )
                    logging.debug(f"Message for offer: {message}")
                # Отправка сообщения через JobQueue
                if message:
                    app.job_queue.run_once(
                        async_send_notification,
                        when=0,
                        data={'user_id': user_id, 'message': message}

                    )
                    logging.debug(f"Scheduled job for user {user_id}")
                # Сохранение уведомления в БД
                new_notification = UserNotification(
                    user_id=user_id,
                    bond_isin=bond_isin,
                    event_type=event_type,
                    event_date=event_date,
                    is_sent=True,
                    sent_at=datetime.utcnow(),
                    days_left=days_left
                )
                session.add(new_notification)
                await session.commit()
                logging.info(f"Уведомление для {user_id} ({event_type}) запланировано")

            else:
                logging.info(f"Уведомление уже существует: {user_id} {bond_isin} {event_type}")

    except Exception as e:
        logging.error(f"Ошибка в notify_user_about_event: {e}", exc_info=True)
        if 'session' in locals():
            await session.rollback()

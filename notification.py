# notification.py
import logging
from datetime import datetime, timedelta

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
                                    app, user.tg_id, bond.isin, "maturity",
                                    bond.maturity_date, bond, user, user_tracking
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
                                    app, user.tg_id, bond.isin, "coupon",
                                    bond.next_coupon_date, bond, user, user_tracking
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
                                    app, user.tg_id, bond.isin, "amortization",
                                    bond.amortization_date, bond, user, user_tracking
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
                            app, user.tg_id, bond.isin, "maturity", bond.maturity_date,
                            bond, user, user_tracking
                        )
                    if bond.next_coupon_date and bond.next_coupon_date == today + timedelta(days=1):
                        await notify_user_about_event(
                            app, user.tg_id, bond.isin, "coupon", bond.next_coupon_date,
                            bond, user, user_tracking
                        )
                    if bond.amortization_date and bond.amortization_date == today + timedelta(days=1):
                        await notify_user_about_event(
                            app, user.tg_id, bond.isin, "amortization", bond.amortization_date,
                            bond, user, user_tracking
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


async def notify_user_about_event(app: Application, user_id: int, bond_isin: str, event_type: str,
                                  event_date: datetime, bond, user, user_tracking):
    try:
        async with get_session() as session:
            logging.debug(f"Attempting to notify user {user_id} about {event_type}")

            # Асинхронный запрос вместо session.query()
            stmt = select(UserNotification).where(
                UserNotification.user_id == user_id,
                UserNotification.bond_isin == bond_isin,
                UserNotification.event_type == event_type,
                UserNotification.event_date == event_date
            )
            result = await session.execute(stmt)
            notification = result.scalar_one_or_none()

            if not notification:
                quantity = user_tracking.quantity
                message = ""

                # Формирование сообщения (оставляем оригинальную логику)
                if event_type == "coupon":
                    total_coupon_value = bond.next_coupon_value * quantity if bond.next_coupon_value else 0
                    message = f"Привет! {user.full_name}, по вашей облигации {bond.name} (ISIN: {bond_isin}) выплата купона {event_date.strftime('%d.%m.%Y')}. Сумма к получению: {total_coupon_value:.2f} руб."
                elif event_type == "maturity":
                    message = f"Привет! {user.full_name}, облигация {bond.name} (ISIN: {bond_isin}) погашается {event_date.strftime('%d.%m.%Y')}. Пожалуйста, учтите это."
                elif event_type == "amortization":
                    total_amortization_value = bond.amortization_value * quantity if bond.amortization_value else 0
                    message = f"Привет! {user.full_name}, по вашей облигации {bond.name} (ISIN: {bond_isin}) частичное погашение (амортизация) {event_date.strftime('%d.%m.%Y')}. Сумма к получению: {total_amortization_value:.2f} руб."

                # Планируем отправку (сохраняем оригинальный подход)
                job_data = {'user_id': user_id, 'message': message}
                app.job_queue.run_once(async_send_notification, when=0, data=job_data)

                # Асинхронное сохранение уведомления
                new_notification = UserNotification(
                    user_id=user_id,  # Используем параметр вместо user.tg_id
                    bond_isin=bond_isin,
                    event_type=event_type,
                    event_date=event_date,
                    is_sent=True,
                    sent_at=datetime.utcnow()
                )
                session.add(new_notification)
                await session.commit()  # Добавляем await

                logging.info(f"Notification scheduled for user {user_id}, bond {bond_isin}, event {event_type}")
            else:
                logging.info(f"Notification already exists for user {user_id}, bond {bond_isin}, event {event_type}")

    except Exception as e:
        logging.exception(f"Error in notify_user_about_event: {e}")
        if 'session' in locals():
            await session.rollback()  # Асинхронный откат
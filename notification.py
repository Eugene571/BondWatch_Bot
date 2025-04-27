import logging
from datetime import datetime, time, timedelta

from config import TELEGRAM_TOKEN
from database.db import get_session, BondsDatabase, User, UserNotification, UserTracking
from telegram import Bot
from telegram.ext import Application, CallbackContext
import asyncio

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Функция для асинхронной отправки уведомлений (для JobQueue)
async def async_send_notification(context: CallbackContext):
    user_id = context.job.data['user_id']
    message = context.job.data['message']
    bot: Bot = context.bot
    try:
        await bot.send_message(chat_id=user_id, text=message)
        logger.info(f"Notification sent to user {user_id}: {message}")
        await asyncio.sleep(0.05)  # Небольшая задержка для соблюдения rate limits
    except Exception as e:
        logger.error(f"Error sending notification to user {user_id}: {e}")


# Функция для проверки и планирования отправки уведомлений через JobQueue
async def notify_user_about_event(app: Application, user_id: int, bond_isin: str, event_type: str,
                                  event_date: datetime, bond, user, user_tracking):
    try:
        # Проверяем, есть ли уведомление для этого события
        with get_session() as session:
            notification = session.query(UserNotification).filter(
                UserNotification.user_id == user_id,
                UserNotification.bond_isin == bond_isin,
                UserNotification.event_type == event_type,
                UserNotification.event_date == event_date
            ).first()

            if not notification:  # Если уведомление еще не отправлялось
                quantity = user_tracking.quantity

                # Создаем сообщение для пользователя
                if event_type == "coupon":
                    total_coupon_value = bond.next_coupon_value * quantity if bond.next_coupon_value else 0
                    message = f"Привет! {user.full_name}, по вашей облигации {bond.name} (ISIN: {bond_isin}) выплата купона {event_date.strftime('%d.%m.%Y')}. Сумма к получению: {total_coupon_value:.2f} руб."
                elif event_type == "maturity":
                    message = f"Привет! {user.full_name}, облигация {bond.name} (ISIN: {bond_isin}) погашается {event_date.strftime('%d.%m.%Y')}. Пожалуйста, учтите это."
                elif event_type == "amortization":
                    total_amortization_value = bond.amortization_value * quantity if bond.amortization_value else 0
                    message = f"Привет! {user.full_name}, по вашей облигации {bond.name} (ISIN: {bond_isin}) частичное погашение (амортизация) {event_date.strftime('%d.%m.%Y')}. Сумма к получению: {total_amortization_value:.2f} руб."

                # Планируем отправку уведомления через JobQueue
                job_data = {'user_id': user_id, 'message': message}
                await app.job_queue.run_once(async_send_notification, when=0, data=job_data)

                # Добавляем запись об уведомлении в базу данных
                new_notification = UserNotification(
                    user_id=user_id,
                    bond_isin=bond_isin,
                    event_type=event_type,
                    event_date=event_date,
                    is_sent=True,
                    sent_at=datetime.utcnow()
                )
                session.add(new_notification)
                session.commit()
                logger.info(f"Notification scheduled for user {user_id}, bond {bond_isin}, event {event_type}")

            else:
                logger.info(
                    f"Notification already scheduled/sent for user {user_id}, bond {bond_isin}, event {event_type}")

    except Exception as e:
        logger.error(f"Error in notify_user_about_event: {e}")


async def check_and_notify_all(app: Application):
    # Используем контекстный менеджер для работы с сессией
    with get_session() as session:
        today = datetime.now().date()
        try:
            # Получаем все облигации и пользователей
            bonds = session.query(BondsDatabase).all()
            users = session.query(User).all()

            # Обрабатываем все облигации и пользователей
            for bond in bonds:
                # Проверяем дату погашения, если облигация недавно добавлена (в течение 7 дней до погашения)
                if bond.maturity_date and bond.maturity_date <= today + timedelta(days=7):
                    # Проверяем, была ли облигация добавлена пользователем недавно (например, в течение последних 7 дней)
                    for user in users:
                        user_tracking = session.query(UserTracking).filter(
                            UserTracking.user_id == user.id,
                            UserTracking.isin == bond.isin
                        ).first()
                        if user_tracking:
                            # Если дата погашения в пределах 7 дней, уведомляем пользователя
                            if bond.maturity_date > today:
                                await notify_user_about_event(
                                    app, user.tg_id, bond.isin, "maturity", bond.maturity_date,
                                    bond, user, user_tracking
                                )

                # Проверяем дату следующего купона
                if bond.next_coupon_date and bond.next_coupon_date == today + timedelta(days=1):
                    for user in users:
                        user_tracking = session.query(UserTracking).filter(
                            UserTracking.user_id == user.id,
                            UserTracking.isin == bond.isin
                        ).first()
                        if user_tracking:
                            await notify_user_about_event(
                                app, user.tg_id, bond.isin, "coupon", bond.next_coupon_date,
                                bond, user, user_tracking
                            )

                # Проверяем дату амортизации
                if bond.amortization_date and bond.amortization_date == today + timedelta(days=1):
                    for user in users:
                        user_tracking = session.query(UserTracking).filter(
                            UserTracking.user_id == user.id,
                            UserTracking.isin == bond.isin
                        ).first()
                        if user_tracking:
                            await notify_user_about_event(
                                app, user.tg_id, bond.isin, "amortization", bond.amortization_date,
                                bond, user, user_tracking
                            )

        except Exception as e:
            logger.error(f"Error in check_and_notify_all: {e}")


async def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    await check_and_notify_all(app)
    # Регулярная проверка уведомлений по событиям каждый день в 10:00 и 18:00
    app.job_queue.run_daily(check_and_notify_all, time(hour=10, minute=0))
    app.job_queue.run_daily(check_and_notify_all, time(hour=18, minute=0))

    await app.run_polling()


if __name__ == '__main__':
    asyncio.run(main())

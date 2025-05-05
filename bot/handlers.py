# bot.handlers.py
import html
import sys

from sqlalchemy import select, func
from sqlalchemy.orm import selectinload
from telegram import Update, Message
from telegram.ext import (
    CommandHandler,
    Application,
    ContextTypes,
    filters,
    MessageHandler,
    ConversationHandler,
    CallbackQueryHandler
)
from telegram.constants import ParseMode
import re
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import logging

from bonds_get.bond_update import get_next_coupon
from bonds_get.moex_name_lookup import get_bond_name_from_moex
from bot.subscription_utils import check_tracking_limit
from database.db import get_session, User, BondsDatabase, UserTracking, Subscription

ISIN_PATTERN = re.compile(r'^[A-Z]{2}[A-Z0-9]{9}\d$')

AWAITING_ISIN_TO_REMOVE = 1
AWAITING_ISIN_TO_ADD = 2
AWAITING_QUANTITY = 3
AWAITING_ISIN_TO_CHANGE = 4
AWAITING_SUPPORT_MESSAGE = 5


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    async with get_session() as session:
        # Асинхронный запрос к БД
        result = await session.execute(select(User).filter_by(tg_id=user.id))
        db_user = result.scalar()
        if not db_user:
            db_user = User(tg_id=user.id, full_name=user.full_name)
            session.add(db_user)
            await session.commit()
            subscription = Subscription(user_id=user.id, plan="free")
            session.add(subscription)
            await session.commit()
        context.bot_data.get("logger", print)(f"✅ Новый пользователь: {user.full_name} ({user.id})")

    await update.message.reply_text(
        f"👋 Привет, {user.first_name}!\n\n"
        "Я BondWatch — бот, который следит за событиями по вашим облигациям.\n\n"
        "📎 Введите /help для ознакомлением со списком команд и функционалом\n\n"
        "📎 Вы можете бесплатно отслеживать 1 бумагу\n\n"
        "📎 Для управлением тарифным планом используйте /upgrade\n\n"
        "🔔 Начнём!"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
📚 <b>Список доступных команд</b>

🔹 <b>Основные команды</b>
/start - Начать работу  
/help - Показать это сообщение  
/list - Список отслеживаемых облигаций  
/events - Ближайшие события  
/upgrade - Управление подпиской  
/support - Обратиться в поддержку (разрешена отправка изображений)

🔹 <b>Управление облигациями</b>  
/add - Добавить облигацию  
/remove - Удалить облигацию  
/change_quantity - Изменить количество бумаг  

🔹 <b>Особенности работы</b>  
📌 Бот работает только с <b>российскими облигациями</b>  
📌 Обрабатываемые события:  
 • Купоны  
 • Амортизации  
 • Погашения  
 • Оферты  

🔔 <b>Уведомления:</b>  
⏰ Отправляются в 12:00 по МСК:  
 • Купоны/амортизации - за 1 день до события  
 • Погашения - за 7 дней  
 • Оферты - за 14 дней  
 • Уведомления отправляются единоразово

⚠️ <b>Важно:</b>  
Для участия в оферте необходимо заранее уточнять порядок действий у вашего брокера. Бот только информирует о дате оферты.  

📌 <b>Как добавить облигацию?</b>  
1. Найти ISIN на <a href="https://www.moex.com/">MOEX</a>  
2. Отправить: <code>/add RU000A10AV15</code>  
3. Указать количество бумаг  

💎 <b>Тарифы:</b>  
🔸 Бесплатно: 1 облигация  
🔸 Платно: 10/20/∞ бумаг  

❓ <b>Проблемы?</b> Напишите /support  
    """
    await update.message.reply_text(
        help_text,
        parse_mode="HTML",
        disable_web_page_preview=True,
        disable_notification=True
    )


async def support_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрашивает текст обращения в поддержку."""
    await update.message.reply_text(
        "✍️ Напишите свой вопрос или проблему. Можно:\n"
        "• Написать текст\n"
        "• Отправить скриншот (как фото)\n\n"
        "❌ Чтобы отменить, отправь /cancel",
        parse_mode=ParseMode.HTML
    )
    return AWAITING_SUPPORT_MESSAGE


async def process_support_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает медиафайлы (фото/документы) для поддержки"""
    user = update.effective_user
    caption = update.message.caption or "Без описания"

    support_text = (
        f"🆘 <b>Новое обращение с медиафайлом</b>\n\n"
        f"👤 Пользователь: {user.full_name} (@{user.username or 'нет'}, ID: {user.id})\n"
        f"📝 Описание: {caption}"
    )

    try:
        # Отправляем админу (ваш tg_id: 247176848)
        if update.message.photo:
            photo_file = await update.message.photo[-1].get_file()
            await context.bot.send_photo(
                chat_id=247176848,
                photo=photo_file.file_id,
                caption=support_text,
                parse_mode=ParseMode.HTML
            )
        elif update.message.document:
            doc_file = await update.message.document.get_file()
            await context.bot.send_document(
                chat_id=247176848,
                document=doc_file.file_id,
                caption=support_text,
                parse_mode=ParseMode.HTML
            )

        await update.message.reply_text("✅ Ваше обращение с медиафайлом отправлено в поддержку!")
    except Exception as e:
        logging.error(f"Ошибка отправки медиа в поддержку: {e}")
        await update.message.reply_text("❌ Не удалось отправить медиафайл. Попробуйте позже.")

    return ConversationHandler.END


async def forward_text(user: User, text: str, context: ContextTypes.DEFAULT_TYPE):
    support_text = (
        f"🆘 <b>Новое обращение</b>\n"
        f"👤 {user.mention_html()}\n"
        f"🆔 ID: {user.id}\n\n"
        f"📝 {html.escape(text)}"
    )
    await context.bot.send_message(
        chat_id=247176848,
        text=support_text,
        parse_mode=ParseMode.HTML
    )


async def forward_media(user: User, message: Message, caption: str, context: ContextTypes.DEFAULT_TYPE):
    support_text = (
        f"🆘 <b>Медиа от {user.mention_html()}</b>\n"
        f"🆔 ID: {user.id}\n"
        f"📝 {html.escape(caption)}"
    )

    if message.photo:
        await context.bot.send_photo(
            chat_id=247176848,
            photo=message.photo[-1].file_id,
            caption=support_text,
            parse_mode=ParseMode.HTML
        )
    elif message.document:
        await context.bot.send_document(
            chat_id=247176848,
            document=message.document.file_id,
            caption=support_text,
            parse_mode=ParseMode.HTML
        )


async def process_support_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    try:
        # Для сообщений с фото/документом и подписью
        if update.message.caption or update.message.photo or update.message.document:
            caption = update.message.caption or "Без описания"
            await forward_media(user, update.message, caption, context)

        # Для текстовых сообщений без медиа
        elif update.message.text:
            await forward_text(user, update.message.text, context)

        await update.message.reply_text("✅ Ваше обращение принято!")

    except Exception as e:
        logging.error(f"Ошибка: {str(e)}", exc_info=True)
        await update.message.reply_text("❌ Ошибка при отправке")

    return ConversationHandler.END


async def cancel_support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отменяет диалог с поддержкой."""
    await update.message.reply_text("❌ Отправка сообщения в поддержку отменена.")
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Операция отменена.")
    return ConversationHandler.END


async def list_tracked_bonds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with get_session() as session:
        result = await session.execute(
            select(User)
            .options(selectinload(User.tracked_bonds))
            .filter_by(tg_id=update.effective_user.id)
        )
        user = result.scalar()

        if not user or not user.tracked_bonds:
            await update.message.reply_text("❗️Вы пока не отслеживаете ни одной облигации.")
            return

        text = "📋 Вот список ваших отслеживаемых бумаг:\n\n"
        for ut in user.tracked_bonds:
            bond_result = await session.execute(select(BondsDatabase).filter_by(isin=ut.isin))
            bond = bond_result.scalar()

            name = bond.name or bond.isin
            if not bond.name:
                moex_name = await get_bond_name_from_moex(bond.isin)
                if moex_name:
                    bond.name = moex_name
                    await session.commit()
                    name = moex_name

            text += f"• {name} - {ut.quantity} бумаг \n({bond.isin}, добавлена {ut.added_at.strftime('%Y-%m-%d')})\n\n"

        await update.message.reply_text(text)


async def process_add_isin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.message.text.strip().upper()

    if not text.isascii():
        await update.message.reply_text(
            "❌ ISIN должен содержать только латинские буквы и цифры\nВведите корректный ISIN повторно или используйте /cancel")
        return AWAITING_ISIN_TO_ADD

    if not ISIN_PATTERN.match(text):
        await update.message.reply_text(
            "⚠️ Неверный формат ISIN.\n"
            "Формат: 2 буквы + 9 символов + 1 цифра.\n"
            "Пример: RU000A0JX0J6\n"
            "Введите корректный ISIN повторно или используйте /cancel"

        )
        return AWAITING_ISIN_TO_ADD

    async with get_session() as session:
        user_result = await session.execute(select(User).filter_by(tg_id=user.id))
        user_db = user_result.scalar()

        if not user_db:
            await update.message.reply_text("Пожалуйста, сначала напишите /start.")
            return ConversationHandler.END

        # Проверка лимита с учетом подписки
        if not await check_tracking_limit(user_db.tg_id):
            await update.message.reply_text(
                "❌ Лимит отслеживаемых бумаг исчерпан.\n"
                "Перейдите на платный тариф: /upgrade"
            )
            return ConversationHandler.END

        tracking_result = await session.execute(
            select(UserTracking).filter_by(user_id=user_db.tg_id, isin=text)
        )
        if tracking_result.scalar():
            await update.message.reply_text("✅ Эта бумага уже отслеживается.")
            return ConversationHandler.END

        bond_result = await session.execute(select(BondsDatabase).filter_by(isin=text))
        bond = bond_result.scalar()

        if not bond:
            name = await get_bond_name_from_moex(text)
            bond = BondsDatabase(isin=text, name=name)
            session.add(bond)
            await session.commit()

        try:
            coupon = await get_next_coupon(bond.isin, bond.figi, bond, session)
            if coupon:
                bond.next_coupon_date = coupon["date"]
                bond.next_coupon_value = coupon["value"]
                await session.commit()
        except Exception as e:
            logging.warning(f"Ошибка обновления купона: {e}")

        tracking = UserTracking(user_id=user_db.tg_id, isin=bond.isin)
        session.add(tracking)
        await session.commit()

        context.user_data['isin'] = text
        await update.message.reply_text(f"Введите количество бумаг для {bond.name or bond.isin}:")
        return AWAITING_QUANTITY


async def process_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        quantity = int(update.message.text.strip())
        if quantity <= 0:
            raise ValueError

        async with get_session() as session:
            # Получаем данные
            user_result = await session.execute(
                select(User).filter_by(tg_id=update.effective_user.id)
            )
            bond_result = await session.execute(
                select(BondsDatabase).filter_by(isin=context.user_data['isin'])
            )
            user_db = user_result.scalar()
            bond = bond_result.scalar()

            # Обновление количества
            tracking_result = await session.execute(
                select(UserTracking).filter_by(user_id=user_db.tg_id, isin=bond.isin)
            )
            existing_tracking = tracking_result.scalar()

            if existing_tracking:
                existing_tracking.quantity = quantity
                await session.commit()
                await update.message.reply_text(f"✅ Количество обновлено: {quantity}")
            else:
                tracking = UserTracking(user_id=user_db.tg_id, isin=bond.isin, quantity=quantity)
                session.add(tracking)
                await session.commit()
                await update.message.reply_text(f"📌 Облигация добавлена! Количество: {quantity}")

        return ConversationHandler.END

    except ValueError:
        await update.message.reply_text("⚠️ Некорректное количество!")
        return AWAITING_QUANTITY
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        await update.message.reply_text("❌ Произошла ошибка!")
        return ConversationHandler.END


async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("➕ Введите ISIN бумаги, которую хотите добавить:")
    return AWAITING_ISIN_TO_ADD


async def remove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🗑 Введите ISIN бумаги, которую нужно удалить из отслеживания:")
    return AWAITING_ISIN_TO_REMOVE


async def process_remove_isin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    isin = update.message.text.strip().upper()
    async with get_session() as session:
        user_result = await session.execute(
            select(User).filter_by(tg_id=update.effective_user.id))
        user = user_result.scalar()

        if not user:
            await update.message.reply_text("Вы пока что не зарегистрированы. Напишите /start.")
            return ConversationHandler.END

        tracking_result = await session.execute(
            select(UserTracking).filter_by(user_id=user.tg_id, isin=isin))
        tracking = tracking_result.scalar()
        if tracking:
            await session.delete(tracking)
            await session.commit()
            await update.message.reply_text(f"✅ Бумага {isin} удалена!")
        else:
            await update.message.reply_text(f"❌ Бумага {isin} не найдена!")

        return ConversationHandler.END


async def show_events(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with get_session() as session:
        user_result = await session.execute(
            select(User)
            .options(selectinload(User.tracked_bonds))
            .filter_by(tg_id=update.effective_user.id)
        )
        user = user_result.scalar()

        if not user or not user.tracked_bonds:
            await update.message.reply_text(
                "❗️ Вы пока что не отслеживаете ни одной облигации.\nДобавьте бумагу при помощи /add")
            return

        text = "📊 Ближайшие события по вашим облигациям:\n\n"
        for ut in user.tracked_bonds:
            bond_result = await session.execute(
                select(BondsDatabase).filter_by(isin=ut.isin))
            bond = bond_result.scalar()

            if not bond:
                continue

            name = bond.name or bond.isin
            quantity = ut.quantity or 1
            event_lines = []

            # Обработка купонов
            if bond.next_coupon_date:
                coupon_status = []
                if bond.next_coupon_value is not None and bond.next_coupon_value != 0:
                    total_coupon = quantity * bond.next_coupon_value
                    coupon_status.append(
                        f"купон {bond.next_coupon_value:.2f} руб.\n"
                        f"💰 Итого: {total_coupon:.2f} руб. для {quantity} шт."
                    )
                else:
                    coupon_status.append("размер купона не указан")

                event_lines.append(
                    f"🏷️ {bond.next_coupon_date} — " + "\n".join(coupon_status)
                )

            # Погашение
            if bond.maturity_date:
                event_lines.append(f"💸🔙 {bond.maturity_date} — погашение")

            # Амортизация
            if bond.amortization_date:
                amort_status = []
                if bond.amortization_value is not None:
                    amort_status.append(f"{bond.amortization_value:.2f} руб.")
                else:
                    amort_status.append("сумма не указана")

                event_lines.append(
                    f"⬇️ Амортизация {bond.amortization_date} — " + "\n".join(amort_status)
                )

            # Оферта
            if bond.offer_date:
                event_lines.append(f"🤝📝 Оферта — {bond.offer_date}")

            # Формирование блока
            if event_lines:
                text += f"• {name}:\n" + "\n".join([f"  {line}" for line in event_lines]) + "\n\n"
            else:
                text += f"• {name}:\n  ✨ Нет ближайших событий\n\n"

        await update.message.reply_text(text)


async def change_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with get_session() as session:
        # Асинхронный запрос пользователя с отслеживаемыми бумагами
        user_result = await session.execute(
            select(User)
            .options(selectinload(User.tracked_bonds))
            .filter_by(tg_id=update.effective_user.id))
        user = user_result.scalar()

        if not user or not user.tracked_bonds:
            await update.message.reply_text(
                "❗️ Вы пока что не отслеживаете ни одной облигации.\nДобавьте бумагу при помощи /add")
            return

        keyboard = []
        for ut in user.tracked_bonds:
            bond_result = await session.execute(
                select(BondsDatabase).filter_by(isin=ut.isin))
            bond = bond_result.scalar()

            if bond:
                keyboard.append([
                    InlineKeyboardButton(
                        f"{bond.name or bond.isin} — Количество: {ut.quantity}",
                        callback_data=bond.isin
                    )
                ])

        await update.message.reply_text(
            "📋 Выберите облигацию для изменения количества:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return AWAITING_ISIN_TO_CHANGE


async def process_change_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    isin = update.message.text.strip().upper()
    async with get_session() as session:
        bond_result = await session.execute(
            select(BondsDatabase).filter_by(isin=isin))
        bond = bond_result.scalar()

        if not bond:
            await update.message.reply_text("❌ Облигация не найдена. Попробуйте снова.")
            return

        context.user_data['isin'] = isin
        await update.message.reply_text("🔢 Введите новое количество облигаций:")
        return AWAITING_QUANTITY


async def handle_change_quantity_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    isin = query.data
    async with get_session() as session:
        bond_result = await session.execute(
            select(BondsDatabase).filter_by(isin=isin))
        bond = bond_result.scalar()

        if not bond:
            await query.answer("❌ Облигация не найдена. Попробуйте снова.")
            return

        context.user_data['isin'] = isin
        await query.answer()

        if query.message:
            await query.message.reply_text("🔢 Введите новое количество облигаций:")
        else:
            await query.answer("❌ Ошибка. Сообщение недоступно.")

        return AWAITING_QUANTITY


# В раздел импортов добавьте:
from datetime import datetime, timedelta


# Добавьте новые обработчики в bot.handlers.py:

async def upgrade_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    async with get_session() as session:
        subscription = await session.scalar(
            select(Subscription).where(Subscription.user_id == user.id)
        )  # Исправлена синтаксическая ошибка

        if not subscription:
            await update.message.reply_text("❌ Подписка не найдена. Начните с /start")
            return

        text = f"📋 <b>Ваша текущая подписка</b>\n\n"
        text += f"• Тариф: {subscription.plan.capitalize() if subscription.plan else 'Не активирован'}\n"

        if subscription.payment_date:
            next_payment = subscription.payment_date + timedelta(days=30)
            text += f"• Следующее списание: {next_payment.strftime('%d.%m.%Y')}\n"

        if subscription.subscription_end:
            text += f"• Действует до: {subscription.subscription_end.strftime('%d.%m.%Y')}\n"

        text += "\n🔐 <b>Доступные тарифы:</b>\n\n" \
                "• Basic - 10 облигаций (390₽/мес)\n" \
                "• Optimal - 20 облигаций (590₽/мес)\n" \
                "• Pro - без ограничений (990₽/мес)\n\n" \
                "Выберите новый тариф:"

        keyboard = [
            [InlineKeyboardButton("Basic", callback_data="upgrade_basic"),
             InlineKeyboardButton("Optimal", callback_data="upgrade_optimal")],
            [InlineKeyboardButton("Pro", callback_data="upgrade_pro")],
            [InlineKeyboardButton("Отмена", callback_data="upgrade_cancel")]
        ]

        await update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )


async def handle_upgrade_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()  # Обязательно подтверждаем callback
    user = query.from_user
    action = query.data.split("_")[1]

    async with get_session() as session:
        subscription = await session.scalar(
            select(Subscription).where(Subscription.user_id == user.id))

        if not subscription:
            await query.answer("❌ Ошибка подписки")
            return

        if action == "cancel":
            await query.message.delete()
            return

        new_plan = action
        price_map = {"basic": 290, "optimal": 590, "pro": 990}

        is_upgrade_from_free = (subscription.plan == "free" and new_plan != "free")

        # Обновляем данные подписки
        if is_upgrade_from_free:
            subscription.is_subscribed = True
            subscription.subscription_start = datetime.now()
            subscription.subscription_end = datetime.now() + timedelta(days=30)
        elif new_plan == "free":  # Если вдруг будет опция downgrade
            subscription.is_subscribed = False
            subscription.subscription_start = None
            subscription.subscription_end = None

        subscription.plan = new_plan
        subscription.payment_date = datetime.now()
        subscription.payment_amount = price_map.get(new_plan, 0)

        # Для платных тарифов обновляем срок действия
        if new_plan != "free" and not is_upgrade_from_free:
            subscription.subscription_end = subscription.subscription_end + timedelta(days=30)

        await session.commit()

        # Формируем ответ
        response_text = (
            f"✅ Тариф изменен на {new_plan.capitalize()}!\n"
            f"Списано: {price_map[new_plan]}₽\n"
            f"Следующее списание: {subscription.subscription_end.strftime('%d.%m.%Y')}"
        )
        await query.edit_message_text(response_text, parse_mode="HTML")


def register_handlers(app: Application):
    # Высший приоритет: базовые команды и колбэки
    app.add_handler(CommandHandler("start", start), group=0)
    app.add_handler(CommandHandler("help", help_command), group=0)
    app.add_handler(CommandHandler("list", list_tracked_bonds), group=0)
    app.add_handler(CommandHandler("events", show_events), group=0)
    app.add_handler(CallbackQueryHandler(handle_upgrade_callback, pattern="^upgrade_"), group=0)

    # Обработчики команд с состояниями (низший приоритет)
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("add", add_command),
            CommandHandler("remove", remove_command),
            CommandHandler("change_quantity", change_quantity),
            CommandHandler("support", support_command),
            CommandHandler("upgrade", upgrade_command)
        ],
        states={
            AWAITING_ISIN_TO_ADD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, process_add_isin)
            ],
            AWAITING_QUANTITY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, process_quantity)
            ],
            AWAITING_ISIN_TO_REMOVE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, process_remove_isin)
            ],
            AWAITING_ISIN_TO_CHANGE: [
                CallbackQueryHandler(handle_change_quantity_callback)
            ],
            AWAITING_SUPPORT_MESSAGE: [
                MessageHandler(
                    filters.TEXT | filters.PHOTO | filters.ATTACHMENT | filters.CAPTION,
                    process_support_message
                )
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CommandHandler("cancel_support", cancel_support)
        ],
        allow_reentry=True,
        per_chat=True,
        per_user=True,
        per_message=False
    )
    app.add_handler(conv_handler, group=1)
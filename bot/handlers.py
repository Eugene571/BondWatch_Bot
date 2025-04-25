# bot.handlers.py
import sys

from sqlalchemy.orm import selectinload
from telegram import Update
from telegram.ext import (
    CommandHandler,
    Application,
    ContextTypes,
    filters,
    MessageHandler,
    ConversationHandler,
    CallbackQueryHandler
)
import re
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import logging
from datetime import datetime

from bonds_get.bond_update import get_next_coupon
from bonds_get.bond_utils import save_bond_events
from bonds_get.moex_lookup import get_bondization_data_from_moex
from bonds_get.moex_name_lookup import get_bond_name_from_moex
from database.db import get_session, User, BondsDatabase, UserTracking

ISIN_PATTERN = re.compile(r'^[A-Z]{2}[A-Z0-9]{10}$')

AWAITING_ISIN_TO_REMOVE = 1
AWAITING_ISIN_TO_ADD = 2
AWAITING_QUANTITY = 3
AWAITING_ISIN_TO_CHANGE = 4


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    session = get_session()

    db_user = session.query(User).filter_by(tg_id=user.id).first()
    if not db_user:
        db_user = User(tg_id=user.id, full_name=user.full_name)
        session.add(db_user)
        session.commit()
        context.bot_data.get("logger", print)(f"✅ Новый пользователь: {user.full_name} ({user.id})")

    await update.message.reply_text(
        f"👋 Привет, {user.first_name}!\n\n"
        "Я BondWatch — бот, который следит за купонами и погашениями твоих облигаций.\n\n"
        "📎 Отправь ISIN, чтобы я добавил бумагу и прислал напоминание о купонах и погашении.\n"
        "Ты можешь бесплатно отслеживать до 3 бумаг.\n\n"
        "🔔 Начнём!"
    )


async def list_tracked_bonds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session = get_session()
    user = session.query(User).options(selectinload(User.tracked_bonds)).filter_by(
        tg_id=update.effective_user.id).first()

    if not user or not user.tracked_bonds:
        await update.message.reply_text(
            "❗️Ты пока не отслеживаешь ни одной облигации. Добавь хотя бы одну через команду /add!")
        session.close()
        return

    text = "📋 Вот список твоих отслеживаемых бумаг:\n\n"
    for ut in user.tracked_bonds:
        bond = session.query(BondsDatabase).filter_by(isin=ut.isin).first()
        if not bond:
            continue

        name = bond.name or bond.isin
        if not bond.name:
            moex_name = await get_bond_name_from_moex(bond.isin)
            if moex_name:
                bond.name = moex_name
                session.commit()
                name = moex_name

        added = ut.added_at.strftime("%Y-%m-%d")
        quantity = ut.quantity  # Извлекаем количество бумаг из UserTracking

        next_coupon_text = ""  # Здесь можно добавить логику для следующего купона, если нужно

        text += f"• {name} - {quantity} бумаг \n({bond.isin}, добавлена {added}){next_coupon_text}\n\n"

    session.close()
    await update.message.reply_text(text)


async def process_add_isin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.message.text.strip().upper()

    if not ISIN_PATTERN.match(text):
        await update.message.reply_text("⚠️ Это не похоже на ISIN. Попробуй ещё раз.")
        return AWAITING_ISIN_TO_ADD

    session = get_session()
    user_db = session.query(User).filter_by(tg_id=user.id).first()
    if not user_db:
        await update.message.reply_text("Пожалуйста, сначала напиши /start.")
        return ConversationHandler.END

    # Проверяем, сколько бумаг отслеживает пользователь
    tracking_count = session.query(UserTracking).filter_by(user_id=user_db.id).count()
    if tracking_count >= 3:
        await update.message.reply_text("❌ Ты уже отслеживаешь 3 бумаги. Удали одну, чтобы добавить новую.")
        return ConversationHandler.END

    # Проверяем, отслеживает ли пользователь уже эту бумагу
    already_tracking = session.query(UserTracking).filter_by(user_id=user_db.id, isin=text).first()
    if already_tracking:
        await update.message.reply_text("✅ Ты уже отслеживаешь эту бумагу.")
        return ConversationHandler.END

    # Получаем данные о бумаге
    bond = session.query(BondsDatabase).filter_by(isin=text).first()
    if not bond:
        name = await get_bond_name_from_moex(text)
        bond = BondsDatabase(isin=text, name=name)
        session.add(bond)
        session.commit()

    # Получаем данные о купоне и дате погашения
    try:
        coupon = await get_next_coupon(bond.isin, bond.figi, bond, session)
        if coupon:
            bond.next_coupon_date = coupon["date"]
            bond.next_coupon_value = coupon["value"]
            session.commit()
    except Exception as e:
        logging.warning(f"T-API купон не получен: {e}")

    # Если не удалось получить купоны через T-API, пробуем MOEX
    if not bond.next_coupon_date or not bond.next_coupon_value:
        try:
            moex_coupons = await get_bondization_data_from_moex(bond.isin)
            if moex_coupons:
                bond.next_coupon_date = moex_coupons[0]["date"]
                bond.next_coupon_value = moex_coupons[0]["value"]
                session.commit()
        except Exception as e:
            logging.warning(f"MOEX купон не получен: {e}")

    # Сохраняем отслеживание бумаги для пользователя
    tracking = UserTracking(user_id=user_db.id, isin=bond.isin)
    session.add(tracking)
    session.commit()

    context.user_data['isin'] = text

    # Запрашиваем количество бумаг у пользователя
    await update.message.reply_text(f"Введите количество бумаг для {bond.name or bond.isin}:")
    return AWAITING_QUANTITY


async def process_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.bot_data.get("logger", print)(
        f"➡️ process_quantity: update={update.message.text}, user_data={context.user_data}")
    try:
        quantity = int(update.message.text.strip())
        context.bot_data.get("logger", print)(f"➡️ process_quantity: quantity={quantity}")
        if quantity <= 0:
            raise ValueError("Количество должно быть положительным числом.")

        session = get_session()
        user_db = session.query(User).filter_by(tg_id=update.effective_user.id).first()
        bond = session.query(BondsDatabase).filter_by(isin=context.user_data['isin']).first()

        if not user_db or not bond:
            await update.message.reply_text("Ошибка. Попробуй снова.")
            return ConversationHandler.END

        existing_tracking = session.query(UserTracking).filter_by(user_id=user_db.tg_id, isin=bond.isin).first()

        if existing_tracking:
            existing_tracking.quantity = quantity
            session.commit()
            await update.message.reply_text(
                f"✅ Количество для бумаги {bond.name or bond.isin} обновлено на {quantity}.")
        else:
            tracking = UserTracking(user_id=user_db.id, isin=bond.isin, quantity=quantity)
            session.add(tracking)
            session.commit()
            await update.message.reply_text(f"📌 Бумага {bond.name or bond.isin} добавлена! Количество: {quantity}")

        session.close()
        return ConversationHandler.END

    except ValueError:
        await update.message.reply_text("⚠️ Введено некорректное количество. Попробуй снова.")
        return AWAITING_QUANTITY
    except Exception as e:
        context.bot_data.get("logger", print)(f"⚠️ Ошибка в process_quantity: {e}")
        await update.message.reply_text("❌ Произошла непредвиденная ошибка. Попробуй позже.")
        return ConversationHandler.END


async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("➕ Введи ISIN бумаги, которую хочешь добавить:")
    return AWAITING_ISIN_TO_ADD


async def remove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🗑 Введи ISIN бумаги, которую хочешь удалить из отслеживания:")
    return AWAITING_ISIN_TO_REMOVE


async def process_remove_isin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    isin = update.message.text.strip().upper()
    session = get_session()
    user = session.query(User).filter_by(tg_id=update.effective_user.id).first()

    if not user:
        await update.message.reply_text("Ты пока не зарегистрирован. Напиши /start.")
        return ConversationHandler.END

    tracking = session.query(UserTracking).filter_by(user_id=user.tg_id, isin=isin).first()
    if not tracking:
        await update.message.reply_text(f"❌ Ты не отслеживаешь бумагу с ISIN {isin}.")
    else:
        session.delete(tracking)
        session.commit()
        await update.message.reply_text(f"✅ Бумага {isin} успешно удалена из отслеживания.")

    return ConversationHandler.END


async def show_events(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session = get_session()
    user = session.query(User).options(selectinload(User.tracked_bonds)).filter_by(
        tg_id=update.effective_user.id).first()

    if not user or not user.tracked_bonds:
        await update.message.reply_text("❗️ Ты пока не отслеживаешь ни одной облигации.")
        return

    text = "📊 Ближайшие события по твоим облигациям:\n\n"
    for ut in user.tracked_bonds:
        bond = session.query(BondsDatabase).filter_by(isin=ut.isin).first()
        if not bond:
            continue
        name = bond.name or bond.isin
        if bond.next_coupon_date and bond.next_coupon_value and bond.maturity_date:
            total_coupon = ut.quantity * bond.next_coupon_value
            text += f"• {name}:\n  🏷️ {bond.next_coupon_date} — купон {bond.next_coupon_value:.2f} руб.\n"
            text += f"  💸🔙 {bond.maturity_date} — погашение\n"
            text += f"  💰 Итого: {total_coupon:.2f} руб. для {ut.quantity} облигаций\n\n"
        elif bond.next_coupon_date and bond.next_coupon_value and not bond.maturity_date:
            total_coupon = ut.quantity * bond.next_coupon_value
            text += f"• {name}:\n  🏷️ {bond.next_coupon_date} — купон {bond.next_coupon_value:.2f} руб.\n"
            text += f"  💰 Итого: {total_coupon:.2f} руб. для {ut.quantity} облигаций\n"
        else:
            text += f"• {name}:\n  ✨ Нет ближайших событий\n"

    await update.message.reply_text(text)


async def change_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session = get_session()
    user = session.query(User).filter_by(tg_id=update.effective_user.id).first()

    if not user or not user.tracked_bonds:
        await update.message.reply_text("❗️ Ты пока не отслеживаешь ни одной облигации.")
        return

    # Создаем клавиатуру с кнопками для каждой облигации
    keyboard = []
    for ut in user.tracked_bonds:
        bond = session.query(BondsDatabase).filter_by(isin=ut.isin).first()
        keyboard.append(
            [InlineKeyboardButton(f"{bond.name or bond.isin} — Количество: {ut.quantity}", callback_data=bond.isin)])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "📋 Выбери облигацию для изменения количества:", reply_markup=reply_markup)
    return AWAITING_ISIN_TO_CHANGE


async def process_change_quantity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    isin = update.message.text.strip().upper()
    session = get_session()

    bond = session.query(BondsDatabase).filter_by(isin=isin).first()
    if not bond:
        await update.message.reply_text("❌ Облигация не найдена. Попробуй снова.")
        return

    # Запрашиваем новое количество
    context.user_data['isin'] = isin
    await update.message.reply_text("🔢 Введи новое количество облигаций:")
    return AWAITING_QUANTITY


async def handle_change_quantity_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    isin = query.data  # Получаем ISIN из callback_data

    context.bot_data.get("logger", print)(f"➡️ handle_change_quantity_callback: isin={isin}")

    session = get_session()
    bond = session.query(BondsDatabase).filter_by(isin=isin).first()
    if not bond:
        await query.answer("❌ Облигация не найдена. Попробуй снова.")
        return

    # Запрашиваем новое количество
    context.user_data['isin'] = isin
    await query.answer()  # Подтверждаем нажатие на кнопку

    context.bot_data.get("logger", print)(
        f"➡️ handle_change_quantity_callback: context.user_data['isin']={context.user_data.get('isin')}")

    # Проверяем, что сообщение доступно перед отправкой
    if query.message:
        await query.message.reply_text("🔢 Введи новое количество облигаций:")
    else:
        await query.answer("❌ Ошибка. Сообщение недоступно.")
    logger = logging.getLogger(__name__)
    logger.info(f"➡️ Устанавливаю состояние: {AWAITING_QUANTITY}")
    return AWAITING_QUANTITY  # Убедитесь, что возвращаете правильное состояние


def register_handlers(app: Application):
    # Основные команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("list", list_tracked_bonds))
    app.add_handler(CommandHandler("events", show_events))

    # Конверсация для добавления новой облигации
    add_conv = ConversationHandler(
        entry_points=[CommandHandler("add", add_command)],
        states={
            AWAITING_ISIN_TO_ADD: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_add_isin)],
            AWAITING_QUANTITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_quantity)],
        },
        fallbacks=[],
    )
    app.add_handler(add_conv)

    # Конверсация для изменения количества облигаций
    change_quantity_conv = ConversationHandler(
        entry_points=[CommandHandler("change_quantity", change_quantity)],
        states={
            AWAITING_ISIN_TO_CHANGE: [CallbackQueryHandler(handle_change_quantity_callback)],
            AWAITING_QUANTITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_quantity)],
        },
        fallbacks=[
            MessageHandler(filters.TEXT & ~filters.COMMAND,
                           lambda update, context: print(f"Fallback: {update.message.text}"))
        ],
    )
    app.add_handler(change_quantity_conv)

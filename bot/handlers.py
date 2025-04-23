# bot.handlers.py
from sqlalchemy.orm import selectinload
from telegram import Update
from telegram.ext import (
    CommandHandler,
    Application,
    ContextTypes,
    filters,
    MessageHandler,
    ConversationHandler,
)
import re
import logging
from datetime import datetime
from bonds_get.moex_lookup import get_bond_coupons_from_moex
from bonds_get.moex_name_lookup import get_bond_name_from_moex
from database.db import get_session, User, BondsDatabase, UserTracking

ISIN_PATTERN = re.compile(r'^[A-Z]{2}[A-Z0-9]{10}$')

AWAITING_ISIN_TO_REMOVE = 1
AWAITING_ISIN_TO_ADD = 2


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
    user = session.query(User).options(selectinload(User.tracked_bonds)).filter_by(tg_id=update.effective_user.id).first()

    if not user or not user.tracked_bonds:
        await update.message.reply_text("❗️Ты пока не отслеживаешь ни одной облигации. Добавь хотя бы одну через команду /add!")
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
        next_coupon_text = ""
        if bond.next_coupon_date and bond.next_coupon_value:
            next_coupon_text = f"\n👉 Следующий купон: {bond.next_coupon_date} на сумму {bond.next_coupon_value} руб."

        text += f"• {name} ({bond.isin}, добавлена {added}){next_coupon_text}\n"

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

    tracking_count = session.query(UserTracking).filter_by(user_id=user.id).count()
    if tracking_count >= 3:
        await update.message.reply_text("❌ Ты уже отслеживаешь 3 бумаги. Удали одну, чтобы добавить новую.")
        return ConversationHandler.END

    already_tracking = session.query(UserTracking).filter_by(user_id=user.id, isin=text).first()
    if already_tracking:
        await update.message.reply_text("✅ Ты уже отслеживаешь эту бумагу.")
        return ConversationHandler.END

    # Создаём или получаем бумагу
    bond = session.query(BondsDatabase).filter_by(isin=text).first()
    if not bond:
        name = await get_bond_name_from_moex(text)
        bond = BondsDatabase(isin=text, name=name)
        session.add(bond)
        session.commit()

    # Если купонные данные отсутствуют — загружаем с MOEX
    if not bond.next_coupon_date or not bond.next_coupon_value:
        try:
            coupons = await get_bond_coupons_from_moex(bond.isin)
            today = datetime.today().date()

            upcoming = [
                {
                    "date": datetime.strptime(c["couponDate"], "%Y-%m-%d").date(),
                    "value": c["couponValue"]
                }
                for c in coupons
                if c.get("couponDate") and datetime.strptime(c["couponDate"], "%Y-%m-%d").date() >= today
            ]

            if upcoming:
                upcoming.sort(key=lambda x: x["date"])
                bond.next_coupon_date = upcoming[0]["date"]
                bond.next_coupon_value = upcoming[0]["value"]
                session.commit()
                logging.info(f"✅ Купонные данные загружены с MOEX для {bond.isin}")
        except Exception as e:
            logging.warning(f"❌ Ошибка при получении купонов с MOEX для {bond.isin}: {e}")

    # Добавляем отслеживание пользователю
    tracking = UserTracking(user_id=user.id, isin=bond.isin)
    session.add(tracking)
    session.commit()

    await update.message.reply_text(f"📌 Бумага {bond.name or bond.isin} добавлена!")
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
    user = session.query(User).options(selectinload(User.tracked_bonds)).filter_by(tg_id=update.effective_user.id).first()

    if not user or not user.tracked_bonds:
        await update.message.reply_text("❗️ Ты пока не отслеживаешь ни одной облигации.")
        return

    text = "📊 Ближайшие события по твоим облигациям:\n\n"
    for ut in user.tracked_bonds:
        bond = session.query(BondsDatabase).filter_by(isin=ut.isin).first()
        if not bond:
            continue
        name = bond.name or bond.isin
        if bond.next_coupon_date and bond.next_coupon_value:
            text += f"• {name}:\n  🏷️ {bond.next_coupon_date} — купон {bond.next_coupon_value:.2f} руб.\n"
        else:
            text += f"• {name}:\n  ✨ Нет ближайших событий\n"

    await update.message.reply_text(text)


def register_handlers(app: Application):
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("list", list_tracked_bonds))
    app.add_handler(CommandHandler("events", show_events))

    add_conv = ConversationHandler(
        entry_points=[CommandHandler("add", add_command)],
        states={AWAITING_ISIN_TO_ADD: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_add_isin)]},
        fallbacks=[],
    )
    app.add_handler(add_conv)

    remove_conv = ConversationHandler(
        entry_points=[CommandHandler("remove", remove_command)],
        states={AWAITING_ISIN_TO_REMOVE: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_remove_isin)]},
        fallbacks=[],
    )
    app.add_handler(remove_conv)
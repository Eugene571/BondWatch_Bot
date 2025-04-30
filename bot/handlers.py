# bot.handlers.py
import sys

from sqlalchemy import select, func
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

from bonds_get.bond_update import get_next_coupon
from bonds_get.moex_name_lookup import get_bond_name_from_moex
from bot.subscription_utils import check_tracking_limit
from database.db import get_session, User, BondsDatabase, UserTracking, Subscription

ISIN_PATTERN = re.compile(r'^[A-Z]{2}[A-Z0-9]{10}$')

AWAITING_ISIN_TO_REMOVE = 1
AWAITING_ISIN_TO_ADD = 2
AWAITING_QUANTITY = 3
AWAITING_ISIN_TO_CHANGE = 4


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
        "Я BondWatch — бот, который следит за купонами и погашениями твоих облигаций.\n\n"
        "📎 Отправь ISIN, чтобы я добавил бумагу и прислал напоминание о купонах и погашении.\n"
        "Ты можешь бесплатно отслеживать 1 бумагу.\n\n"
        "🔔 Начнём!"
    )


async def list_tracked_bonds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with get_session() as session:
        result = await session.execute(
            select(User)
            .options(selectinload(User.tracked_bonds))
            .filter_by(tg_id=update.effective_user.id)
        )
        user = result.scalar()

        if not user or not user.tracked_bonds:
            await update.message.reply_text("❗️Ты пока не отслеживаешь ни одной облигации.")
            return

        text = "📋 Вот список твоих отслеживаемых бумаг:\n\n"
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

    if not ISIN_PATTERN.match(text):
        await update.message.reply_text("⚠️ Это не похоже на ISIN. Попробуй ещё раз.")
        return AWAITING_ISIN_TO_ADD

    async with get_session() as session:
        user_result = await session.execute(select(User).filter_by(tg_id=user.id))
        user_db = user_result.scalar()

        if not user_db:
            await update.message.reply_text("Пожалуйста, сначала напиши /start.")
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
    await update.message.reply_text("➕ Введи ISIN бумаги, которую хочешь добавить:")
    return AWAITING_ISIN_TO_ADD


async def remove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🗑 Введи ISIN бумаги, которую хочешь удалить из отслеживания:")
    return AWAITING_ISIN_TO_REMOVE


async def process_remove_isin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    isin = update.message.text.strip().upper()
    async with get_session() as session:
        user_result = await session.execute(
            select(User).filter_by(tg_id=update.effective_user.id))
        user = user_result.scalar()

        if not user:
            await update.message.reply_text("Ты пока не зарегистрирован. Напиши /start.")
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
        # Асинхронный запрос пользователя с отслеживаемыми бумагами
        user_result = await session.execute(
            select(User)
            .options(selectinload(User.tracked_bonds))
            .filter_by(tg_id=update.effective_user.id))
        user = user_result.scalar()

        if not user or not user.tracked_bonds:
            await update.message.reply_text("❗️ Ты пока не отслеживаешь ни одной облигации.")
            return

        text = "📊 Ближайшие события по твоим облигациям:\n\n"
        for ut in user.tracked_bonds:
            # Асинхронный запрос данных облигации
            bond_result = await session.execute(
                select(BondsDatabase).filter_by(isin=ut.isin))
            bond = bond_result.scalar()

            if not bond:
                continue

            name = bond.name or bond.isin
            quantity = ut.quantity or 1  # На всякий случай, если quantity может быть None
            total_coupon = quantity * (bond.next_coupon_value or 0)

            # Формирование текста события
            event_lines = []
            if bond.next_coupon_date:
                event_lines.append(
                    f"🏷️ {bond.next_coupon_date} — купон {bond.next_coupon_value:.2f} руб.\n"
                    f"💰 Итого: {total_coupon:.2f} руб. для {quantity} облигаций"
                )

            if bond.maturity_date:
                event_lines.append(f"💸🔙 {bond.maturity_date} — погашение")

            if bond.amortization_date and bond.amortization_value:
                event_lines.append(
                    f"⬇️ Амортизация {bond.amortization_date} — {bond.amortization_value:.2f} руб."
                )
            if bond.offer_date:
                event_lines.append(
                    f"🤝📝 Оферта — {bond.offer_date}.")

            text += f"• {name}:\n" + "\n".join(event_lines) + "\n\n" if event_lines else "✨ Нет событий\n\n"

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
            await update.message.reply_text("❗️ Ты пока не отслеживаешь ни одной облигации.")
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
            "📋 Выбери облигацию для изменения количества:",
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
            await update.message.reply_text("❌ Облигация не найдена. Попробуй снова.")
            return

        context.user_data['isin'] = isin
        await update.message.reply_text("🔢 Введи новое количество облигаций:")
        return AWAITING_QUANTITY


async def handle_change_quantity_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    isin = query.data
    async with get_session() as session:
        bond_result = await session.execute(
            select(BondsDatabase).filter_by(isin=isin))
        bond = bond_result.scalar()

        if not bond:
            await query.answer("❌ Облигация не найдена. Попробуй снова.")
            return

        context.user_data['isin'] = isin
        await query.answer()

        if query.message:
            await query.message.reply_text("🔢 Введи новое количество облигаций:")
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
    # Основные команды
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("list", list_tracked_bonds))
    app.add_handler(CommandHandler("events", show_events))
    app.add_handler(CommandHandler("upgrade", upgrade_command))
    app.add_handler(CallbackQueryHandler(handle_upgrade_callback, pattern="^upgrade_"))

    # Конверсация для добавления новой облигации
    add_conv = ConversationHandler(
        entry_points=[CommandHandler("add", add_command)],
        states={
            AWAITING_ISIN_TO_ADD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Regex(ISIN_PATTERN), process_add_isin)],
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

    # Конверсация для удаления облигации
    remove_conv = ConversationHandler(
        entry_points=[CommandHandler("remove", remove_command)],
        states={
            AWAITING_ISIN_TO_REMOVE: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_remove_isin)],
        },
        fallbacks=[],
    )
    app.add_handler(remove_conv)

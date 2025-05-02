# bonds_get/nightly_sync.py

import logging
from datetime import datetime, date
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from database.db import get_session, BondsDatabase
from bonds_get.moex_lookup import get_bondization_data_from_moex

logger = logging.getLogger("nightly_sync")


async def needs_update(bond: BondsDatabase) -> bool:
    """Проверяет, требуется ли обновление для облигации (аналогично bond_update)"""
    today = date.today()

    # Обязательное обновление если нет maturity_date
    if bond.maturity_date is None:
        return True

    # Проверка прошедших дат (купон, оферта, амортизация)
    date_fields = [
        bond.next_coupon_date,
        bond.offer_date,
        bond.amortization_date
    ]

    if any(d and d <= today for d in date_fields):
        return True

    # Проверка пустых полей
    empty_fields = [
        bond.next_coupon_date,
        bond.next_coupon_value,
        bond.offer_date,
        bond.amortization_date,
        bond.amortization_value
    ]

    return any(field is None for field in empty_fields)


async def update_bond_data(bond: BondsDatabase, session: AsyncSession):
    """Унифицированная версия обновления (аналогично bond_update)"""
    try:
        logger.info(f"🔄 Начинаем обновление для {bond.isin}")
        data = await get_bondization_data_from_moex(bond.isin)
        today = date.today()

        # Основные поля
        updates_made = False
        if data.get("maturity_date"):
            bond.maturity_date = data["maturity_date"]
            updates_made = True
            logger.info(f"📅 Дата погашения: {bond.maturity_date}")

        if data.get("next_offer_date"):
            bond.offer_date = data["next_offer_date"]
            updates_made = True
            logger.info(f"📆 Дата оферты: {bond.offer_date}")

        # Обработка купонов
        upcoming_coupons = []
        for c in data.get("coupons", []):
            if raw_date := c.get("couponDate"):
                try:
                    parsed_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
                    if parsed_date > today:
                        upcoming_coupons.append({
                            "date": parsed_date,
                            "value": c.get("couponValue", 0.0)
                        })
                except ValueError:
                    logger.warning(f"⚠️ Невалидная дата купона: {raw_date}")

        if upcoming_coupons:
            next_coupon = min(upcoming_coupons, key=lambda x: x["date"])
            bond.next_coupon_date = next_coupon["date"]
            bond.next_coupon_value = float(next_coupon["value"])
            logger.info(f"✅ Купон: {bond.next_coupon_date}, {bond.next_coupon_value}")
            updates_made = True

        # Обработка амортизаций
        upcoming_amorts = []
        for a in data.get("amortizations", []):
            if a.get("dataSource") == "amortization" and (raw_date := a.get("amortDate")):
                try:
                    parsed_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
                    if parsed_date >= today:
                        upcoming_amorts.append({
                            "date": parsed_date,
                            "value": a.get("amortValue", 0.0)
                        })
                except ValueError:
                    logger.warning(f"⚠️ Невалидная дата амортизации: {raw_date}")

        if upcoming_amorts:
            next_amort = min(upcoming_amorts, key=lambda x: x["date"])
            bond.amortization_date = next_amort["date"]
            bond.amortization_value = float(next_amort["value"])
            logger.info(f"✅ Амортизация: {bond.amortization_date}, {bond.amortization_value}")
            updates_made = True

        if updates_made:
            bond.last_updated = datetime.utcnow()
            await session.commit()
            logger.info(f"✅ Успешное обновление {bond.isin}")

    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении {bond.isin}: {e}", exc_info=True)
        await session.rollback()
        raise


async def perform_nightly_sync():
    """Основная функция ночной сверки"""
    logger.info("🌙 Запуск ночной сверки данных")

    async with get_session() as session:
        try:
            result = await session.execute(select(BondsDatabase))
            bonds = result.scalars().all()

            for bond in bonds:
                if await needs_update(bond):
                    await update_bond_data(bond, session)
                else:
                    logger.debug(f"✓ {bond.isin} не требует обновления")

        except Exception as e:
            logger.error(f"🚨 Критическая ошибка: {e}")

    logger.info("🏁 Сверка завершена")
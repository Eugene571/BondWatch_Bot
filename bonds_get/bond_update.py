# bonds_get.bond_update

from datetime import datetime
import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from bonds_get.moex_lookup import get_bondization_data_from_moex
from database.db import BondsDatabase

logger = logging.getLogger("bond_update")


async def get_next_coupon(
        isin: str,
        figi: str | None,
        bond: BondsDatabase,
        session: AsyncSession  # Исправлен тип сессии
) -> None:
    today = datetime.today().date()

    try:
        data = await get_bondization_data_from_moex(isin)
        coupons = data.get("coupons", [])
        amortizations = data.get("amortizations", [])
        maturity_date = data.get("maturity_date")
        next_offer_date = data.get("next_offer_date")

        # Обновление основных данных
        updates_made = False
        if maturity_date:
            bond.maturity_date = maturity_date
            updates_made = True
            logger.debug(f"📅 Дата погашения обновлена: {bond.maturity_date}")

        if next_offer_date:
            bond.offer_date = next_offer_date
            updates_made = True
            logger.debug(f"📆 Дата оферты обновлена: {bond.offer_date}")

        if updates_made:
            await session.commit()  # Асинхронный коммит

        # Обработка купонов
        upcoming = []
        for c in coupons:
            if raw_date := c.get("couponDate"):
                try:
                    parsed_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
                    if parsed_date >= today:
                        upcoming.append({**c, "parsed_date": parsed_date})
                except ValueError:
                    logger.warning(f"⚠️ Невалидная дата купона: {raw_date} для {isin}")

        if upcoming:
            upcoming.sort(key=lambda x: x["parsed_date"])
            first = upcoming[0]
            bond.next_coupon_date = first["parsed_date"]
            bond.next_coupon_value = float(first["couponValue"]) if first.get("couponValue") else None
            logger.info(f"✅ Обновлён купон: {bond.next_coupon_date}, {bond.next_coupon_value}")
            await session.commit()

        # Обработка амортизаций
        upcoming_amortizations = [
            {**a, "parsed_date": datetime.strptime(a["amortDate"], "%Y-%m-%d").date()}
            for a in amortizations
            if a.get("dataSource") == "amortization"
               and a.get("amortDate")
               and datetime.strptime(a["amortDate"], "%Y-%m-%d").date() >= today
        ]

        if upcoming_amortizations:
            upcoming_amortizations.sort(key=lambda x: x["parsed_date"])
            first_amort = upcoming_amortizations[0]
            bond.amortization_date = first_amort["parsed_date"]
            bond.amortization_value = float(first_amort.get("amortValue") or 0)
            logger.info(f"✅ Обновлена амортизация: {bond.amortization_date}, {bond.amortization_value}")
            await session.commit()

        # Получение обновлённой записи
        result = await session.execute(
            select(BondsDatabase).filter_by(isin=isin)  # Асинхронный запрос
        )
        updated_bond = result.scalars().first()

        logger.debug(
            f"Обновленные данные: "
            f"погашение={updated_bond.maturity_date}, "
            f"оферта={updated_bond.offer_date}, "
            f"купон={updated_bond.next_coupon_date}"
        )

    except Exception as e:
        logger.error(f"❌ Критическая ошибка для {isin}: {e}", exc_info=True)
        await session.rollback()  # Асинхронный откат

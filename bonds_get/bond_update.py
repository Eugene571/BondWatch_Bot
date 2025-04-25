# bonds_get.bond_update

from datetime import datetime
import logging
from sqlalchemy.orm import Session
from bonds_get.moex_lookup import get_bondization_data_from_moex
from database.db import BondsDatabase

logger = logging.getLogger("bond_update")


async def get_next_coupon(isin: str, figi: str | None, bond: BondsDatabase, session: Session) -> None:
    today = datetime.today().date()

    try:
        # Получаем все данные о купонах и амортизациях, включая дату погашения
        data = await get_bondization_data_from_moex(isin)
        coupons = data.get("coupons", [])
        amortizations = data.get("amortizations", [])
        maturity_date = data.get("maturity_date")  # Получаем дату погашения

        logging.debug(f"🧾 Все купоны от MOEX для {isin}: {coupons}")
        logging.debug(f"💸 Все амортизации от MOEX для {isin}: {amortizations}")
        # Обновляем дату погашения, если она была получена
        if maturity_date:
            bond.maturity_date = maturity_date
            logging.debug(f"📅 Дата погашения обновлена: {bond.maturity_date}")
        session.commit()

        # Обработка ближайшего купона
        upcoming = []
        for c in coupons:
            raw_date = c.get("couponDate")
            if not raw_date:
                continue
            try:
                parsed_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
                if parsed_date >= today:
                    c["parsed_date"] = parsed_date
                    upcoming.append(c)
            except ValueError:
                logging.warning(f"⚠️ Невалидная дата {raw_date} от MOEX для {isin}")
                continue

        if upcoming:
            upcoming.sort(key=lambda x: x["parsed_date"])
            first = upcoming[0]
            bond.next_coupon_date = first["parsed_date"]
            bond.next_coupon_value = float(first["couponValue"]) if first.get("couponValue") else None
            logging.info(f"✅ Обновлён купон: {bond.next_coupon_date}, {bond.next_coupon_value}")

        session.commit()

        upcoming_amortizations = []
        for a in amortizations:
            if a.get("dataSource") != "amortization":
                continue  # фильтруем строго по типу

            raw_date = a.get("amortDate")
            if not raw_date:
                continue

            try:
                parsed_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
                if parsed_date >= today:
                    a["parsed_date"] = parsed_date
                    upcoming_amortizations.append(a)
            except ValueError:
                logger.warning(f"⚠️ Невалидная дата амортизации: {raw_date} для ISIN {isin}")

        if upcoming_amortizations:
            upcoming_amortizations.sort(key=lambda x: x["parsed_date"])
            first_amort = upcoming_amortizations[0]
            bond.amortization_date = first_amort["parsed_date"]
            bond.amortization_value = float(first_amort.get("amortValue") or 0)
            logger.info(f"✅ Обновлена амортизация: {bond.amortization_date}, {bond.amortization_value}")

        session.commit()
        bond = session.query(BondsDatabase).filter_by(isin=isin).first()
        logger.debug(f"Дата погашения после коммита: {bond.maturity_date}")
        logger.debug(f"💾 Commit завершён: {bond}")
    except Exception as e:
        logging.warning(f"❌ Ошибка при обновлении данных купона и погашения для {isin}: {e}")

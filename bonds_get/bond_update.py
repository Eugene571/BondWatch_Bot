# bonds_get.bond_update
from datetime import datetime, timedelta
import logging
from sqlalchemy.orm import Session
from bonds_get.moex_lookup import get_bond_coupons_from_moex
from database.db import BondsDatabase


async def get_next_coupon(isin: str, figi: str | None, bond: BondsDatabase, session: Session) -> None:
    today = datetime.today().date()

    # Пробуем получить купоны через MOEX
    try:
        coupons = await get_bond_coupons_from_moex(isin)
        logging.debug(f"🧾 Все купоны от MOEX для {isin}: {coupons}")

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

        logging.debug(f"🔍 Подходящие купоны от MOEX для {isin}: {upcoming}")

        if upcoming:
            upcoming.sort(key=lambda x: x["parsed_date"])
            first = upcoming[0]
            logging.debug(f"💾 Первый купон для сохранения: {first}")
            bond.next_coupon_date = first["parsed_date"]
            bond.next_coupon_value = float(first["couponValue"]) if first.get("couponValue") else None
            session.commit()
            logging.debug(f"💾 Commit завершён, купон сохранён: {bond.next_coupon_date}, {bond.next_coupon_value}")
            logging.info(f"✅ Данные купона обновлены через MOEX для {bond.isin}")
            return
    except Exception as e:
        logging.warning(f"❌ MOEX купоны не получены для {isin}: {e}")

    logging.info(f"❌ Купоны не найдены для {isin}")
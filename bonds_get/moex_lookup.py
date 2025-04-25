# bonds_get.moex_lookup.py

import httpx
import logging
from datetime import datetime
from typing import List, Optional


async def get_bondization_data_from_moex(isin: str) -> dict:
    """
    Получение данных о купонах и амортизациях (включая погашение) по ISIN с MOEX.
    Возвращает словарь:
    {
        "isin": str,
        "coupons": List[dict],
        "amortizations": List[dict],
        "maturity_date": Optional[date]
    }
    """
    url = f"https://iss.moex.com/iss/securities/{isin}/bondization.json"
    logging.info(f"🔄 Запрос bondization.json к MOEX для ISIN {isin}: {url}")

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            logging.info(f"📦 Ответ от MOEX для {isin} успешно получен")

        result = {
            "isin": isin,
            "coupons": [],
            "amortizations": [],
            "maturity_date": None
        }

        # Обработка купонов
        coupons_meta = data.get("coupons", {}).get("columns", [])
        coupons_data = data.get("coupons", {}).get("data", [])

        try:
            idx_coupondate = coupons_meta.index("coupondate")
            idx_value = coupons_meta.index("value")
            idx_percent = coupons_meta.index("valueprc")
        except ValueError as e:
            logging.warning(f"⚠️ Не найдены нужные поля купонов для {isin}: {e}")
            idx_coupondate = idx_value = idx_percent = -1

        for row in coupons_data:
            if idx_coupondate == -1:
                break
            coupon_date = row[idx_coupondate]
            if not coupon_date:
                continue
            result["coupons"].append({
                "couponDate": str(coupon_date),
                "couponValue": row[idx_value] or 0,
                "couponPercent": row[idx_percent] or 0,
                "type": "COUPON"
            })

        logging.info(f"📈 Найдено {len(result['coupons'])} купонов для {isin}")

        # Обработка амортизаций и определение maturity_date
        amort_meta = data.get("amortizations", {}).get("columns", [])
        amort_data = data.get("amortizations", {}).get("data", [])

        try:
            idx_source = amort_meta.index("data_source")
            idx_amortdate = amort_meta.index("amortdate")
            idx_value = amort_meta.index("value")
        except ValueError as e:
            logging.warning(f"⚠️ Не найдены нужные поля амортизаций для {isin}: {e}")
            idx_amortdate = idx_value = -1

        maturity_candidate_dates = []

        for row in amort_data:
            if idx_amortdate == -1:
                break
            amort_date = row[idx_amortdate]
            if not amort_date:
                continue
            result["amortizations"].append({
                "amortDate": str(amort_date),
                "amortValue": row[idx_value] or 0,
                "dataSource": row[idx_source] or "",
                "type": "AMORTIZATION"
            })
            maturity_candidate_dates.append(amort_date)

        logging.info(f"🏁 Найдено {len(result['amortizations'])} амортизаций для {isin}")

        if maturity_candidate_dates:
            try:
                parsed_dates = [
                    datetime.strptime(str(d), "%Y-%m-%d").date()
                    for d in maturity_candidate_dates
                ]
                result["maturity_date"] = max(parsed_dates)
                logging.info(f"🎯 Определена дата погашения: {result['maturity_date']}")
            except Exception as e:
                logging.warning(f"⚠️ Ошибка при парсинге дат погашения: {e}")
        return result

    except Exception as e:
        logging.error(f"❌ Ошибка при получении bondization.json для {isin}: {e}")
        return {
            "isin": isin,
            "coupons": [],
            "amortizations": [],
            "maturity_date": None
        }

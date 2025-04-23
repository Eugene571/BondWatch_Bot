# bonds_get.moex_lookup.py
import httpx
import logging
import json


async def get_bond_coupons_from_moex(isin: str):
    """Получение купонов облигации с MOEX по ISIN через bondization.json."""
    url = f"https://iss.moex.com/iss/securities/{isin}/bondization.json"

    try:
        logging.info(f"🔄 Отправка запроса к MOEX для ISIN {isin} по URL: {url}")

        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        # Логируем весь JSON-ответ (можно частично, если слишком много)
        logging.info(f"📦 Ответ от MOEX для {isin}: {json.dumps(data, indent=2, ensure_ascii=False)[:3000]}...")

        coupons_metadata = data.get("coupons", {}).get("columns", [])
        coupons_data = data.get("coupons", {}).get("data", [])

        # Безопасное определение индексов для нужных данных
        try:
            idx_coupondate = coupons_metadata.index("coupondate")  # Дата купона
            idx_value = coupons_metadata.index("value")  # Сумма купона
            idx_percent = coupons_metadata.index("valueprc")  # Процент купона
        except ValueError as e:
            logging.error(f"❌ Не найдены нужные поля в bondization.json для {isin}: {e}")
            return []

        coupons = []
        for row in coupons_data:
            coupon_date = row[idx_coupondate]
            coupon_value = row[idx_value] or 0
            coupon_percent = row[idx_percent] or 0

            if not coupon_date:
                logging.warning(f"⚠️ Пропущена строка с отсутствующей датой купона для {isin}")
                continue  # Пропускаем строки без даты

            coupons.append({
                "couponDate": str(coupon_date),  # Преобразуем в строку
                "couponValue": coupon_value,
                "couponPercent": coupon_percent,
                "type": "COUPON"
            })

        logging.info(f"📈 Найдено {len(coupons)} купонов для {isin}")
        return coupons

    except Exception as e:
        logging.error(f"❌ Ошибка при получении купонов с МОЕКС для {isin}: {e}")
        return []


async def get_bond_amortizations_from_moex(isin: str):
    """Получение амортизаций и погашения облигации с MOEX по ISIN через bondization.json."""
    url = f"https://iss.moex.com/iss/securities/{isin}/bondization.json"
    try:
        logging.info(f"🔄 Запрос амортизаций к MOEX для ISIN {isin} по URL: {url}")
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            logging.debug(f"📦 Ответ от MOEX (амортизации) для {isin}: {json.dumps(data.get('amortizations'), indent=2, ensure_ascii=False)[:500]}...")

        return data.get("amortizations", {"columns": [], "data": []})

    except Exception as e:
        logging.error(f"❌ Ошибка при получении амортизаций с МОЕКС для {isin}: {e}")
        return {"columns": [], "data": []}
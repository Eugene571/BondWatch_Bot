# bonds_get.moex_lookup.py
import asyncio

import aiohttp
import httpx
import logging
from datetime import datetime, timedelta, date
from typing import List, Optional, Dict


async def get_bondization_data_from_moex(isin: str) -> dict:
    """
    Получение данных о купонах, амортизациях и офертах с MOEX.
    Возвращает словарь:
    {
        "isin": str,
        "coupons": List[dict],
        "amortizations": List[dict],
        "offers": List[dict],
        "maturity_date": Optional[date],
        "next_offer_date": Optional[date]
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
            "offers": [],
            "maturity_date": None,
            "next_offer_date": None
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

        # Проверка наличия будущих купонов
        today = datetime.utcnow().date()
        future_coupons = [
            c for c in result["coupons"]
            if datetime.strptime(c["couponDate"], "%Y-%m-%d").date() >= today
        ]

        # Фоллбэк при отсутствии будущих купонов
        if not future_coupons:
            logging.warning(f"⚠️ Будущие купоны не найдены, запуск фоллбэка для {isin}")
            try:
                from bonds_get.moex_lookup import get_all_bondization_data
                fallback_data = await get_all_bondization_data(isin)

                # Объединение данных
                combined_coupons = {c["couponDate"]: c for c in result["coupons"]}
                for coupon in fallback_data.get("coupons", []):
                    if coupon["couponDate"] not in combined_coupons:
                        combined_coupons[coupon["couponDate"]] = coupon
                result["coupons"] = list(combined_coupons.values())

                # Обновление других полей при необходимости
                if not result["amortizations"]:
                    result["amortizations"] = fallback_data.get("amortizations", [])
                if not result["next_offer_date"]:
                    result["next_offer_date"] = fallback_data.get("next_offer_date")
                if not result["maturity_date"]:
                    result["maturity_date"] = fallback_data.get("maturity_date")

                logging.info(f"🔄 Фоллбэк добавил {len(fallback_data['coupons'])} купонов")

            except Exception as e:
                logging.error(f"❌ Ошибка фоллбэка: {e}")

        # Обработка амортизаций
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

        # Обработка оферт
        offers_meta = data.get("offers", {}).get("columns", [])
        offers_data = data.get("offers", {}).get("data", [])

        try:
            idx_offerdate = offers_meta.index("offerdate")
            idx_offertype = offers_meta.index("offertype")
        except ValueError as e:
            logging.warning(f"⚠️ Не найдены поля оферт для {isin}: {e}")
            idx_offerdate = idx_offertype = -1

        valid_offers = []
        today = datetime.utcnow().date()

        for row in offers_data:
            if idx_offerdate == -1:
                break

            offer_type = row[idx_offertype] if idx_offertype != -1 else ""
            if "отмен" in offer_type.lower():
                continue

            offer_date_str = row[idx_offerdate]
            if not offer_date_str:
                continue

            try:
                offer_date = datetime.strptime(offer_date_str, "%Y-%m-%d").date()
                if offer_date > today:
                    valid_offers.append(offer_date)
                    result["offers"].append({
                        "offer_date": offer_date_str,
                        "type": offer_type,
                        "status": "UPCOMING"
                    })
            except Exception as e:
                logging.error(f"Ошибка парсинга даты оферты {offer_date_str}: {e}")

        # Обновление результатов после фоллбэка
        if valid_offers:
            result["next_offer_date"] = min(valid_offers)
            logging.info(f"🎯 Ближайшая оферта: {result['next_offer_date']}")

        if maturity_candidate_dates:
            try:
                parsed_dates = [
                    datetime.strptime(str(d), "%Y-%m-%d").date()
                    for d in maturity_candidate_dates
                ]
                result["maturity_date"] = max(parsed_dates)
                logging.info(f"🏁 Дата погашения: {result['maturity_date']}")
            except Exception as e:
                logging.warning(f"⚠️ Ошибка при парсинге дат погашения: {e}")

        return result

    except Exception as e:
        logging.error(f"❌ Ошибка при получении данных для {isin}: {e}")
        return {
            "isin": isin,
            "coupons": [],
            "amortizations": [],
            "offers": [],
            "maturity_date": None,
            "next_offer_date": None
        }

async def get_all_bondization_data(isin: str) -> dict:
    """
    Получает полные данные по облигации с Мосбиржи с учетом пагинации.
    Возвращает словарь:
    {
        "coupons": List[dict],
        "amortizations": List[dict],
        "offers": List[dict],
        "maturity_date": Optional[date],
        "next_offer_date": Optional[date]
    }
    """
    base_url = f"https://iss.moex.com/iss/securities/{isin}/bondization.json"
    result = {
        "coupons": [],
        "amortizations": [],
        "offers": [],
        "maturity_date": None,
        "next_offer_date": None
    }

    async with aiohttp.ClientSession() as session:
        # Первый запрос для получения метаданных
        async with session.get(base_url) as response:
            data = await response.json()
            coupons_meta = data.get("coupons", {}).get("columns", [])
            amort_meta = data.get("amortizations", {}).get("columns", [])
            offers_meta = data.get("offers", {}).get("columns", [])

            # Определяем индексы полей
            try:
                coupon_date_idx = coupons_meta.index("coupondate")
                coupon_value_idx = coupons_meta.index("value")
                coupon_percent_idx = coupons_meta.index("valueprc")
            except ValueError:
                coupon_date_idx = coupon_value_idx = coupon_percent_idx = -1

            try:
                amort_date_idx = amort_meta.index("amortdate")
                amort_value_idx = amort_meta.index("value")
                amort_source_idx = amort_meta.index("data_source")
            except ValueError:
                amort_date_idx = amort_value_idx = amort_source_idx = -1

            try:
                offer_date_idx = offers_meta.index("offerdate")
                offer_type_idx = offers_meta.index("offertype")
            except ValueError:
                offer_date_idx = offer_type_idx = -1

        # Пагинация
        start = 0
        page_size = 20  # MOEX возвращает по 100 элементов на страницу
        today = datetime.now().date()

        while True:
            url = f"{base_url}?start={start}"
            try:
                async with session.get(url, timeout=10) as response:
                    data = await response.json()

                    # Обработка купонов
                    coupons_data = data.get("coupons", {}).get("data", [])
                    for row in coupons_data:
                        if coupon_date_idx == -1: continue
                        try:
                            coupon_date = datetime.strptime(
                                str(row[coupon_date_idx]), "%Y-%m-%d"
                            ).date()
                            result["coupons"].append({
                                "couponDate": row[coupon_date_idx],
                                "couponValue": row[coupon_value_idx],
                                "couponPercent": row[coupon_percent_idx],
                                "type": "COUPON"
                            })
                        except Exception as e:
                            logging.warning(f"Ошибка обработки купона: {e}")

                    # Обработка амортизаций
                    amort_data = data.get("amortizations", {}).get("data", [])
                    for row in amort_data:
                        if amort_date_idx == -1: continue
                        try:
                            result["amortizations"].append({
                                "amortDate": row[amort_date_idx],
                                "amortValue": row[amort_value_idx],
                                "dataSource": row[amort_source_idx],
                                "type": "AMORTIZATION"
                            })
                        except Exception as e:
                            logging.warning(f"Ошибка обработки амортизации: {e}")

                    # Обработка оферт (только из первой страницы)
                    if start == 0:
                        offers_data = data.get("offers", {}).get("data", [])
                        valid_offers = []
                        for row in offers_data:
                            try:
                                offer_date = datetime.strptime(
                                    row[offer_date_idx], "%Y-%m-%d"
                                ).date()
                                if offer_date > today:
                                    valid_offers.append(offer_date)
                            except Exception as e:
                                logging.warning(f"Ошибка обработки оферты: {e}")

                        if valid_offers:
                            result["next_offer_date"] = min(valid_offers)

                    # Проверка на последнюю страницу
                    if len(coupons_data) < page_size:
                        break

                    start += page_size

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logging.error(f"Ошибка запроса: {e}")
                break

    # Сортировка и обработка maturity_date
    if result["amortizations"]:
        try:
            maturity_dates = [
                datetime.strptime(a["amortDate"], "%Y-%m-%d").date()
                for a in result["amortizations"]
            ]
            result["maturity_date"] = max(maturity_dates)
        except Exception as e:
            logging.warning(f"Ошибка определения даты погашения: {e}")

    # Фильтрация будущих купонов
    result["coupons"] = [
        c for c in result["coupons"]
        if datetime.strptime(c["couponDate"], "%Y-%m-%d").date() >= today
    ]
    result["coupons"].sort(key=lambda x: x["couponDate"])

    return result

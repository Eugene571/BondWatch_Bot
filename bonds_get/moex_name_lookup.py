# database.moex_name_lookup.py
import httpx
import logging


async def get_bond_name_from_moex(isin: str) -> str | None:
    """
    Получает название облигации с MOEX по ISIN.
    """
    url = f"https://iss.moex.com/iss/securities/{isin}.json"

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()

        # Логируем весь ответ от MOEX для диагностики
        logging.info(f"Ответ MOEX для ISIN {isin}: {data}")

        # Пробуем достать из блока "description" -> "data"
        description_data = data.get("description", {}).get("data", [])
        for row in description_data:
            if row[0] == "NAME":
                return row[2]  # Название будет в третьем элементе (индекс 2)

        # Альтернатива: пробуем секцию "securities"
        securities_data = data.get("securities", {}).get("data", [])
        if securities_data and len(securities_data[0]) > 2:
            return securities_data[0][2]

    except Exception as e:
        logging.warning(f"⚠️ Не удалось получить название с MOEX для {isin}: {e}")

    return None

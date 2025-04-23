# bonds_get.bond_utils.py
import logging
from typing import Optional

from bonds_get.bond_update import get_next_coupon
from bonds_get.moex_lookup import get_bond_coupons_from_moex
from sqlalchemy.orm import Session
from datetime import datetime

from database.db import BondsDatabase


def process_amortizations(events: dict, current_date: Optional[datetime] = None) -> tuple | None:
    """
    Обрабатывает события амортизаций и погашений, выбирая ближайшее после текущей даты.
    """
    if current_date is None:
        current_date = datetime.now()

    logging.info(f"Обрабатываем амортизации и погашения для даты: {current_date}")

    future_amorts = []
    maturity_date = None

    columns = events.get("columns", [])
    logging.info(f"Колонки: {columns}")

    for row in events.get("data", []):
        event = dict(zip(columns, row))  # <-- преобразование списка в словарь
        logging.info(f"Обрабатываем событие: {event}")

        amort_date = event.get("amortdate")
        if amort_date:
            parsed_date = datetime.strptime(amort_date, "%Y-%m-%d").date()
            logging.info(f"Дата амортизации: {parsed_date}")

            if event.get("data_source") == "maturity":
                maturity_date = parsed_date
                logging.info(f"Дата погашения: {maturity_date}")

            if parsed_date >= current_date.date():
                future_amorts.append((parsed_date, event.get("value"), event.get("data_source")))
                logging.info(f"Добавлено в будущее амортизации: {parsed_date}, {event.get('value')}")

    nearest_amort = min(future_amorts, key=lambda x: x[0]) if future_amorts else None

    if nearest_amort:
        logging.info(f"Ближайшая амортизация: {nearest_amort[0]}, значение: {nearest_amort[1]}")
    else:
        logging.info("Не найдено ближайших амортизаций.")

    return nearest_amort, maturity_date


def process_offers(events):
    """
    Возвращает все доступные оферты (без фильтрации дат).

    :param events: Объект событий оферт
    :return: Список доступных оферт
    """
    offers = events['data']
    result = []
    for event in offers:
        offer_date = event.get('offerdate')
        price = event.get('price')
        result.append({
            'offer_date': offer_date,
            'price': price or 'неизвестно'
        })
    return result


async def save_bond_events(session: Session, tg_user_id: int, events):
    """
    Сохраняет информацию о ближайших значащих событиях облигации в БД, используя данные с MOEX.
    Поддерживает как dict (одна облигация), так и list (несколько).
    """
    current_date = datetime.now()

    # Логируем данные о событиях
    logging.info(f"Начало сохранения событий для пользователя {tg_user_id}. Данные: {events}")

    # Если events — это список, обработаем каждый элемент рекурсивно
    if isinstance(events, list):
        for item in events:
            await save_bond_events(session, tg_user_id, item)
        return

    # Проверка, что мы действительно работаем со словарём
    if not isinstance(events, dict):
        logging.warning(f"⚠️ Некорректный формат данных в save_bond_events: {events}")
        return

    isin = events.get("isin")
    name = events.get("name")

    logging.info(f"Обработанные данные: ISIN = {isin}, Name = {name}")

    if not isin:
        logging.warning("⚠️ Пропущен ISIN, невозможно сохранить данные по облигации.")
        return

    bond = session.query(BondsDatabase).filter_by(isin=isin).first()

    if not bond:
        logging.info(f"Облигация с ISIN {isin} не найдена в базе. Создаём новую.")
        bond = BondsDatabase(
            isin=isin,
            name=name,
            added_at=datetime.utcnow(),
        )
        session.add(bond)
        session.flush()

    # Обновляем купон через функцию
    await get_next_coupon(isin, None, bond, session)

    # Обработка амортизаций: поддержка двух вариантов формата
    if "columns" in events and "data" in events:
        amortizations = events
        logging.info("Обрабатываем амортизации из событий: 'columns' и 'data'.")
    else:
        amortizations = events.get("amortizations", {})
        logging.info("Обрабатываем амортизации из другого источника.")

    next_amort_event, maturity_date = process_amortizations(amortizations, current_date)

    if next_amort_event:
        bond.amortization_date = next_amort_event[0]
        bond.amortization_value = next_amort_event[1]
        logging.info(f"Обновлены данные по амортизации: {next_amort_event[0]}, {next_amort_event[1]}")

    if not bond.maturity_date and maturity_date:
        bond.maturity_date = maturity_date
        logging.info(f"Обновлена дата погашения: {maturity_date}")

    bond.last_updated = datetime.utcnow()
    session.commit()
    logging.info(f"Данные по облигации с ISIN {isin} успешно сохранены в базе.")


def format_bond_info(session: Session, tg_user_id: int, isin: str):
    """
    Формирует информативное сообщение о состоянии облигации на основе данных из БД.

    :param session: Сеанс SQLAlchemy
    :param tg_user_id: Telegram ID пользователя
    :param isin: Идентификатор облигации (ISIN)
    :return: Строка с информацией о событии облигации
    """
    bond = (
        session.query(BondsDatabase)
        .filter_by(user_id=tg_user_id, isin=isin)
        .first()
    )

    if bond:
        message = f"📊 Информация по облигации {isin}:\n\n"
        if bond.next_coupon_date:
            message += f"📝 Следующий купон:\n- Дата: {bond.next_coupon_date}\n- Размер купона: {bond.next_coupon_value:.2f} руб.\n"
        else:
            message += "Следующий купон отсутствует или прошёл.\n"
    else:
        message = f"Нет данных по облигации {isin}.\n"

    return message
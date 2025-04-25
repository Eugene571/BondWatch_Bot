# bonds_get.bond_utils.py
import logging
from typing import Optional

from bonds_get.bond_update import get_next_coupon
from bonds_get.moex_lookup import get_bondization_data_from_moex
from sqlalchemy.orm import Session
from datetime import datetime

from database.db import BondsDatabase

logger = logging.getLogger("bonds_get.bond_utils")  # Логгер с уникальным именем


def process_amortizations(events: list | dict, current_date: Optional[datetime] = None) -> tuple | None:
    """
    Обрабатывает амортизации и дату погашения. Принимает как список словарей, так и events['columns' + 'data'] формат.
    Возвращает ближайшую амортизацию и дату погашения (если найдена).
    """
    if current_date is None:
        current_date = datetime.now()

    logger.info(f"Обрабатываем амортизации для даты: {current_date}")

    future_amorts = []
    maturity_date = None

    # Обработка формата {"columns": [...], "data": [...]}
    if isinstance(events, list) and all("amortDate" in e for e in events):
        # Формат списка словарей (как в логе выше)
        for event in events:
            try:
                amort_date = datetime.strptime(event.get("amortDate"), "%Y-%m-%d").date()
                amort_value = event.get("amortValue")
                event_type = event.get("type")

                if amort_date >= current_date.date():
                    future_amorts.append((amort_date, amort_value, event_type))
                    logging.info(f"Будущая амортизация: {amort_date}, сумма: {amort_value}, тип: {event_type}")
            except Exception as e:
                logging.error(f"Ошибка при обработке амортизации: {event} — {e}")

    elif isinstance(events, dict) and "columns" in events and "data" in events:
        columns = events.get("columns", [])
        for row in events.get("data", []):
            event = dict(zip(columns, row))
            try:
                amort_date = event.get("amortdate")
                if amort_date:
                    parsed_date = datetime.strptime(amort_date, "%Y-%m-%d").date()
                    if event.get("data_source") == "maturity":
                        maturity_date = parsed_date
                        logger.info(f"Дата погашения установлена: {maturity_date}")
                    elif parsed_date >= current_date.date():
                        future_amorts.append((parsed_date, event.get("value_rub"), event.get("data_source")))
            except Exception as e:
                logging.error(f"Ошибка при разборе строки амортизации: {event} — {e}")
    else:
        logging.warning("Формат амортизаций не распознан. Пропускаем обработку.")

    nearest_amort = min(future_amorts, key=lambda x: x[0]) if future_amorts else None

    if nearest_amort:
        logging.info(f"Ближайшая амортизация: {nearest_amort[0]}, сумма: {nearest_amort[1]}")
    else:
        logging.info("Будущие амортизации не найдены.")

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

    logging.info(f"Начало сохранения событий для пользователя {tg_user_id}. Данные: {events}")

    if isinstance(events, list):
        for item in events:
            await save_bond_events(session, tg_user_id, item)
        return

    if not isinstance(events, dict):
        logging.warning(f"⚠️ Некорректный формат данных в save_bond_events: {events}")
        return

    if not events.get("isin"):
        isin_from_args = events.get("secid") or events.get("SECID") or events.get("bond_isin")
        if isin_from_args:
            events["isin"] = isin_from_args
            logging.info(f"Добавлен ISIN из альтернативного поля: {isin_from_args}")
        else:
            logging.warning("⚠️ Пропущен ISIN, невозможно сохранить данные по облигации.")
            return

    isin = events.get("isin")
    name = events.get("name")

    logging.info(f"Обработанные данные: ISIN = {isin}, Name = {name}")

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

    await get_next_coupon(isin, None, bond, session)

    # Устойчивое определение амортизаций
    amortizations = []

    if "amortizations" in events and isinstance(events["amortizations"], list):
        amortizations = events["amortizations"]
        logging.info("Амортизации получены в виде списка словарей.")
    elif "columns" in events and "data" in events:
        amortizations = events
        logging.info("Амортизации получены в виде таблицы с columns + data.")
    else:
        logging.info("Амортизации не найдены или формат не поддерживается.")

    next_amort_event, maturity_date = process_amortizations(amortizations, current_date)

    if next_amort_event:
        logging.info(f"Следующая амортизация: {next_amort_event[0]}, {next_amort_event[1]}")
        bond.amortization_date = next_amort_event[0]
        bond.amortization_value = next_amort_event[1]
        logging.info(f"Обновлены данные по амортизации: {next_amort_event[0]}, {next_amort_event[1]}")
    else:
        logging.info("Нет доступных амортизаций для обновления")

    if not bond.maturity_date and maturity_date:
        bond.maturity_date = maturity_date
        logger.info(f"Обновлена дата погашения: {maturity_date}")

    bond.last_updated = datetime.utcnow()
    session.commit()
    bond = session.query(BondsDatabase).filter_by(isin=isin).first()

    logger.debug(f"Дата погашения после коммита: {bond.maturity_date}")
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

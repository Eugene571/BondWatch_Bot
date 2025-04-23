# bonds_get.bond_utils.py
from typing import Optional

from bonds_get.bond_update import get_next_coupon
from bonds_get.moex_lookup import get_bond_coupons_from_moex
from sqlalchemy.orm import Session
from datetime import datetime

from database.db import BondsDatabase


def process_amortizations(events: dict, current_date: Optional[datetime] = None) -> tuple | None:
    """
    Обрабатывает события амортизаций и погашений, выбирая ближайшее после текущей даты.

    :param events: словарь событий, полученный от API
    :param current_date: текущая дата (datetime.datetime объект)
    :return: кортеж с датой и суммой ближайшего события амортизации или погашения или None
    """
    if current_date is None:
        current_date = datetime.now()

    future_amorts = []
    for event in events.get("data", []):
        amort_date = event.get("amortdate")
        if amort_date:
            parsed_date = datetime.strptime(amort_date, "%Y-%m-%d").date()
            if parsed_date >= current_date.date():
                future_amorts.append((parsed_date, event.get("value"), event.get("data_source")))

    if future_amorts:
        return min(future_amorts, key=lambda x: x[0])
    return None


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


async def save_bond_events(session: Session, tg_user_id: int, events: dict):
    """
    Сохраняет информацию о ближайших значащих событиях облигации в БД.
    """
    current_date = datetime.now()

    # Определяем облигацию в БД
    bond = session.query(BondsDatabase).filter_by(user_id=tg_user_id, isin=events["isins"][0]).first()

    if not bond:
        bond = BondsDatabase(
            user_id=tg_user_id,
            isin=events["isins"][0],
            name=events["name"],
            added_at=datetime.utcnow(),
        )
        session.add(bond)
        session.flush()  # Получаем ID, если нужно

    await get_next_coupon(events["isins"][0], None, bond, session)

    # Получаем следующую амортизацию
    next_amort_event = process_amortizations(events.get("amortizations"), current_date)
    if next_amort_event:
        bond.next_amort_date = next_amort_event[0]
        bond.next_amort_value = next_amort_event[1]

    bond.last_updated = datetime.utcnow()
    session.commit()


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

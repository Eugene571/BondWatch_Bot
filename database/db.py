# database.db.py

from sqlalchemy import create_engine, Column, Integer, String, BigInteger, ForeignKey, DateTime, Date, Float, Boolean, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from contextlib import asynccontextmanager

engine = create_async_engine(
    "postgresql+asyncpg://eugenebt:p8Wry5_#E&3@localhost/BondWatch",
    echo=True  # Для отладки, можно отключить в продакшене
)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
Base = declarative_base()


@asynccontextmanager
async def get_session():
    async with AsyncSessionLocal() as session:
        yield session


async def init_db():
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        print("Таблицы успешно созданы")
    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")
        raise


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    tg_id = Column(BigInteger, unique=True, index=True)
    full_name = Column(String(255))

    tracked_bonds = relationship("UserTracking", back_populates="user", cascade="all, delete-orphan")
    subscription = relationship("Subscription", uselist=False, back_populates="user")  # Связь с подпиской
    notifications = relationship("UserNotification", back_populates="user",
                                 cascade="all, delete-orphan")  # Добавлено свойство для уведомлений


class Subscription(Base):
    __tablename__ = "subscriptions"

    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("users.tg_id"))
    is_subscribed = Column(Boolean, default=False)
    subscription_start = Column(TIMESTAMP, nullable=True)
    subscription_end = Column(TIMESTAMP, nullable=True)
    payment_status = Column(String, nullable=True)
    payment_date = Column(TIMESTAMP, nullable=True)
    payment_amount = Column(Float, nullable=True)
    plan = Column(String, nullable=True)  # Возможные значения: basic, optimal, pro

    user = relationship("User", back_populates="subscription")


class BondsDatabase(Base):
    __tablename__ = "bonds_database"

    id = Column(Integer, primary_key=True)
    isin = Column(String, unique=True, nullable=False)
    name = Column(String)
    figi = Column(String, nullable=True)
    class_code = Column(String, nullable=True)
    ticker = Column(String, nullable=True)
    added_at = Column(TIMESTAMP, default=datetime.utcnow)
    last_updated = Column(TIMESTAMP, default=datetime.utcnow)
    next_coupon_date = Column(Date, nullable=True)
    next_coupon_value = Column(Float, nullable=True)
    offer_date = Column(Date, nullable=True)
    amortization_date = Column(Date, nullable=True)
    amortization_value = Column(Float, nullable=True)
    maturity_date = Column(Date, nullable=True)

    tracking_users = relationship("UserTracking", back_populates="bond", cascade="all, delete-orphan")


class UserTracking(Base):
    __tablename__ = "user_tracking"

    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("users.tg_id"), nullable=False)
    isin = Column(String, ForeignKey("bonds_database.isin"), nullable=False)
    quantity = Column(Integer, nullable=False, default=1)  # Новый столбец для хранения количества бумаг
    added_at = Column(TIMESTAMP, default=datetime.utcnow)

    user = relationship("User", back_populates="tracked_bonds")
    bond = relationship("BondsDatabase", back_populates="tracking_users")


class UserNotification(Base):
    __tablename__ = "user_notifications"

    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("users.tg_id"), nullable=False)
    bond_isin = Column(String, ForeignKey("bonds_database.isin"), nullable=False)
    event_type = Column(String, nullable=False)  # Тип события (coupon, maturity)
    event_date = Column(TIMESTAMP, nullable=False)  # Дата события
    is_sent = Column(Boolean, default=False)  # Статус уведомления (отправлено или нет)
    sent_at = Column(TIMESTAMP)  # Время отправки уведомления
    days_left = Column(Integer)
    user = relationship("User", back_populates="notifications")
    bond = relationship("BondsDatabase")

    def __init__(self, user_id, bond_isin, event_type, event_date, is_sent=False, sent_at=None, days_left=None):
        self.user_id = user_id
        self.bond_isin = bond_isin
        self.event_type = event_type
        self.event_date = event_date
        self.is_sent = is_sent
        self.sent_at = sent_at
        self.days_left = days_left

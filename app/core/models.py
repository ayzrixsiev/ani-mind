from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    String,
    Numeric,
    Boolean,
    TIMESTAMP,
    Text,
    JSON,
    Index,
    func,
)
from sqlalchemy.orm import relationship

from app.core.database import Base


# User
class User(Base):
    __tablename__ = "users_table"

    id = Column(Integer, primary_key=True, autoincrement=True)

    email = Column(String, nullable=False, unique=True, index=True)
    password = Column(String, nullable=False)
    role = Column(String, nullable=False, server_default="user")

    created_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # Relationships
    accounts = relationship(
        "Account",
        back_populates="owner",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    transactions = relationship(
        "Transaction",
        back_populates="owner",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    stats = relationship(
        "UserStats",
        back_populates="user",
        cascade="all, delete-orphan",
        uselist=False,
        passive_deletes=True,
    )


# Financial source
class Account(Base):

    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)

    name = Column(String, nullable=False)
    provider = Column(String, nullable=False)

    account_type = Column(String)

    currency = Column(String, default="UZS", nullable=False)

    balance = Column(Numeric(15, 2), default=0)

    owner_id = Column(
        Integer,
        ForeignKey("users_table.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    is_active = Column(Boolean, default=True)

    created_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    updated_at = Column(
        TIMESTAMP(timezone=True),
        nullable=True,
        onupdate=func.now(),
    )

    # Relationships
    owner = relationship("User", back_populates="accounts")
    transactions = relationship(
        "Transaction",
        back_populates="account",
        cascade="all, delete-orphan",
    )


class Transaction(Base):

    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)

    owner_id = Column(
        Integer,
        ForeignKey("users_table.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    account_id = Column(
        Integer,
        ForeignKey("accounts.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )

    amount = Column(Numeric(15, 2), nullable=False)

    currency = Column(String(3), default="UZS", nullable=False)

    merchant = Column(String(255))
    category = Column(String(100), index=True)
    description = Column(Text)

    raw_payload = Column(JSON, nullable=True)

    transaction_hash = Column(String(64), unique=True, index=True)

    processed = Column(Boolean, default=False, index=True)

    external_id = Column(String(255), index=True)

    # Timestamps
    created_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True,
    )

    ingested_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    updated_at = Column(
        TIMESTAMP(timezone=True),
        nullable=True,
        onupdate=func.now(),
    )

    owner = relationship("User", back_populates="transactions")
    account = relationship("Account", back_populates="transactions")

    __table_args__ = (
        Index("idx_owner_processed", "owner_id", "processed"),
        Index("idx_owner_date", "owner_id", "created_at"),
    )


class UserStats(Base):
    """
    Aggregated statistics for a user, updated during ETL load.
    """

    __tablename__ = "user_stats"

    user_id = Column(
        Integer,
        ForeignKey("users_table.id", ondelete="CASCADE"),
        primary_key=True,
    )

    total_transactions = Column(Integer, nullable=False, server_default="0")
    total_income = Column(Numeric(15, 2), nullable=False, server_default="0")
    total_expense = Column(Numeric(15, 2), nullable=False, server_default="0")
    avg_transaction_amount = Column(Numeric(15, 2), nullable=False, server_default="0")
    spent_by_category = Column(JSON, nullable=True)

    updated_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    user = relationship("User", back_populates="stats")

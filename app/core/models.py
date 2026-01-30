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
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from app.core.database import Base


# =========================
# User
# =========================
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


# =========================
# Account (data source)
# =========================
class Account(Base):
    """
    Represents a financial source:
    - bank account
    - wallet
    - fintech app
    - csv upload
    """

    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)

    name = Column(String, nullable=False)  # "Uzum Wallet", "My Bank"
    provider = Column(String, nullable=False)  # uzum/csv/manual/plaid
    currency = Column(String, default="UZS")

    owner_id = Column(
        Integer,
        ForeignKey("users_table.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    created_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # Relationships
    owner = relationship("User", back_populates="accounts")

    transactions = relationship(
        "Transaction",
        back_populates="account",
        cascade="all, delete-orphan",
    )


# =========================
# Transaction (RAW DATA LAYER)
# =========================
class Transaction(Base):
    """
    Core table of the whole system.
    This is your RAW ingestion layer.

    Everything starts here:
    CSV/API → this table → transformations → analytics
    """

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

    amount = Column(Numeric(12, 2), nullable=False)
    currency = Column(String, default="UZS")

    merchant = Column(String)
    category = Column(String)

    description = Column(Text)

    # raw original payload (for reprocessing/debugging)
    raw_payload = Column(JSON, nullable=True)

    processed = Column(Boolean, default=False, index=True)

    created_at = Column(  # actual transaction time
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    ingested_at = Column(  # when we stored it
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # Relationships
    owner = relationship("User", back_populates="transactions")
    account = relationship("Account", back_populates="transactions")

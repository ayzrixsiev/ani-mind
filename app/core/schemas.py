from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, EmailStr


# =========================
# Enums
# =========================
class UserRole(str, Enum):
    ADMIN = "admin"
    USER = "user"


class AccountProvider(str, Enum):
    CSV = "csv"
    MANUAL = "manual"
    UZUM = "Uzum"
    PAYME = "Payme"
    CLICK = "Click"


# =========================
# USER
# =========================
class UserBase(BaseModel):
    email: EmailStr
    role: UserRole = UserRole.USER


class CreateUser(UserBase):
    password: str = Field(min_length=8)
    model_config = ConfigDict(from_attributes=True)


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class UserResponse(UserBase):
    id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


# =========================
# ACCOUNT
# =========================
class AccountBase(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    provider: AccountProvider
    currency: str = "UZS"


class AccountCreate(AccountBase):
    pass


class AccountUpdate(BaseModel):
    name: Optional[str] = None
    provider: Optional[AccountProvider] = None
    currency: Optional[str] = None


class AccountResponse(AccountBase):
    id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


# =========================
# TRANSACTION
# =========================
class TransactionBase(BaseModel):
    amount: Decimal
    currency: str = "UZS"
    merchant: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None
    account_id: Optional[int] = None


class TransactionCreate(TransactionBase):
    pass


class TransactionResponse(TransactionBase):
    id: int
    processed: bool
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


# =========================
# ANALYTICS / ETL
# =========================
class UserStatsResponse(BaseModel):
    total_transactions: int
    total_income: float
    total_expense: float
    avg_transaction_amount: float
    spent_by_category: Dict[str, float] = {}
    updated_at: Optional[str] = None


class AccountSummaryResponse(BaseModel):
    account_id: int
    account_name: str
    account_type: Optional[str] = None
    currency: str
    balance: float
    provider: str
    total_transactions: int
    recent_transactions_30d: int
    last_updated: Optional[str] = None


class ApiIngestConfig(BaseModel):
    """
    Generic API ingestion config for ETL.
    """
    type: str = "generic"
    url: str
    headers: Dict[str, str] = {}
    params: Optional[Dict[str, Any]] = None
    source: Optional[str] = None


class ApiIngestRequest(BaseModel):
    account_id: int
    api_config: ApiIngestConfig

from datetime import datetime
from decimal import Decimal
from typing import Optional, List
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

from passlib.context import CryptContext
from datetime import datetime, timedelta, timezone
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from typing import Annotated
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import jwt

from app.core.database import get_db
from app.core import models
from app.core.config import settings

db_dep = Annotated[AsyncSession, Depends(get_db)]
# Hash mechanism
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# Hash the password
def hash_password(password: str):
    return pwd_context.hash(password)


# Verify the password
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: dict):
    to_encode = data.copy()

    expire_time = datetime.now(timezone.utc) + timedelta(
        minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES
    )
    to_encode.update({"exp": expire_time})

    encoded_jwt = jwt.encode(
        to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM
    )

    return encoded_jwt


# tokenUrl="profile/login" if you don't have a token yet, go to this address to get one
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="profile/login")


# Decode the token and see who is the user
async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)], db: db_dep):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        # Decode the "Gibberish"
        payload = jwt.decode(
            token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM]
        )
        user_id: int = payload.get("user_id")

        if user_id is None:
            raise credentials_exception

    # In case if token is expired
    except jwt.DecodeError:
        raise credentials_exception

    # Go to the Database and find this specific person
    query = select(models.User).where(models.User.id == user_id)
    result = await db.execute(query)
    user = result.scalars().first()

    if user is None:
        raise credentials_exception

    return user


async def validate_admin_role(
    current_user: Annotated[models.User, Depends(get_current_user)],
):
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have enough privileges (Admin only)",
        )
    # If user admin, we will return him
    return current_user

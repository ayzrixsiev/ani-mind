from typing import Annotated
from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core import schemas, models
from app.core.database import get_db
from app.core.security import verify_password, create_access_token

router = APIRouter(prefix="/profile", tags=["Authentication"])

db_dep = Annotated[AsyncSession, Depends(get_db)]


@router.post("/login", status_code=status.HTTP_200_OK)
async def verify_user(user_credentials: schemas.UserLogin, db: db_dep):
    query = select(models.User).where(models.User.email == user_credentials.email)
    result = await db.execute(query)
    db_user = result.scalars().first()

    if not db_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User does not exists"
        )

    valid_user = verify_password(user_credentials.password, db_user.password)

    if not valid_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect password"
        )
    token = create_access_token({"user_id": db_user.id, "role": db_user.role})
    return {"access_token": token, "token_type": "bearer"}

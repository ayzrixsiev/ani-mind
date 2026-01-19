import logging
from typing import Annotated
from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core import schemas, models
from app.core.database import get_db
from app.core.security import hash_password, validate_admin_role

router = APIRouter(prefix="/profile", tags=["Users"])

db_dep = Annotated[AsyncSession, Depends(get_db)]


# Add user
@router.post(
    "/signup",
    response_model=schemas.UserResponse,
    status_code=status.HTTP_201_CREATED,
)
async def sign_up(user: schemas.CreateUser, db: db_dep):
    # Validate whether a user already exists
    query = select(models.Users).where(models.Users.email == user.email)
    result = await db.execute(query)
    db_user = result.scalars().first()

    if db_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="User already exists"
        )

    # Hash the password and add new user to the db
    try:
        hashed_pwd = hash_password(user.password)
        new_user = models.Users(email=user.email, password=hashed_pwd, role=user.role)
        db.add(new_user)
        await db.commit()
        await db.refresh(new_user)
        return new_user
    except Exception as error:
        await db.rollback()
        logging.error(f"Failed to add a new user: {error}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to sign up",
        )


# Get user
@router.get("/{user_id}", response_model=schemas.UserResponse)
async def get_user(user_id: int, db: db_dep):
    query = select(models.Users).where(models.Users.id == user_id)
    result = await db.execute(query)
    db_user = result.scalars().first()

    if not db_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return db_user


@router.delete("/{user_id}")
async def delete_user(
    user_id: int,
    admin: Annotated[models.Users, Depends(validate_admin_role)],
    db: db_dep,
):
    # Prevent Admin Suicide :D
    if admin.id == user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You cannot delete your own admin account.",
        )

    # Find the User
    query = select(models.Users).where(models.Users.id == user_id)
    result = await db.execute(query)
    user_to_delete = result.scalars().first()

    if not user_to_delete:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with id: {user_id} does not exist",
        )

    # Perform Delete
    try:
        await db.delete(user_to_delete)
        await db.commit()
        return {"Result": "Successfully deleted a user"}
    except Exception as error:
        await db.rollback()
        logging.error(f"Failed to delete user {user_id}: {error}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete a user",
        )

import logging
from typing import Annotated
from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import desc, func, select

from app.core import schemas, models
from app.core.database import get_db
from app.core.security import get_current_user, hash_password, validate_admin_role

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
    query = select(models.User).where(models.User.email == user.email)
    result = await db.execute(query)
    db_user = result.scalars().first()

    if db_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="User already exists"
        )

    # Hash the password and add new user to the db
    try:
        hashed_pwd = hash_password(user.password)
        new_user = models.User(email=user.email, password=hashed_pwd, role=user.role)
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
    query = select(models.User).where(models.User.id == user_id)
    result = await db.execute(query)
    db_user = result.scalars().first()

    if not db_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return db_user


@router.get("/stats")
async def get_user_stats(
    current_user: Annotated[models.User, Depends(get_current_user)], db: db_dep
):
    stats_query = select(
        func.count(models.Anime.id).label("total_anime"),
        func.avg(models.Anime.rating).label("avg_rating"),
    ).where(models.Anime.owner_id == current_user.id)

    result = await db.execute(stats_query)
    stats = result.first()

    genre_query = (
        select(
            func.unnest(models.Anime.genres).label("genre"), func.count().label("count")
        )
        .where(models.Anime.owner_id == current_user.id)
        .group_by("genre")
        .order_by(desc("count"))
        .limit(3)
    )

    genre_result = await db.execute(genre_query)
    top_genres = [
        {"genre": row.genre, "count": row.count} for row in genre_result.all()
    ]

    total_anime = stats.total_anime or 0
    average_rating = round(float(stats.avg_rating), 2) if stats.avg_rating else 0.0

    return {
        "total_anime": total_anime,
        "average_rating": average_rating,
        "top_genres": top_genres,
    }


@router.delete("/{user_id}")
async def delete_user(
    user_id: int,
    admin: Annotated[models.User, Depends(validate_admin_role)],
    db: db_dep,
):
    # Prevent Admin Suicide :D
    if admin.id == user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You cannot delete your own admin account.",
        )

    # Find the User
    query = select(models.User).where(models.User.id == user_id)
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

import logging
from typing import List, Annotated
from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.core import schemas, models
from app.core.database import get_db
from app.core.security import get_current_user

router = APIRouter(prefix="/anime", tags=["Anime"])

# Modern Dependency Injection
db_dep = Annotated[AsyncSession, Depends(get_db)]


# Anime list
@router.get(
    "/list",
    response_model=List[schemas.AnimeResponse],
    status_code=status.HTTP_200_OK,
)
async def get_anime_list(
    current_user: Annotated[models.User, Depends(get_current_user)], db: db_dep
):
    # Get the user and load his anime list
    query = (
        select(models.User)
        .options(selectinload(models.User.anime_list))
        .where(models.User.id == current_user.id)
    )
    result = await db.execute(query)
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(404, "User not found")

    return user.anime_list


# Add anime
@router.post(
    "/list",
    response_model=schemas.AnimeResponse,
    status_code=status.HTTP_201_CREATED,
)
async def add_anime(
    anime: schemas.AnimeCreate,
    db: db_dep,
    current_user: Annotated[models.User, Depends(get_current_user)],
):
    await db.refresh(current_user, attribute_names=["anime_list"])
    try:
        new_anime_entry = models.Anime(**anime.model_dump())
        current_user.anime_list.append(new_anime_entry)
        await db.commit()  # Commit to the DB
        await db.refresh(new_anime_entry)  # Refresh to get a generated ID by DB

        return new_anime_entry

    except Exception as error:
        await db.rollback()  # Undo changes if something went wrong
        logging.error(f"Database error: {error}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to add an anime",
        )


# Update anime details
@router.patch("/list/{anime_id}")
async def update_anime_details(
    anime_id: int,
    user_targeted_anime: schemas.AnimeUpdate,
    db: db_dep,
    current_user: Annotated[models.User, Depends(get_current_user)],
):
    await db.refresh(current_user, attribute_names=["anime_list"])

    try:
        anime = next(a for a in current_user.anime_list if a.id == anime_id)
    except StopIteration:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Anime not found in your list")

    # Get the anime from a user convert it into dictionary and do not store empty fileds
    anime_dict = user_targeted_anime.model_dump(exclude_unset=True)

    for key, value in anime_dict.items():
        setattr(anime, key, value)

    try:
        db.add(anime)
        await db.commit()
        await db.refresh(anime)
        return anime
    except Exception as error:
        await db.rollback()
        logging.error(f"Failed to commit a change: {error}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Update failed"
        )


# Delete anime
@router.delete("/list/{anime_id}", status_code=status.HTTP_200_OK)
async def delete_anime(
    anime_id: int,
    db: db_dep,
    current_user: Annotated[models.User, Depends(get_current_user)],
):
    await db.refresh(current_user, attribute_names=["anime_list"])
    try:
        anime = next(a for a in current_user.anime_list if a.id == anime_id)
        current_user.anime_list.remove(anime)
        await db.commit()
        return {"message": f"Deleted anime {anime_id}"}
    except StopIteration:
        raise HTTPException(404, "Anime not found")

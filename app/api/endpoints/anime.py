import logging
from typing import List, Annotated
from fastapi import APIRouter, HTTPException, status, Depends
from httpx import get
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

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
    try:
        query = select(models.Anime).where(models.Anime.owner_id == current_user.id)
        result = await db.execute(query)
        db_list = (
            result.scalars().all()
        )  # Cast all the sql data into python object scalars() and save in the list all()

        return db_list
    except Exception as error:
        logging.error(f"Error fetching Anime List: {error}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve Anime List",
        )


# Add an anime
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
    try:
        new_anime_entry = models.Anime(**anime.model_dump(), owner_id=current_user.id)
        db.add(new_anime_entry)  # Add to the session
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
    # Find anime and store it in db_anime
    query = select(models.Anime).where(
        models.Anime.id == anime_id, models.Anime.owner_id == current_user.id
    )
    result = await db.execute(query)
    db_anime = result.scalars().first()

    if not db_anime:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Anime with id {anime_id} not found",
        )
    # Get the anime from a user convert it into dictionary and do not store empty fileds
    updated_anime_dict = user_targeted_anime.model_dump(exclude_unset=True)

    for key, value in updated_anime_dict.items():
        setattr(db_anime, key, value)
    try:
        db.add(db_anime)
        await db.commit()
        await db.refresh(db_anime)
        return db_anime
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
    # Find and save the targeted anime
    query = select(models.Anime).where(
        models.Anime.id == anime_id, models.Anime.owner_id == current_user.id
    )
    result = await db.execute(query)
    db_anime = result.scalars().first()

    if not db_anime:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Anime not found"
        )
    try:
        await db.delete(db_anime)
        await db.commit()
        return {"message": f"Successfully deleted anime with ID {anime_id}"}
    except Exception as error:
        await db.rollback()
        logging.error(f"Delete error: {error}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete an anime with id {anime_id}",
        )

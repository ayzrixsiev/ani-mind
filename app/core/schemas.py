from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, EmailStr
from typing import List, Optional
from enum import Enum


# Define the allowed statuses
class AnimeStatus(str, Enum):
    WATCHING = "Watching"
    PLANNED = "Planned"
    DROPPED = "Dropped"
    COMPLETED = "Completed"


# Define the allowed genres
class AnimeGenre(str, Enum):
    COMEDY = "Comedy"
    ROMANCE = "Romance"
    SCHOOL = "School"
    ACTION = "Action"
    SLICE_OF_LIFE = "Slice of Life"
    DRAMA = "Drama"
    FANTASY = "Fantasy"
    ADVENTURE = "Adventure"


# Define the allowed roles
class UserRole(str, Enum):
    ADMIN = "admin"
    USER = "user"


# Parent class for anime validation
class AnimeBase(BaseModel):
    title: str = Field(..., min_length=1, max_length=150)
    rating: int = Field(default=0, ge=0, le=10)
    status: AnimeStatus = Field(default=AnimeStatus.PLANNED)
    genres: List[AnimeGenre] = Field(default=[])


# Child
class AnimeCreate(AnimeBase):
    pass


# Child
class AnimeUpdate(AnimeBase):
    title: Optional[str] = Field(None, min_length=1, max_length=150)
    rating: Optional[int] = Field(None, ge=0, le=10)
    status: Optional[AnimeStatus] = None
    genres: Optional[List[AnimeGenre]] = None


# Child with it's own additional parameters
class AnimeResponse(AnimeBase):
    id: int

    # Telling Pydantic to read the data even if it is not a dict
    model_config = ConfigDict(from_attributes=True)


# Parent class for User validation
class UserBase(BaseModel):
    email: EmailStr
    role: UserRole = "user"


class CreateUser(UserBase):
    password: str = Field(
        min_length=8, description="Password must be at least 8 characters"
    )
    model_config = ConfigDict(from_attributes=True)


class UserLogin(UserBase):
    password: str = Field(
        min_length=8, description="Password must be at least 8 characters"
    )
    model_config = ConfigDict(from_attributes=True)


class UserResponse(UserBase):
    id: int
    email: str
    role: UserRole
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)

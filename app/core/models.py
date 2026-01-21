from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    String,
    Enum as SQLEnum,
    ARRAY,
    TIMESTAMP,
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.core.database import Base
from app.core import schemas


class User(Base):
    __tablename__ = "users_table"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String, nullable=False, unique=True)
    password = Column(String, nullable=False)
    role = Column(String, nullable=False, server_default="user")
    created_at = Column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    anime_list = relationship(
        "Anime",  # Target class name (string is fine)
        back_populates="owner",  # Must match the attribute name we'll add on Anime
        cascade="all, delete-orphan",  # Delete anime when removed from user list or user deleted
        passive_deletes=True,  # Helps with ON DELETE CASCADE from DB
    )


class Anime(Base):
    __tablename__ = "anime_list"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False, unique=True)
    rating = Column(Integer, default=0)
    status = Column(SQLEnum(schemas.AnimeStatus), default=schemas.AnimeStatus.PLANNED)
    genres = Column(ARRAY(String), default=[])
    owner_id = Column(
        Integer, ForeignKey("users_table.id", ondelete="CASCADE"), nullable=False
    )
    owner = relationship("User", back_populates="anime_list")

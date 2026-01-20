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
from app.core.database import Base
from app.core import schemas


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


class User(Base):
    __tablename__ = "users_table"

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String, nullable=False, unique=True)
    password = Column(String, nullable=False)
    role = Column(String, nullable=False, server_default="user")
    created_at = Column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

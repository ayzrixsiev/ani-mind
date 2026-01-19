from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from app.core.config import settings

engine = create_async_engine(settings.DATABASE_URL, echo=True)

# Talk to the DB through async sessions without refreshes after closed conn to avoid errors in async programming
AsyncSessionLocal = async_sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)


# This is the "Bridge" that gives my routes access to postgres
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


# All the models are "stored" in the Base class will be processed by the Engine
class Base(DeclarativeBase):
    pass

import uuid
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from app.core.security import create_access_token, hash_password
from app.main import app
from app.core import models
from app.core.database import Base, get_db
from app.core.config import settings

# Force to use a test db for tests
TEST_DATABASE_URL = settings.DATABASE_URL + "_test"

# Create an engine and session (workers) factory
test_engine = create_async_engine(TEST_DATABASE_URL, echo=False)
TestingSessionLocal = async_sessionmaker(
    test_engine, class_=AsyncSession, expire_on_commit=False
)


# Create a db every time test file runs and drop the db after tests are done
@pytest_asyncio.fixture(scope="session", autouse=True)
async def set_up_db():
    async with test_engine.begin() as conn:
        # Create all tables
        await conn.run_sync(Base.metadata.create_all)
    yield  # Tests happens here
    async with test_engine.begin() as conn:
        # Drop tests db once we are done
        await conn.run_sync(Base.metadata.drop_all)


# Create session and rollback once it is done
@pytest_asyncio.fixture(scope="function")
async def db_session():
    async with TestingSessionLocal() as session:
        yield session
        await session.rollback()
        await session.close()


# Client
@pytest_asyncio.fixture(scope="function")
async def client(db_session: AsyncSession):
    async def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac

    app.dependency_overrides.clear()


# User
@pytest_asyncio.fixture(scope="function")
async def test_user(db_session: AsyncSession):
    # Generate unique email for each test to avoid duplicates
    unique_email = f"test_{uuid.uuid4().hex[:8]}@gmail.com"
    hashed_pwd = hash_password("password123")  # Make sure to hash the password

    user = models.User(email=unique_email, password=hashed_pwd, role="user")

    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)
    return user


# Admin
@pytest_asyncio.fixture(scope="function")
async def test_admin(db_session: AsyncSession):
    # Generate unique email for each test
    unique_email = f"admin_{uuid.uuid4().hex[:8]}@gmail.com"
    hashed_pwd = hash_password("password123")

    user = models.User(email=unique_email, password=hashed_pwd, role="admin")

    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)
    return user


# Token for user
@pytest_asyncio.fixture(scope="function")
async def auth_headers_user(test_user):
    token = create_access_token({"user_id": test_user.id})
    return {"Authorization": f"Bearer {token}"}


# Token for admin
@pytest_asyncio.fixture(scope="function")
async def auth_headers_admin(test_admin):
    token = create_access_token({"user_id": test_admin.id})
    return {"Authorization": f"Bearer {token}"}


# Anime
@pytest_asyncio.fixture(scope="function")
async def test_anime(db_session: AsyncSession, test_user):
    anime = models.Anime(
        title=f"Test Anime {uuid.uuid4().hex[:8]}",
        rating=8,
        status="Watching",
        genres=["Action", "Adventure"],
        owner_id=test_user.id,
    )
    db_session.add(anime)
    await db_session.commit()
    await db_session.refresh(anime)
    return anime

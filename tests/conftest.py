import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.main import app
from app.core.database import Base, get_db
from app.core.config import settings

# Force to use a test db for this project
TEST_DATABSE_URL = settings.DATABASE_URL + "_test"

# Create an engine and session (workers) factory
test_engine = create_async_engine(TEST_DATABSE_URL, echo=False)
TestingSessionLocal = sessionmaker(
    test_engine, class_=AsyncSession, expire_on_commit=False
)


# Create a db every time test file runs and drop the db after tests are done
@pytest.fixture(scope="session", autouse=True)
async def set_up_db():
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
async def db_session():
    async with test_engine.connect() as connection:  # Create a pipeline to the db
        transaction = (
            await connection.begin()
        )  # Activate that pipeline, have a draft but it is not commited
        session = TestingSessionLocal(bind=connection)  # Send a worker to that pipeline

        yield session

        await session.close()
        await transaction.rollback()  # Undo eveyrthing the test did, to use fresh one for the next test


@pytest.fixture
async def client(db_session):

    def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    async with AsyncClient(  # Client simlulator that connects to the FastAPI app using ASGITransport function and sets up a test server url
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac
    app.dependency_overrides.clear()  # Clean up after we are done

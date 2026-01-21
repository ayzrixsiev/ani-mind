import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
import alembic.config
import alembic.command
from app.core.database import engine, Base
from app.api.router import api_router


def run_migrations():
    """Sync function to run migrations"""
    alembic_cfg = alembic.config.Config("alembic.ini")
    alembic.command.upgrade(alembic_cfg, "head")


# Close the engine once everything is done and close all the sessions
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Apply any pending migrations automatically when the app starts
    try:
        await asyncio.to_thread(run_migrations)
        print("Migrations applied successfully (or already up-to-date)")
    except Exception as e:
        print(f"Migration error during startup: {e}")

    yield
    await engine.dispose()


app = FastAPI(title="Anime ML Analytics API", lifespan=lifespan)

# Include the master router containing all our endpoints
app.include_router(api_router)


@app.get("/")
async def root():
    return {"message": "Welcome to the Anime Recommendation API"}

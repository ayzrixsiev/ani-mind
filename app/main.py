from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.core.database import engine, Base
from app.api.router import api_router


# Close the engine once everything is done and close all the sessions
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await engine.dispose()


app = FastAPI(title="Anime ML Analytics API", lifespan=lifespan)

# Include the master router containing all our endpoints
app.include_router(api_router)


@app.get("/")
async def root():
    return {"message": "Welcome to the Anime Recommendation API"}

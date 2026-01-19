from fastapi import APIRouter
from app.api.endpoints import anime, auth, users

api_router = APIRouter()

# Combine all sub-routers into one
api_router.include_router(anime.router)
api_router.include_router(users.router)
api_router.include_router(auth.router)

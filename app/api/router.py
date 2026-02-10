from fastapi import APIRouter
from app.api.endpoints import accounts, auth, transactions, etl, analytics

api_router = APIRouter()

# Combine all sub-routers into one
api_router.include_router(accounts.router)
api_router.include_router(transactions.router)
api_router.include_router(auth.router)
api_router.include_router(etl.router)
api_router.include_router(analytics.router)

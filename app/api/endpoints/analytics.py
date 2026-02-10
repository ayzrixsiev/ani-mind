from datetime import date, timedelta
from typing import Annotated, List

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core import models, schemas
from app.core.database import get_db
from app.core.security import get_current_user
from app.core.etl import aggregate, load

router = APIRouter(prefix="/analytics", tags=["Analytics"])

db_dep = Annotated[AsyncSession, Depends(get_db)]
user_dep = Annotated[models.User, Depends(get_current_user)]


@router.get("/dashboard")
async def get_dashboard(current_user: user_dep, db: db_dep):
    """Return the full analytics dashboard for the current user."""
    return await aggregate.get_financial_dashboard(current_user.id, db)


@router.get("/spending-by-category")
async def spending_by_category(
    current_user: user_dep,
    db: db_dep,
    start_date: date,
    end_date: date,
):
    """
    Return spending totals grouped by category for a date range.
    """
    spending = await aggregate.get_user_spending_by_category(
        user_id=current_user.id, start_date=start_date, end_date=end_date, db=db
    )
    return {"spending_by_category": spending}


@router.get("/budget-recommendations")
async def budget_recommendations(current_user: user_dep, db: db_dep):
    """
    Return budget recommendations based on the last ~3 months of spending.
    """
    end_date = date.today()
    start_date = end_date.replace(day=1) - timedelta(days=90)
    recommendations = await aggregate.create_budget_recommendations(
        user_id=current_user.id, start_date=start_date, end_date=end_date, db=db
    )
    return {"budget_recommendations": recommendations}


@router.get("/user-stats", response_model=schemas.UserStatsResponse)
async def user_stats(current_user: user_dep, db: db_dep):
    """
    Return (and refresh) cached user stats from the load step.
    """
    return await load.update_user_stats(current_user.id, db)


@router.get(
    "/account-summary", response_model=List[schemas.AccountSummaryResponse]
)
async def account_summary(current_user: user_dep, db: db_dep):
    """Return per-account summary metrics for the current user."""
    return await load.get_user_account_summary(current_user.id, db)

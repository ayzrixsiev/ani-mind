from typing import Annotated, Optional

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core import models, schemas
from app.core.database import get_db
from app.core.security import get_current_user, validate_admin_role
from app.core.etl import pipeline

router = APIRouter(prefix="/etl", tags=["ETL"])

db_dep = Annotated[AsyncSession, Depends(get_db)]
user_dep = Annotated[models.User, Depends(get_current_user)]


@router.post("/run-csv")
async def run_csv_pipeline(
    current_user: user_dep,
    db: db_dep,
    file: UploadFile = File(...),
    account_id: Optional[int] = None,
):
    """
    Run the full ETL pipeline with a CSV upload:
    ingest -> transform -> load -> aggregate.
    """
    file_content = await file.read()
    if not file_content:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, "Uploaded file is empty or unreadable"
        )

    result = await pipeline.run_complete_etl_pipeline(
        user_id=current_user.id,
        db=db,
        account_id=account_id,
        file_content=file_content,
    )
    return result


@router.post("/run-api")
async def run_api_pipeline(
    current_user: user_dep,
    payload: schemas.ApiIngestRequest,
    db: db_dep,
):
    """
    Run the full ETL pipeline with an API source.
    Provide URL/headers/params in the request body.
    """
    api_config = payload.api_config.model_dump()
    result = await pipeline.run_complete_etl_pipeline(
        user_id=current_user.id,
        db=db,
        account_id=payload.account_id,
        api_config=api_config,
    )
    return result


@router.post("/transform-only")
async def transform_only(current_user: user_dep, db: db_dep):
    """Process all unprocessed transactions for the user."""
    return await pipeline.run_transform_pipeline(current_user.id, db)


@router.post("/load-only")
async def load_only(current_user: user_dep, db: db_dep):
    """Update balances + stats after transform."""
    return await pipeline.run_load_pipeline(current_user.id, db)


@router.post("/aggregate-only")
async def aggregate_only(current_user: user_dep, db: db_dep):
    """Generate analytics/insights from processed data."""
    return await pipeline.run_aggregate_pipeline(current_user.id, db)


@router.get("/status")
async def get_status(current_user: user_dep, db: db_dep):
    """Get current pipeline status for this user."""
    return await pipeline.get_pipeline_status(current_user.id, db)


@router.get("/health")
async def health_check(
    db: db_dep,
    current_user: Annotated[models.User, Depends(validate_admin_role)],
):
    """Admin-only system health check for the ETL stack."""
    return await pipeline.get_pipeline_health_check(db)

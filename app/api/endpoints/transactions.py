# app/core/routers/transactions.py
import logging
from typing import List, Annotated
from fastapi import APIRouter, HTTPException, status, Depends, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc

from app.core import schemas, models
from app.core.database import get_db
from app.core.security import get_current_user
from app.core.etl.ingest import ingest_from_csv

router = APIRouter(prefix="/transactions", tags=["Transactions"])


# Default parameter must be first in FastAPI
def db_dep():
    return Depends(get_db)


@router.post(
    "", response_model=schemas.TransactionResponse, status_code=status.HTTP_201_CREATED
)
async def ingest_transaction(
    current_user: Annotated[models.User, Depends(get_current_user)],
    transaction: schemas.TransactionCreate,
    db: AsyncSession = db_dep(),
):
    try:
        user_id = getattr(current_user, "id", None)
        new_tx = models.Transaction(**transaction.model_dump(), owner_id=user_id)
        db.add(new_tx)
        await db.commit()
        await db.refresh(new_tx)
        return new_tx
    except Exception as error:
        await db.rollback()
        logging.error(f"Failed to ingest transaction: {error}")
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "Failed to add transaction"
        )


@router.post("/upload-csv")
async def upload_transactions_csv(
    current_user: Annotated[models.User, Depends(get_current_user)],
    file: UploadFile = File(...),
    db: AsyncSession = db_dep(),
):
    """
    Upload CSV of transactions; file processing happens in app.core.etl.ingest.
    Note: this only ingests raw transactions. For the full ETL flow, use /etl/run-csv.
    """
    try:
        content = await file.read()
        # Pass content (bytes) to your ETL helper that parses CSV and inserts into raw table
        user_id = getattr(current_user, "id", None)
        if user_id is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "User not found")

        result = await ingest_from_csv(content, user_id, None, db)
        inserted = result.get("saved", 0)
        return {"inserted": inserted}
    except Exception as error:
        logging.error(f"CSV upload failed: {error}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Upload failed")


@router.get("/raw", response_model=List[schemas.TransactionResponse])
async def get_raw_transactions(
    current_user: Annotated[models.User, Depends(get_current_user)],
    db: AsyncSession = db_dep(),
    limit: int = 100,
):
    user_id = getattr(current_user, "id", None)
    query = (
        select(models.Transaction)
        .where(models.Transaction.owner_id == user_id)
        .order_by(desc(models.Transaction.created_at))
        .limit(limit)
    )
    result = await db.execute(query)
    txs = result.scalars().all()
    return txs

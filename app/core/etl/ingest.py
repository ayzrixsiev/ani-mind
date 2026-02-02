# app/etl/ingest.py
"""
INGEST MODULE - Get raw transaction data from different sources

Purpose:
    1. Read CSV files (bank exports, manual uploads)
    2. Fetch from APIs (Payme, Click, future: Plaid)
    3. Convert to standard format
    4. Save RAW data to database (no cleaning yet - that's transform.py's job)

Data Flow:
    CSV/API → read_data() → to_standard_format() → save_to_db() → raw_payload
"""

import csv
import io
import hashlib
from typing import List, Dict, Any, Optional
from datetime import datetime

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.core import models


# ============================================================================
# STEP 1: READ DATA FROM SOURCE
# ============================================================================


def read_csv_file(file_content: bytes) -> List[Dict[str, str]]:
    """
    Read CSV file and return list of dictionaries.

    Handles:
        - UTF-8 encoding (Uzbek/Russian text)
        - Different CSV formats (comma, semicolon separated)
        - Empty rows

    Args:
        file_content: Raw CSV bytes from file upload

    Returns:
        List of dicts, each dict = one transaction row

    Example:
        Input CSV:
            date,amount,merchant
            2025-01-15,-50000,Starbucks
            2025-01-16,1000000,Salary

        Output:
            [
                {"date": "2025-01-15", "amount": "-50000", "merchant": "Starbucks"},
                {"date": "2025-01-16", "amount": "1000000", "merchant": "Salary"}
            ]
    """
    # Try UTF-8 first (most common)
    try:
        text = file_content.decode("utf-8")
    except UnicodeDecodeError:
        # Fallback for old Windows CSV files
        text = file_content.decode("windows-1251")

    # Parse CSV
    reader = csv.DictReader(io.StringIO(text))

    # Filter out empty rows
    rows = [row for row in reader if any(row.values())]

    print(f"Read {len(rows)} rows from CSV")
    return rows


async def fetch_from_api(
    api_url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch transactions from external API.

    Supports:
        - Payme API
        - Click API
        - Future: Plaid, bank APIs

    Args:
        api_url: API endpoint
        headers: HTTP headers (auth tokens)
        params: Query parameters (date range, filters)

    Returns:
        List of transaction dicts from API
    """
    print(f"Fetching from API: {api_url}")

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(api_url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()

        # Handle different API response formats
        if isinstance(data, list):
            # Direct array: [{"id": 1, ...}, {"id": 2, ...}]
            transactions = data
        elif isinstance(data, dict):
            # Wrapped format: {"data": [...]} or {"transactions": [...]}
            transactions = (
                data.get("data")
                or data.get("transactions")
                or data.get("result", {}).get("transactions")  # Payme format
                or []
            )
        else:
            raise ValueError(f"Unexpected API format: {type(data)}")

    print(f"Fetched {len(transactions)} transactions")
    return transactions


# ============================================================================
# STEP 2: CONVERT TO STANDARD FORMAT
# ============================================================================


def generate_hash(data: Dict[str, Any]) -> str:
    """
    Generate unique hash for transaction deduplication.

    Uses: date + amount + merchant
    This prevents duplicate uploads of same transaction.

    Example:
        Transaction: 2025-01-15, -50000, Starbucks
        Hash: "a3f8b2c..." (SHA256)
    """
    # Create string from key fields
    key = f"{data.get('date')}|{data.get('amount')}|{data.get('merchant')}"

    # Generate hash
    return hashlib.sha256(key.encode()).hexdigest()


def to_standard_format(raw_row: Dict[str, Any], source: str = "csv") -> Dict[str, Any]:
    """
    Convert raw data to our standard format.

    Why? Different sources use different field names:
        - CSV might have "Date" or "date" or "Дата"
        - Payme uses "time" (unix timestamp)
        - Click uses "created_datetime"

    We need ONE standard format for the rest of the pipeline.

    Args:
        raw_row: Original data from CSV/API
        source: Where it came from ("csv", "payme", "click")

    Returns:
        Standardized dict ready for database
    """
    # Extract date (try common field names)
    date = (
        raw_row.get("date")
        or raw_row.get("Date")
        or raw_row.get("created_at")
        or raw_row.get("timestamp")
        or raw_row.get("Дата")  # Russian keyboards
    )

    # For API sources, handle timestamps
    if source == "payme" and "time" in raw_row:
        # Payme uses unix timestamp in milliseconds
        timestamp = int(raw_row["time"]) / 1000
        date = datetime.fromtimestamp(timestamp).isoformat()

    if source == "click" and "created_datetime" in raw_row:
        date = raw_row["created_datetime"]

    # Extract amount (different formats)
    amount = (
        raw_row.get("amount")
        or raw_row.get("Amount")
        or raw_row.get("Сумма")
        or raw_row.get("value")
    )

    # Extract merchant/recipient
    merchant = (
        raw_row.get("merchant")
        or raw_row.get("Merchant")
        or raw_row.get("recipient")
        or raw_row.get("payee")
        or raw_row.get("Получатель")
    )

    # Extract category (if provided)
    category = (
        raw_row.get("category") or raw_row.get("Category") or raw_row.get("Категория")
    )

    # Extract description
    description = (
        raw_row.get("description")
        or raw_row.get("Description")
        or raw_row.get("note")
        or raw_row.get("Описание")
    )

    # External ID (from bank/API)
    external_id = (
        raw_row.get("id") or raw_row.get("transaction_id") or raw_row.get("payment_id")
    )

    # Build standard format
    standard = {
        "date": date,
        "amount": amount,
        "merchant": merchant,
        "category": category,
        "description": description,
        "external_id": str(external_id) if external_id else None,
        "raw_payload": raw_row,  # Keep original for debugging
    }

    # Generate deduplication hash
    standard["transaction_hash"] = generate_hash(standard)

    return standard


# ============================================================================
# STEP 3: SAVE TO DATABASE (RAW/UNPROCESSED)
# ============================================================================


async def save_to_database(
    transactions: List[Dict[str, Any]],
    user_id: int,
    account_id: Optional[int],
    db: AsyncSession,
) -> Dict[str, Any]:
    """
    Save raw transactions to database.

    Important: We save data AS-IS (no cleaning yet)
    - Amount might be string: "1,500,000"
    - Date might be weird: "15.01.2025"
    - Merchant might be messy: "MAKRO TASHKENT   "

    That's ok, transform.py will clean it later.

    Args:
        transactions: List of standardized transaction dicts
        user_id: Who owns this data
        account_id: Which account (if known)
        db: Database session

    Returns:
        Statistics: {saved: 10, duplicates: 2, errors: []}
    """
    saved = 0
    duplicates = 0
    errors = []

    for idx, txn in enumerate(transactions, start=1):
        try:
            # Check if this transaction already exists (deduplication)
            existing = await db.execute(
                models.Transaction.__table__.select().where(
                    models.Transaction.transaction_hash == txn["transaction_hash"]
                )
            )

            if existing.first():
                duplicates += 1
                continue  # Skip duplicate

            # Create transaction record (UNPROCESSED)
            transaction = models.Transaction(
                owner_id=user_id,
                account_id=account_id,
                amount=str(txn["amount"]),
                merchant=txn["merchant"],
                category=txn["category"],
                description=txn["description"],
                external_id=txn["external_id"],
                raw_payload=txn["raw_payload"],
                transaction_hash=txn["transaction_hash"],
                processed=False,  # NOT cleaned yet
            )

            db.add(transaction)
            saved += 1

        except Exception as e:
            errors.append(f"Row {idx}: {str(e)}")

    # Save to database
    try:
        await db.commit()
        print(f"Saved {saved} transactions, skipped {duplicates} duplicates")
    except Exception as e:
        await db.rollback()
        print(f"Database error: {e}")
        raise

    return {"saved": saved, "duplicates": duplicates, "errors": errors}


# ============================================================================
# MAIN FUNCTIONS - The ones you'll actually use
# ============================================================================


async def ingest_from_csv(
    file_content: bytes, user_id: int, account_id: Optional[int], db: AsyncSession
) -> Dict[str, Any]:
    """
    Complete CSV ingestion pipeline.

    Usage in FastAPI:
        @router.post("/upload")
        async def upload(file: UploadFile, user: User, db: AsyncSession):
            content = await file.read()
            result = await ingest_from_csv(
                file_content=content,
                user_id=user.id,
                account_id=None,  # or specific account
                db=db
            )
            return result
    """
    # Step 1: Read CSV
    rows = read_csv_file(file_content)

    # Step 2: Convert to standard format
    transactions = [to_standard_format(row, source="csv") for row in rows]

    # Step 3: Save to database
    result = await save_to_database(transactions, user_id, account_id, db)

    return {"total": len(rows), **result}


async def ingest_from_payme(
    user_id: int, account_id: int, merchant_id: str, api_token: str, db: AsyncSession
) -> Dict[str, Any]:
    """
    Fetch transactions from Payme API.

    Payme API docs: https://developer.help.paycom.uz/

    Note: You'll need merchant credentials
    """
    # Payme API endpoint
    api_url = "https://checkout.paycom.uz/api"

    headers = {
        "X-Auth": f"{merchant_id}:{api_token}",
        "Content-Type": "application/json",
    }

    # Fetch transactions
    transactions = await fetch_from_api(api_url, headers=headers)

    # Convert to standard format
    std_transactions = [to_standard_format(txn, source="payme") for txn in transactions]

    # Save to database
    result = await save_to_database(std_transactions, user_id, account_id, db)

    return {"total": len(transactions), **result}


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

"""
EXAMPLE 1: Upload CSV from Uzum Bank
=====================================

User downloads CSV from Uzum Bank:
    Date        | Amount      | Merchant
    15.01.2025  | -1,500,000  | MAKRO
    16.01.2025  | +5,000,000  | Salary

In your FastAPI endpoint:

@router.post("/transactions/upload")
async def upload_csv(
    file: UploadFile,
    account_id: int,  # User selects which account this CSV is from
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    content = await file.read()
    
    result = await ingest_from_csv(
        file_content=content,
        user_id=current_user.id,
        account_id=account_id,
        db=db
    )
    
    return {
        "message": f"Uploaded {result['saved']} transactions",
        "duplicates": result['duplicates'],
        "errors": result['errors']
    }


EXAMPLE 2: Fetch from Payme
============================

@router.post("/transactions/sync-payme")
async def sync_payme(
    account_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    # User's Payme credentials stored in account settings
    account = await db.get(Account, account_id)
    
    result = await ingest_from_payme(
        user_id=current_user.id,
        account_id=account_id,
        merchant_id=account.payme_merchant_id,
        api_token=account.payme_token,
        db=db
    )
    
    return result
"""

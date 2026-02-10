import csv
import io
import hashlib
from typing import List, Dict, Any, Optional
from datetime import datetime
import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from app.core import models


# -----------------------------------------------------------------------------
# INGEST MODULE
# Purpose: bring raw external data into the system in a consistent shape.
# Why: standardizing early simplifies transform/load and reduces data drift.
# -----------------------------------------------------------------------------


# Decode, clean empty rows, make dict - CSV
def read_csv_file(file_bytes: bytes) -> list[dict]:
    """
    Decode and parse a CSV file into a list of row dictionaries.
    Why: normalize file input into a consistent structure for downstream parsing.

    Args:
        file_bytes: Raw CSV bytes uploaded by the user.

    Returns:
        List of non-empty row dictionaries.

    Example:
        rows = read_csv_file(file_bytes)
    """
    try:
        text = file_bytes.decode("utf-8")
    except UnicodeDecodeError:
        text = file_bytes.decode("windows-1251")

    reader = csv.DictReader(io.StringIO(text))
    rows = [row for row in reader if any(row.values())]
    return rows


# Fetch, normalize data into JSON from dict[list] - API
def normalize_api_response(data: Any) -> list[dict]:
    """
    Normalize API responses into a list of transaction-like dicts.
    Why: APIs return different shapes and this creates a single expected format.

    Args:
        data: Raw JSON from an API response.

    Returns:
        List of transaction dictionaries.

    Example:
        rows = normalize_api_response(response.json())
    """
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return (
            data.get("data")
            or data.get("transactions")
            or data.get("result", {}).get("transactions")
            or []
        )
    raise ValueError(f"Unexpected API response type: {type(data)}")


async def fetch_from_api(
    url: str, headers: dict, params: Optional[Dict[str, Any]] = None
) -> list[dict]:
    """
    Fetch JSON data from an external API and normalize it.
    Why: centralize API fetching and normalization for reusability and testing.

    Args:
        url: API endpoint URL.
        headers: HTTP headers for authentication and metadata.
        params: Optional query parameters.

    Returns:
        Normalized list of transaction dicts.

    Example:
        rows = await fetch_from_api(url, headers, params={"from": "2026-01-01"})
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url, headers=headers, params=params)
        response.raise_for_status()
        data: Any = response.json()

        return normalize_api_response(data)


# Fetch data from Uzum
def uzum_webhook_to_standard(payload: dict, event_type: str) -> dict:
    """
    Convert an Uzum webhook payload into the standard transaction shape.
    Why: keeps webhook ingestion consistent with CSV/API ingestion flows.

    Args:
        payload: Raw webhook payload.
        event_type: Event type label from the webhook.

    Returns:
        Standardized transaction dict.

    Example:
        txn = uzum_webhook_to_standard(payload, "payment.completed")
    """
    ts_ms = (
        payload.get("timestamp")
        or payload.get("transTime")
        or payload.get("confirmTime")
    )
    date = datetime.fromtimestamp(ts_ms / 1000).isoformat() if ts_ms else None

    return {
        "date": date,
        "amount": payload.get("amount"),
        "merchant": "Uzum Bank",
        "category": None,
        "description": f"Uzum webhook: {event_type}",
        "external_id": payload.get("transId"),
        "raw_payload": payload,
        "source": "uzum_webhook",
    }


# Create a standart format
def to_standard_format(raw_row: Dict[str, Any], source: str = "csv") -> dict:
    """
    Convert a raw row into the standard transaction format.
    Why: ensures all sources map to the same schema before persistence.

    Args:
        raw_row: Raw CSV/API row.
        source: Source label (csv, api, webhook, etc.).

    Returns:
        Standardized transaction dict with a transaction hash.

    Example:
        txn = to_standard_format(row, source="csv")
    """

    date = (
        raw_row.get("date")
        or raw_row.get("Date")
        or raw_row.get("created_at")
        or raw_row.get("timestamp")
        or raw_row.get("Дата")
    )

    amount = (
        raw_row.get("amount")
        or raw_row.get("Amount")
        or raw_row.get("Сумма")
        or raw_row.get("value")
    )

    merchant = (
        raw_row.get("merchant")
        or raw_row.get("Merchant")
        or raw_row.get("recipient")
        or raw_row.get("payee")
        or raw_row.get("Получатель")
    )

    category = (
        raw_row.get("category") or raw_row.get("Category") or raw_row.get("Категория")
    )

    description = (
        raw_row.get("description")
        or raw_row.get("Description")
        or raw_row.get("note")
        or raw_row.get("Описание")
    )

    external_id = (
        raw_row.get("id") or raw_row.get("transaction_id") or raw_row.get("payment_id")
    )

    raw_payload = raw_row.get("raw_payload", raw_row)

    standard = {
        "date": date,
        "amount": amount,
        "merchant": merchant,
        "category": category,
        "description": description,
        "external_id": str(external_id) if external_id else None,
        "raw_payload": raw_payload,
        "source": source,
    }
    standard["transaction_hash"] = generate_hash(standard)

    return standard


# Make each transaction unique with it's own hash
def generate_hash(tnx: dict) -> str:
    """
    Create a stable hash for deduplication.
    Why: prevents duplicate transaction inserts across multiple imports.

    Args:
        tnx: Standardized transaction dict.

    Returns:
        SHA-256 hash string.

    Example:
        tx_hash = generate_hash(txn)
    """
    key = f"{tnx.get('date')}|{tnx.get('amount')}|{tnx.get('merchant')}|{tnx.get('source')}"
    return hashlib.sha256(key.encode()).hexdigest()


# Save transactions to db without duplicates
async def save_to_database(
    transactions: List[Dict[str, Any]],
    user_id: int,
    account_id: Optional[int],
    db: AsyncSession,
) -> Dict[str, Any]:
    """
    Persist standardized transactions to the database with deduplication.
    Why: keeps raw data while enforcing uniqueness at ingest time.

    Args:
        transactions: List of standardized transactions.
        user_id: Owner of the transactions.
        account_id: Optional account association.
        db: Async database session.

    Returns:
        Save stats with counts and errors.

    Example:
        result = await save_to_database(transactions, user_id, account_id, db)
    """

    saved = 0
    duplicates = 0
    errors: list[str] = []

    for idx, txn in enumerate(transactions, start=1):
        # Deduplication
        try:
            existing = await db.execute(
                models.Transaction.__table__.select().where(
                    models.Transaction.transaction_hash == txn["transaction_hash"]
                )
            )
            if existing.first():
                duplicates += 1
                continue

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
                processed=False,
            )

            db.add(transaction)
            saved += 1

        except Exception as e:
            errors.append(f"Row {idx}: {str(e)}")

    try:
        await db.commit()
        print(f"Saved {saved} transactions, skipped {duplicates} duplicates")
    except Exception as e:
        await db.rollback()
        print(f"Database error: {e}")
        raise

    return {"saved": saved, "duplicates": duplicates, "errors": errors}


# Orchestrate the whole process for CSV
async def ingest_from_csv(
    file_content: bytes, user_id: int, account_id: Optional[int], db: AsyncSession, source: str = "csv"
) -> Dict[str, Any]:
    """
    Ingest transactions from a CSV file into the raw transactions table.
    Why: CSV is a primary ingestion path for manual uploads.

    Args:
        file_content: CSV bytes.
        user_id: Owner of transactions.
        account_id: Optional account association.
        db: Async database session.
        source: Source label.

    Returns:
        Ingestion stats with total rows, saved, duplicates, errors.

    Example:
        result = await ingest_from_csv(content, user_id, account_id, db)
    """

    rows = read_csv_file(file_content)
    transactions = [to_standard_format(row, source=source) for row in rows]
    result = await save_to_database(transactions, user_id, account_id, db)

    return {"total": len(rows), **result}


# Orchestrate the whole process for API
async def ingest_from_api(
    url: str,
    headers: dict,
    user_id: int,
    account_id: int,
    db: AsyncSession,
    params: Optional[Dict[str, Any]] = None,
    source: str = "api",
) -> Dict[str, Any]:
    """
    Ingest transactions from an external API into the raw table.
    Why: supports automated data pulls from providers.

    Args:
        url: API endpoint URL.
        headers: HTTP headers.
        user_id: Owner of transactions.
        account_id: Account association.
        db: Async database session.
        params: Optional query parameters.
        source: Source label.

    Returns:
        Ingestion stats with total rows, saved, duplicates, errors.

    Example:
        result = await ingest_from_api(url, headers, user_id, account_id, db)
    """
    # Fetch raw data from API, normalize, and persist
    rows = await fetch_from_api(url, headers, params=params)
    transactions = [to_standard_format(r, source=source) for r in rows]
    result = await save_to_database(transactions, user_id, account_id, db)
    return {"total": len(rows), **result}


# Orchestrate the whole process for Uzum
async def ingest_from_uzum_webhook(
    payload: dict,
    event_type: str,
    user_id: int,
    account_id: int,
    db: AsyncSession,
) -> Dict[str, Any]:
    """
    Ingest a single Uzum webhook transaction into the raw table.
    Why: allows real-time transaction capture from webhook events.

    Args:
        payload: Webhook payload.
        event_type: Webhook event label.
        user_id: Owner of transaction.
        account_id: Account association.
        db: Async database session.

    Returns:
        Ingestion stats with total rows, saved, duplicates, errors.

    Example:
        result = await ingest_from_uzum_webhook(payload, event_type, user_id, account_id, db)
    """
    txn = to_standard_format(
        uzum_webhook_to_standard(payload, event_type), source="uzum_webhook"
    )
    result = await save_to_database([txn], user_id, account_id, db)
    return {"total": 1, **result}

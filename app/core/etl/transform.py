# app/core/etl/transform.py
"""
TRANSFORM MODULE - Clean and normalize raw transaction data

Purpose:
    1. Fix messy data from different sources
    2. Standardize formats (dates, amounts, currencies)
    3. Auto-categorize transactions
    4. Mark transactions as "processed"

Data Flow:
    raw_data (from ingest.py) → clean_date() → clean_amount() → categorize() → clean_data
                                                                                   ↓
                                                                           Update in database
                                                                                   ↓
                                                                          Mark as processed

Why this matters:
    - Banks export "1,500,000" with commas → we need "1500000.00"
    - Uzbek dates "15.01.2025" → we need "2025-01-15"
    - "MAKRO TASHKENT" and "Makro" → should be same merchant
    - Missing categories → need smart auto-categorization
"""

import re
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, date
from decimal import Decimal, InvalidOperation

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from app.core import models


# ============================================================================
# STEP 1: CLEAN AND STANDARDIZE DATES
# ============================================================================


def parse_uzbek_date(date_str: str) -> Optional[date]:
    """
    Parse various Uzbek date formats to standard date object.

    Handles:
        - "15.01.2025" (Uzbek bank format)
        - "15/01/2025"
        - "2025-01-15" (ISO standard)
        - "15-01-2025"
        - "15 Jan 2025"
        - Unix timestamps (from APIs)

    Why so many formats?
        Uzbek banks use DD.MM.YYYY
        International banks use YYYY-MM-DD
        APIs use Unix timestamps
        Manual uploads might use anything

    Args:
        date_str: Raw date string from source

    Returns:
        datetime.date object or None if parsing fails

    Examples:
        "15.01.2025" → datetime.date(2025, 1, 15)
        "1673750400" → datetime.date(2023, 1, 15)  # Unix timestamp
    """
    if not date_str:
        return None

    # Remove whitespace and common separators
    date_str = str(date_str).strip()

    # Format patterns to try (most common first)
    formats = [
        "%d.%m.%Y",  # 15.01.2025 (Uzbek banks)
        "%d/%m/%Y",  # 15/01/2025
        "%d-%m-%Y",  # 15-01-2025
        "%Y-%m-%d",  # 2025-01-15 (ISO)
        "%d %b %Y",  # 15 Jan 2025
        "%d %B %Y",  # 15 January 2025
    ]

    # Try string formats first
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue

    # Try Unix timestamp (API data)
    try:
        # Handle both seconds and milliseconds
        timestamp = float(date_str)
        if timestamp > 1e10:  # Milliseconds
            timestamp /= 1000
        return datetime.fromtimestamp(timestamp).date()
    except (ValueError, OSError):
        pass

    # Try to extract year-month-day from messy strings
    # Example: "Transaction date: 15.01.2025" → extract the date part
    date_pattern = r"(\d{1,4}[.\-/]\d{1,2}[.\-/]\d{2,4})"
    match = re.search(date_pattern, date_str)
    if match:
        extracted_date = match.group(1)
        return parse_uzbek_date(extracted_date)  # Recursive call with cleaner date

    return None


def clean_transaction_date(transaction_date: Any) -> Optional[date]:
    """
    Main function to clean transaction dates.

    This is the public interface that other modules will call.
    """
    if not transaction_date:
        return date.today()  # Default to today if missing

    return parse_uzbek_date(str(transaction_date))


# ============================================================================
# STEP 2: CLEAN AND STANDARDIZE AMOUNTS
# ============================================================================


def parse_amount(amount_str: str) -> Optional[Decimal]:
    """
    Parse amount strings to Decimal numbers.

    Handles:
        - "1,500,000" → Decimal('1500000')
        - "1,500,000.50" → Decimal('1500000.50')
        - "-50000" → Decimal('-50000')
        - "1 500 000" (space separator)
        - "50000 so'm" → Decimal('50000')
        - "$100.50" → Decimal('100.50')
        - "100.50 USD" → Decimal('100.50')

    Why Decimal instead of float?
        💰 Money needs precision - float has rounding errors
        🏦 Banking requires exact calculations
        📊 Reporting must be accurate

    Args:
        amount_str: Raw amount string

    Returns:
        Decimal object or None if parsing fails

    Examples:
        "1,500,000.50" → Decimal('1500000.50')
        "-50,000" → Decimal('-50000')
        "100 USD" → Decimal('100')
    """
    if not amount_str:
        return None

    # Convert to string and clean
    amount_str = str(amount_str).strip()

    # Remove currency symbols and words
    currency_patterns = [
        r"[UZS\s]*$",  # Remove "UZS" at end
        r"USD\s*$",  # Remove "USD" at end
        r"\$",  # Remove dollar sign
        r"so'm",  # Remove "so'm" (Uzbek currency)
        r"сум",  # Remove "сум" (Russian)
    ]

    for pattern in currency_patterns:
        amount_str = re.sub(pattern, "", amount_str, flags=re.IGNORECASE)

    # Handle spaces as thousand separators: "1 500 000" → "1500000"
    amount_str = amount_str.replace(" ", "")

    # Handle commas as thousand separators: "1,500,000" → "1500000"
    # But keep comma as decimal separator if it's the last one: "1,500" → "1500"
    if "," in amount_str:
        parts = amount_str.split(",")
        if len(parts) == 2 and len(parts[1]) <= 2:
            # Likely decimal comma: "1,50" → "1.50"
            amount_str = ".".join(parts)
        else:
            # Thousand separators: "1,500,000" → "1500000"
            amount_str = "".join(parts)

    # Clean any remaining non-numeric characters except minus and dot
    amount_str = re.sub(r"[^0-9.-]", "", amount_str)

    try:
        return Decimal(amount_str)
    except (InvalidOperation, ValueError):
        return None


def clean_transaction_amount(amount: Any) -> Optional[Decimal]:
    """
    Main function to clean transaction amounts.

    Returns positive Decimal for income, negative for expenses.
    """
    if amount is None:
        return None

    cleaned = parse_amount(str(amount))
    return cleaned


# ============================================================================
# STEP 3: STANDARDIZE MERCHANT NAMES
# ============================================================================


def normalize_merchant_name(merchant: Optional[str]) -> Optional[str]:
    """
    Clean and normalize merchant names for better grouping.

    Handles:
        - "MAKRO TASHKENT" → "Makro"
        - "STARBUCKS COFFEE" → "Starbucks"
        - "Evos Restoran" → "Evos"
        - "OLCHA.UZ" → "Olcha"
        - "  MAKRO  " → "Makro" (remove whitespace)

    Why normalize?
        📊 Better spend analysis (Makro appears once, not 5 times)
        🔍 Easier searching
        📈 Accurate category assignment

    Args:
        merchant: Raw merchant name

    Returns:
        Normalized merchant name

    Examples:
        "MAKRO TASHKENT" → "Makro"
        "STARBUCKS #123" → "Starbucks"
        None → None
    """
    if not merchant:
        return None

    merchant = str(merchant).strip()

    # Common Uzbek brand mappings
    brand_mappings = {
        # Supermarkets
        r"makro.*tashkent": "Makro",
        r"makro.*yunusobod": "Makro",
        r"korzinka.*": "Korzinka",
        r"market.*": "Market",
        r"avicenna.*": "Avicenna",
        # Food & Restaurants
        r"starbucks.*": "Starbucks",
        r"evos.*": "Evos",
        r"cbon.*": "Cbon",
        r"texas.*": "Texas Chicken",
        r"kfc.*": "KFC",
        # Services
        r"olcha.*": "Olcha",
        r"uzum.*": "Uzum",
        r"click.*": "Click",
        r"payme.*": "Payme",
        # Banks
        r"uzum.*bank": "Uzum Bank",
        r"kapital.*bank": "Kapital Bank",
        r"agrobank": "Agrobank",
        r"hamkorbank": "Hamkorbank",
        # Transport
        r"yandex.*taxi": "Yandex Taxi",
        r"taxi.*": "Taxi",
        r"metro.*": "Metro",
    }

    # Convert to lowercase for matching
    merchant_lower = merchant.lower()

    # Try to match patterns
    for pattern, replacement in brand_mappings.items():
        if re.search(pattern, merchant_lower):
            return replacement

    # General cleanup
    # Remove common suffixes
    suffixes = ["tashkent", "yunusobod", "restaurant", "cafe", "llc", "inc"]
    for suffix in suffixes:
        merchant = re.sub(rf"\s*{suffix}.*$", "", merchant, flags=re.IGNORECASE)

    # Remove extra spaces and capitalize
    merchant = re.sub(r"\s+", " ", merchant).strip()

    # Capitalize properly (not ALL CAPS)
    if len(merchant) > 0:
        merchant = merchant[0].upper() + merchant[1:].lower()

    return merchant if merchant else None


# ============================================================================
# STEP 4: AUTO-CATEGORIZE TRANSACTIONS
# ============================================================================


def categorize_transaction(
    merchant: Optional[str], description: Optional[str], amount: Optional[Decimal]
) -> Optional[str]:
    """
    Automatically assign category based on merchant and description.

    Categories used in Uzbekistan:
        - Food & Restaurants
        - Transport & Taxi
        - Shopping & Retail
        - Health & Medicine
        - Education
        - Entertainment & Leisure
        - Bills & Utilities
        - Transfer & Withdrawal
        - Salary & Income
        - Other

    Why auto-categorize?
        📊 Users don't want to categorize hundreds of transactions
        🎯 Smart defaults save time
        ✋ Still allows manual overrides

    Args:
        merchant: Normalized merchant name
        description: Transaction description
        amount: Transaction amount (positive/negative helps categorize)

    Returns:
        Category name or None if uncertain

    Examples:
        merchant="Starbucks", amount=-50000 → "Food & Restaurants"
        merchant="Taxi", amount=-25000 → "Transport & Taxi"
        merchant="Salary", amount=5000000 → "Salary & Income"
    """
    # Combine search text
    search_text = f"{merchant or ''} {description or ''}".lower()

    # Income categories (positive amounts)
    if amount and amount > 0:
        if any(word in search_text for word in ["salary", "зарплата", "заработок"]):
            return "Salary & Income"
        if any(word in search_text for word in ["transfer", "перевод", "возврат"]):
            return "Transfer & Income"

    # Expense categories (negative amounts or small positives which might be refunds)
    if not amount or amount < 0 or (amount > 0 and amount < 100000):

        # Food & Restaurants
        food_keywords = [
            "starbucks",
            "evos",
            "cbon",
            "texas",
            "kfc",
            "mcdonald",
            "restaurant",
            "кафе",
            "ресторан",
            "coffe",
            "coffee",
            "lavash",
            "osh",
            "palov",
            "food",
        ]
        if any(word in search_text for word in food_keywords):
            return "Food & Restaurants"

        # Transport
        transport_keywords = [
            "taxi",
            "yandex",
            "metro",
            "bus",
            "такси",
            "метро",
            "transport",
            "uber",
            "bolt",
        ]
        if any(word in search_text for word in transport_keywords):
            return "Transport & Taxi"

        # Shopping & Retail
        shopping_keywords = [
            "makro",
            "korzinka",
            "market",
            "supermarket",
            "olcha",
            "uzum",
            "clothing",
            "clothes",
            "одежда",
            "обувь",
            "electronics",
        ]
        if any(word in search_text for word in shopping_keywords):
            return "Shopping & Retail"

        # Health & Medicine
        health_keywords = [
            "pharmacy",
            "аптека",
            "hospital",
            "больница",
            "doctor",
            "врач",
            "medicine",
            "лекарство",
            "clinic",
            "клиника",
        ]
        if any(word in search_text for word in health_keywords):
            return "Health & Medicine"

        # Bills & Utilities
        bills_keywords = [
            "electricity",
            "gas",
            "water",
            "internet",
            "phone",
            "mobile",
            "электричество",
            "газ",
            "вода",
            "интернет",
            "телефон",
            "utility",
            "коммунальные",
        ]
        if any(word in search_text for word in bills_keywords):
            return "Bills & Utilities"

        # Entertainment & Leisure
        entertainment_keywords = [
            "cinema",
            "movie",
            "theater",
            "concert",
            "game",
            "кино",
            "театр",
            "концерт",
            "развлечение",
            "entertainment",
        ]
        if any(word in search_text for word in entertainment_keywords):
            return "Entertainment & Leisure"

        # Education
        education_keywords = [
            "school",
            "university",
            "course",
            "training",
            "education",
            "школа",
            "университет",
            "курс",
            "образование",
            "обучение",
        ]
        if any(word in search_text for word in education_keywords):
            return "Education"

        # Banks & Financial Services
        bank_keywords = [
            "bank",
            "комиссия",
            "обслуживание",
            "fee",
            "commission",
            "перевод",
            "вывод",
            "withdraw",
        ]
        if any(word in search_text for word in bank_keywords):
            return "Bank & Financial Services"

    return "Other"


# ============================================================================
# STEP 5: MAIN TRANSFORMATION FUNCTION
# ============================================================================


async def transform_transaction(
    transaction: models.Transaction, db: AsyncSession
) -> bool:
    """
    Transform a single transaction from raw to clean.

    This is where all the cleaning functions come together!

    Args:
        transaction: Raw transaction from database
        db: Database session for updates

    Returns:
        True if transformation succeeded, False if failed

    Process:
        1. Extract raw data from raw_payload
        2. Clean date → format properly
        3. Clean amount → remove commas, convert to Decimal
        4. Normalize merchant → standard naming
        5. Auto-categorize → smart category assignment
        6. Update database with clean data
        7. Mark as processed = True
    """
    try:
        # Get raw data (what we originally received)
        raw_data = transaction.raw_payload or {}

        # === STEP 1: Clean Date ===
        # Try raw data first, then fallback to stored date
        raw_date = raw_data.get("date") or raw_data.get("Date") or raw_data.get("Дата")
        if raw_date:
            cleaned_date = clean_transaction_date(raw_date)
        else:
            # Convert stored datetime to date - access the attribute value
            created_at_attr = getattr(transaction, "created_at", None)
            if created_at_attr is not None:
                cleaned_date = created_at_attr.date()
            else:
                cleaned_date = None

        if cleaned_date is None:
            print(f"⚠️  Could not parse date for transaction {transaction.id}")
            return False

        # === STEP 2: Clean Amount ===
        # Try multiple sources for amount
        raw_amount = (
            raw_data.get("amount") or raw_data.get("Amount") or raw_data.get("Сумма")
        )
        if raw_amount:
            cleaned_amount = clean_transaction_amount(raw_amount)
        else:
            # Use existing amount from database - access the attribute value
            amount_attr = getattr(transaction, "amount", None)
            cleaned_amount = amount_attr if amount_attr is not None else None

        if cleaned_amount is None:
            print(f"⚠️  Could not parse amount for transaction {transaction.id}")
            return False

        # === STEP 3: Normalize Merchant ===
        raw_merchant = raw_data.get("merchant") or raw_data.get("Merchant")
        if raw_merchant:
            cleaned_merchant = normalize_merchant_name(raw_merchant)
        else:
            # Access the merchant attribute value
            merchant_attr = getattr(transaction, "merchant", None)
            cleaned_merchant = merchant_attr

        # === STEP 4: Auto-Categorize ===
        raw_description = raw_data.get("description") or raw_data.get("Description")
        if not raw_description:
            # Access the description attribute value
            description_attr = getattr(transaction, "description", None)
            raw_description = description_attr

        # Use existing category if user already set it, otherwise auto-categorize
        category_attr = getattr(transaction, "category", None)
        if category_attr is None or str(category_attr) == "":
            cleaned_category = categorize_transaction(
                str(cleaned_merchant) if cleaned_merchant else None,
                str(raw_description) if raw_description else None,
                cleaned_amount,
            )
        else:
            cleaned_category = category_attr

        # === STEP 5: Update Database ===
        # Update transaction with clean data
        update_data = {
            "amount": cleaned_amount,
            "created_at": cleaned_date,
            "merchant": cleaned_merchant,
            "category": cleaned_category,
            "description": raw_description,
            "processed": True,  # Mark as cleaned!
            "updated_at": datetime.now(),
        }

        # Update in database
        stmt = (
            update(models.Transaction)
            .where(models.Transaction.id == transaction.id)
            .values(**update_data)
        )

        await db.execute(stmt)
        await db.commit()

        print(
            f"✅ Transformed transaction {transaction.id}: {cleaned_merchant} → {cleaned_category}"
        )
        return True

    except Exception as e:
        print(f"❌ Error transforming transaction {transaction.id}: {e}")
        await db.rollback()
        return False


# ============================================================================
# STEP 6: BATCH PROCESSING
# ============================================================================


async def transform_all_unprocessed(
    user_id: int, db: AsyncSession, batch_size: int = 100
) -> Dict[str, int]:
    """
    Transform all unprocessed transactions for a user.

    This is the main function you'll call from your API endpoints.

    Args:
        user_id: Which user's transactions to process
        db: Database session
        batch_size: How many transactions to process at once

    Returns:
        Statistics about what was processed

    Example result:
        {
            "total": 250,
            "processed": 247,
            "failed": 3,
            "skipped": 0
        }
    """
    # Get all unprocessed transactions for this user
    stmt = (
        select(models.Transaction)
        .where(
            models.Transaction.owner_id == user_id,
            models.Transaction.processed == False,
        )
        .order_by(models.Transaction.created_at.desc())
    )

    result = await db.execute(stmt)
    transactions = result.scalars().all()

    stats = {"total": len(transactions), "processed": 0, "failed": 0, "skipped": 0}

    print(f"🔄 Starting transformation of {len(transactions)} transactions...")

    for transaction in transactions:
        success = await transform_transaction(transaction, db)
        if success:
            stats["processed"] += 1
        else:
            stats["failed"] += 1

    print(
        f"✅ Transformation complete: {stats['processed']}/{stats['total']} processed"
    )
    return stats


# ============================================================================
# STEP 7: UTILITY FUNCTIONS
# ============================================================================


async def reprocess_transaction(transaction_id: int, db: AsyncSession) -> bool:
    """
    Re-process a single transaction (useful for debugging or fixes).

    Sometimes you need to re-run transformation:
    - Fixed a bug in categorization logic
    - Updated merchant mappings
    - User wants to re-categorize with new rules

    Args:
        transaction_id: Specific transaction to reprocess
        db: Database session

    Returns:
        True if successful
    """
    # Get the transaction
    stmt = select(models.Transaction).where(models.Transaction.id == transaction_id)
    result = await db.execute(stmt)
    transaction = result.scalar_one_or_none()

    if not transaction:
        print(f"❌ Transaction {transaction_id} not found")
        return False

    # Mark as unprocessed first
    stmt = (
        update(models.Transaction)
        .where(models.Transaction.id == transaction_id)
        .values(processed=False)
    )

    await db.execute(stmt)
    await db.commit()

    # Now transform it
    return await transform_transaction(transaction, db)


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

"""
EXAMPLE 1: Transform all transactions for a user
================================================

@router.post("/transactions/transform")
async def transform_transactions(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    
    stats = await transform_all_unprocessed(
        user_id=current_user.id,
        db=db
    )
    
    return {
        "message": f"Transformed {stats['processed']} transactions",
        "stats": stats
    }


EXAMPLE 2: Reprocess a specific transaction
==========================================

@router.post("/transactions/{transaction_id}/reprocess")
async def reprocess_single(
    transaction_id: int,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    
    # Verify ownership
    transaction = await db.get(Transaction, transaction_id)
    if not transaction or transaction.owner_id != current_user.id:
        raise HTTPException(404, "Transaction not found")
    
    success = await reprocess_transaction(transaction_id, db)
    
    if success:
        return {"message": "Transaction reprocessed successfully"}
    else:
        raise HTTPException(500, "Failed to reprocess transaction")


EXAMPLE 3: Manual category override
===================================

Sometimes the auto-categorization gets it wrong. Let users fix it:

@router.put("/transactions/{transaction_id}/category")
async def update_category(
    transaction_id: int,
    new_category: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    
    # Update category
    stmt = update(models.Transaction).where(
        models.Transaction.id == transaction_id,
        models.Transaction.owner_id == current_user.id
    ).values(category=new_category)
    
    await db.execute(stmt)
    await db.commit()
    
    return {"message": "Category updated"}
"""

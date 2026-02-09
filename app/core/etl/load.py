from typing import Dict, Any, List, Optional
from datetime import datetime, date
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, func
from sqlalchemy.orm import selectinload

from app.core import models


async def calculate_account_balance(
    account_id: int, db: AsyncSession, end_date: Optional[date] = None
) -> Optional[Decimal]:

    # Build query for transactions
    query = select(func.coalesce(func.sum(models.Transaction.amount), 0)).where(
        models.Transaction.account_id == account_id,
        models.Transaction.processed == True,
    )

    if end_date:
        query = query.where(models.Transaction.created_at <= end_date)

    result = await db.execute(query)
    balance = result.scalar()

    return Decimal(str(balance)) if balance else Decimal("0")


async def update_account_balance(account_id: int, db: AsyncSession) -> bool:

    try:
        # Calculate current balance
        new_balance = await calculate_account_balance(account_id, db)

        if new_balance is None:
            return False

        # Update account record
        stmt = (
            update(models.Account)
            .where(models.Account.id == account_id)
            .values(balance=new_balance, updated_at=datetime.now())
        )

        await db.execute(stmt)
        await db.commit()

        print(f"Updated account {account_id} balance: {new_balance}")
        return True

    except Exception as e:
        print(f"Error updating account balance: {e}")
        await db.rollback()
        return False


async def update_all_account_balances(user_id: int, db: AsyncSession) -> Dict[str, int]:

    # Get all user's accounts
    stmt = select(models.Account).where(models.Account.owner_id == user_id)
    result = await db.execute(stmt)
    accounts = result.scalars().all()

    stats = {"updated": 0, "failed": 0}

    for account in accounts:
        success = await update_account_balance(account.id, db)
        if success:
            stats["updated"] += 1
        else:
            stats["failed"] += 1

    print(
        f"Balance update complete: {stats['updated']}/{len(accounts)} accounts updated"
    )
    return stats


async def create_performance_indexes(db: AsyncSession) -> bool:

    try:
        # Common query patterns and their indexes:
        index_patterns = [
            # User dashboard: "Get user's transactions for last 30 days"
            "CREATE INDEX IF NOT EXISTS idx_user_date ON transactions(owner_id, created_at DESC)",
            # Category filtering: "Get all food transactions for user"
            "CREATE INDEX IF NOT EXISTS idx_user_category ON transactions(owner_id, category)",
            # Merchant search: "Find all Starbucks transactions"
            "CREATE INDEX IF NOT EXISTS idx_user_merchant ON transactions(owner_id, merchant)",
            # Account statements: "Get transactions for account X"
            "CREATE INDEX IF NOT EXISTS idx_account_date ON transactions(account_id, created_at DESC)",
            # ETL processing: "Find unprocessed transactions"
            "CREATE INDEX IF NOT EXISTS idx_processed_date ON transactions(processed, created_at)",
            # Search queries: "Search by description"
            "CREATE INDEX IF NOT EXISTS idx_description_text ON transactions USING gin(to_tsvector('english', description))",
        ]

        for index_sql in index_patterns:
            await db.execute(index_sql)

        await db.commit()
        print("Performance indexes created successfully")
        return True

    except Exception as e:
        print(f"Error creating indexes: {e}")
        await db.rollback()
        return False


async def validate_transaction_data(
    transaction_id: int, db: AsyncSession
) -> Dict[str, Any]:

    # Get the transaction with related data
    stmt = (
        select(models.Transaction)
        .options(
            selectinload(models.Transaction.owner),
            selectinload(models.Transaction.account),
        )
        .where(models.Transaction.id == transaction_id)
    )

    result = await db.execute(stmt)
    transaction = result.scalar_one_or_none()

    if not transaction:
        return {"valid": False, "errors": ["Transaction not found"], "warnings": []}

    errors = []
    warnings = []

    # VALIDATION CHECKS

    # 1. Amount validation
    if not transaction.amount:
        errors.append("Amount cannot be null")
    elif transaction.amount == 0:
        warnings.append("Amount is zero - possible data entry error")

    # 2. Date validation
    if not transaction.created_at:
        errors.append("Date cannot be null")
    else:
        # Check whether the date is valid, in the future or too old
        today = date.today()
        txn_date = transaction.created_at.date()

        if txn_date > today:
            warnings.append("Transaction date is in the future")
        elif txn_date.year < 2000:
            warnings.append("Transaction date is very old - verify data")

    # 3. Owner validation
    if not transaction.owner:
        errors.append("Transaction has no owner")

    # 4. Account validation
    if transaction.account_id:
        if not transaction.account:
            errors.append("Referenced account not found")
        elif transaction.account.owner_id != transaction.owner_id:
            errors.append("Account does not belong to transaction owner")

    # 5. Category validation
    valid_categories = [
        "Food & Restaurants",
        "Transport & Taxi",
        "Shopping & Retail",
        "Health & Medicine",
        "Education",
        "Entertainment & Leisure",
        "Bills & Utilities",
        "Bank & Financial Services",
        "Transfer & Income",
        "Salary & Income",
        "Other",
    ]

    if transaction.category and transaction.category not in valid_categories:
        warnings.append(f"Unknown category: {transaction.category}")

    # 6. Merchant validation
    if not transaction.merchant and abs(float(transaction.amount)) > 100000:
        warnings.append("Large transaction missing merchant name")

    return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}


async def validate_user_data(user_id: int, db: AsyncSession) -> Dict[str, Any]:

    # Get all user's transactions
    stmt = select(models.Transaction).where(models.Transaction.owner_id == user_id)
    result = await db.execute(stmt)
    transactions = result.scalars().all()

    report = {
        "total_transactions": len(transactions),
        "valid_transactions": 0,
        "invalid_transactions": 0,
        "warnings": [],
        "common_errors": {},
        "balance_issues": [],
    }

    # Validate each transaction
    for txn in transactions:
        validation = await validate_transaction_data(txn.id, db)

        if validation["valid"]:
            report["valid_transactions"] += 1
        else:
            report["invalid_transactions"] += 1

            # Track common errors
            for error in validation["errors"]:
                if error not in report["common_errors"]:
                    report["common_errors"][error] = 0
                report["common_errors"][error] += 1

        # Collect warnings
        report["warnings"].extend(validation["warnings"])

    # Check account balance consistency
    accounts_stmt = select(models.Account).where(models.Account.owner_id == user_id)
    accounts_result = await db.execute(accounts_stmt)
    accounts = accounts_result.scalars().all()

    for account in accounts:
        # Calculate balance from transactions
        calculated_balance = await calculate_account_balance(account.id, db)

        if calculated_balance != account.balance:
            report["balance_issues"].append(
                {
                    "account_id": account.id,
                    "account_name": account.name,
                    "stored_balance": float(account.balance),
                    "calculated_balance": float(calculated_balance),
                    "difference": float(account.balance - calculated_balance),
                }
            )

    print(
        f"Validation complete: {report['valid_transactions']}/{report['total_transactions']} valid"
    )
    return report


async def load_processed_data(user_id: int, db: AsyncSession) -> Dict[str, Any]:

    stats = {
        "accounts_updated": 0,
        "accounts_failed": 0,
        "data_valid": True,
        "issues_found": [],
    }

    print(f"Loading processed data for user {user_id}...")

    # === STEP 1: Update Account Balances ===
    balance_stats = await update_all_account_balances(user_id, db)
    stats["accounts_updated"] = balance_stats["updated"]
    stats["accounts_failed"] = balance_stats["failed"]

    # === STEP 2: Ensure Indexes ===
    await create_performance_indexes(db)

    # === STEP 3: Validate Data ===
    validation_report = await validate_user_data(user_id, db)

    if validation_report["invalid_transactions"] > 0:
        stats["data_valid"] = False
        stats["issues_found"] = validation_report["common_errors"]

    # === STEP 4: Update User Stats ===
    # You could add a user_stats table here to track:
    # - Total transactions
    # - Total spent by category
    # - Average transaction amount
    # etc.

    print(f"Loading complete: {stats['accounts_updated']} balances updated")
    return stats


async def get_user_account_summary(
    user_id: int, db: AsyncSession
) -> List[Dict[str, Any]]:

    # Get all user's accounts with transaction counts
    stmt = (
        select(models.Account)
        .where(models.Account.owner_id == user_id, models.Account.is_active == True)
        .options(selectinload(models.Account.transactions))
    )

    result = await db.execute(stmt)
    accounts = result.scalars().all()

    summaries = []

    for account in accounts:
        # Count transactions in last 30 days
        thirty_days_ago = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        thirty_days_ago = thirty_days_ago.replace(day=thirty_days_ago.day - 30)

        recent_txns = [
            txn for txn in account.transactions if txn.created_at >= thirty_days_ago
        ]

        summary = {
            "account_id": account.id,
            "account_name": account.name,
            "account_type": account.account_type,
            "currency": account.currency,
            "balance": float(account.balance),
            "provider": account.provider,
            "total_transactions": len(account.transactions),
            "recent_transactions_30d": len(recent_txns),
            "last_updated": (
                account.updated_at.isoformat() if account.updated_at else None
            ),
        }

        summaries.append(summary)

    return summaries

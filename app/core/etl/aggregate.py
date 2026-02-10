from typing import Dict, Any, List, Optional
from datetime import date, timedelta

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_, desc, asc

from app.core import models


# -----------------------------------------------------------------------------
# AGGREGATE MODULE
# Purpose: turn processed transactions into analytics and insights.
# Why: users need explanations and trends, not just raw rows.
# -----------------------------------------------------------------------------


# Calculate spendings by category in certain time range
async def get_user_spending_by_category(
    user_id: int, start_date: date, end_date: date, db: AsyncSession
) -> List[Dict[str, Any]]:
    """
    Calculate total spending by category for a date range.

    This is the foundation for pie charts and category breakdowns.
    Why: category totals power budgeting and spending insights.

    Args:
        user_id: User to analyze
        start_date: Start of period
        end_date: End of period
        db: Database session

    Returns:
        List of categories with spending amounts

    Example:
        [
            {"category": "Food & Restaurants", "amount": 500000, "count": 25},
            {"category": "Transport & Taxi", "amount": 150000, "count": 10},
            {"category": "Shopping & Retail", "amount": 1200000, "count": 5}
        ]
    """
    # Query for expense transactions (negative amounts) grouped by category
    stmt = (
        select(
            models.Transaction.category,
            func.coalesce(func.sum(models.Transaction.amount), 0).label("total_amount"),
            func.count(models.Transaction.id).label("transaction_count"),
        )
        .where(
            and_(
                models.Transaction.owner_id == user_id,
                models.Transaction.processed == True,
                models.Transaction.amount < 0,  # Only expenses
                models.Transaction.created_at.between(start_date, end_date),
            )
        )
        .group_by(models.Transaction.category)
        .order_by(desc("total_amount"))
    )

    result = await db.execute(stmt)
    rows = result.all()

    categories = []
    for row in rows:
        if row.category and row.total_amount:
            categories.append(
                {
                    "category": row.category,
                    "amount": abs(float(row.total_amount)),  # Make positive for display
                    "count": row.transaction_count,
                }
            )

    return categories


async def get_monthly_spending_trend(
    user_id: int, db: AsyncSession, months: int = 12
) -> List[Dict[str, Any]]:
    """
    Calculate monthly spending trend over time.
    Why: trends reveal behavior changes and seasonality.

    Args:
        user_id: User to analyze
        months: How many months back to analyze
        db: Database session

    Returns:
        Monthly spending data

    Example:
        [
            {"month": "2025-01", "total_spending": 2500000, "transaction_count": 45},
            {"month": "2024-12", "total_spending": 3200000, "transaction_count": 52}
        ]
    """
    # Calculate date range
    end_date = date.today()
    start_date = end_date.replace(day=1)  # First day of current month

    # Go back N months
    for _ in range(months - 1):
        if start_date.month == 1:
            start_date = start_date.replace(year=start_date.year - 1, month=12)
        else:
            start_date = start_date.replace(month=start_date.month - 1)

    # Group by month
    stmt = (
        select(
            func.date_trunc("month", models.Transaction.created_at).label("month"),
            func.coalesce(func.sum(models.Transaction.amount), 0).label(
                "monthly_spending"
            ),
            func.count(models.Transaction.id).label("transaction_count"),
        )
        .where(
            and_(
                models.Transaction.owner_id == user_id,
                models.Transaction.processed == True,
                models.Transaction.amount < 0,  # Only expenses
                models.Transaction.created_at >= start_date,
                models.Transaction.created_at <= end_date,
            )
        )
        .group_by("month")
        .order_by("month")
    )

    result = await db.execute(stmt)
    rows = result.all()

    monthly_data = []
    for row in rows:
        monthly_data.append(
            {
                "month": row.month.strftime("%Y-%m"),
                "total_spending": abs(float(row.monthly_spending)),
                "transaction_count": row.transaction_count,
            }
        )

    return monthly_data


async def get_top_merchants(
    user_id: int, start_date: date, end_date: date, db: AsyncSession, limit: int = 10
) -> List[Dict[str, Any]]:
    """
    Find top merchants by spending amount.
    Helps users see where they spend most money.
    Why: merchant concentration often indicates optimization opportunities.

    Args:
        user_id: User to analyze
        start_date: Start of period
        end_date: End of period
        limit: How many top merchants to return
        db: Database session

    Returns:
        Top merchants by spending

    Example:
        [
            {"merchant": "Makro", "amount": 1500000, "count": 8},
            {"merchant": "Evos", "amount": 450000, "count": 12}
        ]
    """
    stmt = (
        select(
            models.Transaction.merchant,
            func.coalesce(func.sum(models.Transaction.amount), 0).label("total_amount"),
            func.count(models.Transaction.id).label("transaction_count"),
        )
        .where(
            and_(
                models.Transaction.owner_id == user_id,
                models.Transaction.processed == True,
                models.Transaction.amount < 0,  # Only expenses
                models.Transaction.created_at.between(start_date, end_date),
                models.Transaction.merchant.isnot(None),
            )
        )
        .group_by(models.Transaction.merchant)
        .order_by(desc("total_amount"))
        .limit(limit)
    )

    result = await db.execute(stmt)
    rows = result.all()

    merchants = []
    for row in rows:
        if row.merchant and row.total_amount:
            merchants.append(
                {
                    "merchant": row.merchant,
                    "amount": abs(float(row.total_amount)),
                    "count": row.transaction_count,
                }
            )

    return merchants


async def get_income_analysis(
    user_id: int, start_date: date, end_date: date, db: AsyncSession
) -> Dict[str, Any]:
    """
    Analyze income sources and patterns.
    Helps users understand their income stability.
    Why: stable income improves budgeting and savings planning.

    Args:
        user_id: User to analyze
        start_date: Start of period
        end_date: End of period
        db: Database session

    Returns:
        Income analysis with categories and trends

    Example:
        {
            "total_income": 15000000,
            "income_by_category": [
                {"category": "Salary & Income", "amount": 12000000, "count": 2},
                {"category": "Transfer & Income", "amount": 3000000, "count": 3}
            ],
            "average_monthly": 5000000,
            "most_stable_month": "2025-01"
        }
    """
    # Get income transactions (positive amounts)
    stmt = select(
        func.coalesce(func.sum(models.Transaction.amount), 0).label("total_income"),
        func.count(models.Transaction.id).label("transaction_count"),
    ).where(
        and_(
            models.Transaction.owner_id == user_id,
            models.Transaction.processed == True,
            models.Transaction.amount > 0,  # Only income
            models.Transaction.created_at.between(start_date, end_date),
        )
    )

    result = await db.execute(stmt)
    income_row = result.first()

    # Get income by category
    category_stmt = (
        select(
            models.Transaction.category,
            func.coalesce(func.sum(models.Transaction.amount), 0).label(
                "category_amount"
            ),
            func.count(models.Transaction.id).label("category_count"),
        )
        .where(
            and_(
                models.Transaction.owner_id == user_id,
                models.Transaction.processed == True,
                models.Transaction.amount > 0,
                models.Transaction.created_at.between(start_date, end_date),
            )
        )
        .group_by(models.Transaction.category)
        .order_by(desc("category_amount"))
    )

    category_result = await db.execute(category_stmt)
    category_rows = category_result.all()

    # Calculate months in period for average
    months_diff = (
        (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1
    )
    total_income = (
        float(getattr(income_row, "total_income", 0))
        if income_row and getattr(income_row, "total_income", None)
        else 0
    )

    income_by_category = []
    for row in category_rows:
        if row.category and row.category_amount:
            income_by_category.append(
                {
                    "category": row.category,
                    "amount": float(row.category_amount),
                    "count": row.category_count,
                }
            )

    return {
        "total_income": total_income,
        "income_by_category": income_by_category,
        "average_monthly": total_income / months_diff if months_diff > 0 else 0,
        "total_transactions": (
            getattr(income_row, "transaction_count", 0) if income_row else 0
        ),
    }


async def calculate_savings_rate(
    user_id: int, start_date: date, end_date: date, db: AsyncSession
) -> Dict[str, Any]:
    """
    Calculate user's savings rate.
    Savings Rate = (Income - Expenses) / Income * 100
    Why: savings rate is a primary indicator of financial health.

    Good financial health generally means:
    - 20%+ savings rate = Excellent
    - 10-20% savings rate = Good
    - 5-10% savings rate = Fair
    - <5% savings rate = Needs improvement

    Args:
        user_id: User to analyze
        start_date: Start of period
        end_date: End of period
        db: Database session

    Returns:
        Savings rate analysis
    """
    # Get total income
    income_stmt = select(
        func.coalesce(func.sum(models.Transaction.amount), 0).label("total_income")
    ).where(
        and_(
            models.Transaction.owner_id == user_id,
            models.Transaction.processed == True,
            models.Transaction.amount > 0,
            models.Transaction.created_at.between(start_date, end_date),
        )
    )

    # Get total expenses
    expense_stmt = select(
        func.coalesce(func.sum(models.Transaction.amount), 0).label("total_expenses")
    ).where(
        and_(
            models.Transaction.owner_id == user_id,
            models.Transaction.processed == True,
            models.Transaction.amount < 0,
            models.Transaction.created_at.between(start_date, end_date),
        )
    )

    income_result = await db.execute(income_stmt)
    expense_result = await db.execute(expense_stmt)

    income_value = income_result.scalar()
    expense_value = expense_result.scalar()

    total_income = float(income_value) if income_value is not None else 0
    total_expenses = (
        abs(float(expense_value)) if expense_value is not None else 0
    )  # Make positive

    # Calculate savings rate
    if total_income == 0:
        savings_rate = 0
        savings_amount = 0
    else:
        savings_amount = total_income - total_expenses
        savings_rate = (savings_amount / total_income) * 100

    # Determine financial health level
    if savings_rate >= 20:
        health_level = "Excellent"
        recommendation = "Great job! You're saving 20% or more of your income."
    elif savings_rate >= 10:
        health_level = "Good"
        recommendation = "You're on the right track. Aim for 20% savings rate."
    elif savings_rate >= 5:
        health_level = "Fair"
        recommendation = "Consider reducing expenses to improve your savings rate."
    else:
        health_level = "Needs Improvement"
        recommendation = (
            "Focus on increasing income or reducing expenses significantly."
        )

    return {
        "savings_rate": round(savings_rate, 2),
        "savings_amount": savings_amount,
        "total_income": total_income,
        "total_expenses": total_expenses,
        "health_level": health_level,
        "recommendation": recommendation,
    }


async def create_budget_recommendations(
    user_id: int, start_date: date, end_date: date, db: AsyncSession
) -> List[Dict[str, Any]]:
    """
    Create budget recommendations based on spending patterns.
    Why: guides users toward more sustainable spending targets.

    Uses the 50/30/20 rule as a guideline:
    - 50% for Needs (Food, Transport, Bills, Health)
    - 30% for Wants (Entertainment, Shopping, Dining out)
    - 20% for Savings & Debt

    Args:
        user_id: User to analyze
        start_date: Start of period
        end_date: End of period
        db: Database session

    Returns:
        Budget recommendations by category

    Example:
        [
            {
                "category": "Food & Restaurants",
                "current_spending": 800000,
                "recommended_budget": 600000,
                "status": "over_budget",
                "recommendation": "Consider cooking more at home to reduce food expenses"
            }
        ]
    """
    # Get current spending by category
    current_spending = await get_user_spending_by_category(
        user_id, start_date, end_date, db
    )

    # Define budget percentages based on 50/30/20 rule
    budget_rules = {
        # Needs (50%)
        "Food & Restaurants": {"percentage": 15, "type": "needs"},
        "Transport & Taxi": {"percentage": 10, "type": "needs"},
        "Bills & Utilities": {"percentage": 15, "type": "needs"},
        "Health & Medicine": {"percentage": 5, "type": "needs"},
        "Education": {"percentage": 5, "type": "needs"},
        # Wants (30%)
        "Shopping & Retail": {"percentage": 15, "type": "wants"},
        "Entertainment & Leisure": {"percentage": 10, "type": "wants"},
        "Food & Restaurants": {
            "percentage": 15,
            "type": "needs",
        },  # Part wants, part needs
        # Other (5%)
        "Bank & Financial Services": {"percentage": 2, "type": "other"},
        "Other": {"percentage": 3, "type": "other"},
    }

    # Get user's total income for budget calculation
    income_stmt = select(
        func.coalesce(func.sum(models.Transaction.amount), 0).label("total_income")
    ).where(
        and_(
            models.Transaction.owner_id == user_id,
            models.Transaction.processed == True,
            models.Transaction.amount > 0,
            models.Transaction.created_at.between(start_date, end_date),
        )
    )

    income_result = await db.execute(income_stmt)
    income_value = income_result.scalar()
    total_income = float(income_value) if income_value is not None else 0

    # Calculate recommendations
    recommendations = []

    for category_spending in current_spending:
        category = category_spending["category"]
        current_amount = category_spending["amount"]

        # Get budget rule for this category
        rule = budget_rules.get(category, {"percentage": 5, "type": "other"})
        recommended_percentage = rule["percentage"]
        recommended_budget = (total_income * recommended_percentage) / 100

        # Determine status
        if current_amount > recommended_budget * 1.2:
            status = "over_budget"
            urgency = "high"
        elif current_amount > recommended_budget:
            status = "slightly_over"
            urgency = "medium"
        elif current_amount < recommended_budget * 0.8:
            status = "under_budget"
            urgency = "low"
        else:
            status = "on_budget"
            urgency = "low"

        # Generate specific recommendations
        recommendation_text = generate_category_recommendation(
            category, current_amount, recommended_budget, status
        )

        recommendations.append(
            {
                "category": category,
                "current_spending": current_amount,
                "recommended_budget": recommended_budget,
                "budget_percentage": recommended_percentage,
                "status": status,
                "urgency": urgency,
                "recommendation": recommendation_text,
                "type": rule["type"],
            }
        )

    # Sort by urgency and amount
    recommendations.sort(
        key=lambda x: (
            {"high": 0, "medium": 1, "low": 2}[x["urgency"]],
            -x["current_spending"],
        )
    )

    return recommendations


def generate_category_recommendation(
    category: str, current_amount: float, recommended_budget: float, status: str
) -> str:
    """
    Generate specific recommendations for a category.
    Why: targeted suggestions increase the chance of behavior change.
    """
    if status == "on_budget":
        return f"Great! You're within budget for {category}."

    if status == "under_budget":
        return f"Good job managing {category}. You're spending less than recommended."

    # Over budget recommendations
    category_recommendations = {
        "Food & Restaurants": [
            "Try meal planning and cooking at home more often",
            "Consider packing lunch for work/school",
            "Look for restaurant deals and happy hours",
            "Buy groceries in bulk when possible",
        ],
        "Transport & Taxi": [
            "Consider using public transportation more",
            "Try walking or cycling for short distances",
            "Compare taxi apps for better prices",
            "Consider carpooling with colleagues",
        ],
        "Shopping & Retail": [
            "Create a shopping list and stick to it",
            "Wait 24 hours before making non-essential purchases",
            "Compare prices online before buying",
            "Consider second-hand options when possible",
        ],
        "Entertainment & Leisure": [
            "Look for free entertainment options in your city",
            "Consider streaming services instead of cinema",
            "Take advantage of happy hour and weekday discounts",
            "Plan entertainment budget in advance",
        ],
        "Bills & Utilities": [
            "Review your subscriptions and cancel unused ones",
            "Consider energy-saving measures to reduce bills",
            "Shop around for better internet/phone plans",
            "Use automatic payments to avoid late fees",
        ],
    }

    recommendations = category_recommendations.get(
        category,
        [
            f"Review your {category} expenses and identify areas to reduce",
            f"Set a monthly budget for {category} and track it regularly",
            "Look for alternatives that cost less but provide similar value",
        ],
    )

    # Pick the most relevant recommendation based on how much over budget
    over_percentage = ((current_amount - recommended_budget) / recommended_budget) * 100

    if over_percentage > 50:
        # Very over budget - more urgent recommendation
        return f"Urgent: You're spending {over_percentage:.0f}% more than recommended for {category}. {recommendations[0]}"
    else:
        # Moderately over budget
        return f"You're spending {over_percentage:.0f}% over budget for {category}. {recommendations[0]}"


async def get_user_stats_snapshot(
    user_id: int, db: AsyncSession
) -> Optional[Dict[str, Any]]:
    """
    Fetch the latest precomputed user stats (updated in load.py).

    This avoids re-aggregating lifetime totals during dashboard requests.
    Why: cached stats reduce query cost and improve response time.
    """
    stmt = select(models.UserStats).where(models.UserStats.user_id == user_id)
    result = await db.execute(stmt)
    stats = result.scalar_one_or_none()

    if not stats:
        return None

    return {
        "total_transactions": int(stats.total_transactions or 0),
        "total_income": float(stats.total_income or 0),
        "total_expense": float(stats.total_expense or 0),
        "avg_transaction_amount": float(stats.avg_transaction_amount or 0),
        "spent_by_category": stats.spent_by_category or {},
        "updated_at": stats.updated_at.isoformat() if stats.updated_at else None,
    }


async def get_financial_dashboard(user_id: int, db: AsyncSession) -> Dict[str, Any]:
    """
    Get complete financial dashboard data.
    This is the main function that powers the user's financial overview.
    Why: provides one consolidated payload for UI and assistants.

    Args:
        user_id: User to analyze
        db: Database session

    Returns:
        Complete dashboard data with all insights
    """
    # Date ranges for different analyses (current month and trailing windows)
    today = date.today()
    this_month_start = today.replace(day=1)
    last_month_start = (this_month_start.replace(day=1) - timedelta(days=1)).replace(
        day=1
    )

    # 1. Current month spending by category
    current_month_spending = await get_user_spending_by_category(
        user_id, this_month_start, today, db
    )

    # 2. Income analysis (this month)
    current_month_income = await get_income_analysis(
        user_id, this_month_start, today, db
    )

    # 3. Savings rate (last 3 months for stability)
    three_months_ago = today.replace(day=1) - timedelta(days=90)
    savings_analysis = await calculate_savings_rate(
        user_id, three_months_ago, today, db
    )

    # 4. Monthly trend (last 6 months)
    monthly_trend = await get_monthly_spending_trend(user_id, db, 6)

    # 5. Top merchants (this month)
    top_merchants = await get_top_merchants(user_id, this_month_start, today, db, 5)

    # 6. Budget recommendations (last 3 months)
    budget_recommendations = await create_budget_recommendations(
        user_id, three_months_ago, today, db
    )

    # 7. Calculate key metrics
    total_spending = sum(cat["amount"] for cat in current_month_spending)
    total_income = current_month_income["total_income"]
    net_cash_flow = total_income - total_spending

    # 8. Generate insights from trends and savings
    insights = generate_financial_insights(
        current_month_spending, current_month_income, savings_analysis, monthly_trend
    )

    # 9. Pull cached lifetime stats (from load step) if available
    lifetime_stats = await get_user_stats_snapshot(user_id, db)

    return {
        "period": {
            "start": this_month_start.isoformat(),
            "end": today.isoformat(),
            "type": "current_month",
        },
        "summary": {
            "total_income": total_income,
            "total_spending": total_spending,
            "net_cash_flow": net_cash_flow,
            "savings_rate": savings_analysis["savings_rate"],
            "financial_health": savings_analysis["health_level"],
        },
        "spending_by_category": current_month_spending,
        "income_breakdown": current_month_income,
        "monthly_trend": monthly_trend,
        "top_merchants": top_merchants,
        "budget_recommendations": budget_recommendations,
        "insights": insights,
        "lifetime_summary": lifetime_stats,
    }


def generate_financial_insights(
    spending_by_category: List[Dict],
    income_analysis: Dict,
    savings_analysis: Dict,
    monthly_trend: List[Dict],
) -> List[Dict[str, Any]]:
    """
    Generate actionable financial insights.

    AI-like insights that help users improve their finances.
    Why: insights turn metrics into understandable next steps.
    """
    insights = []

    # Spending trend insight
    if len(monthly_trend) >= 2:
        current_month = monthly_trend[-1]["total_spending"]
        previous_month = monthly_trend[-2]["total_spending"]

        if current_month > previous_month * 1.2:
            insights.append(
                {
                    "type": "warning",
                    "title": "Spending Increased Significantly",
                    "message": f"Your spending increased by {((current_month/previous_month - 1) * 100):.0f}% this month. Review your largest expense categories.",
                    "actionable": True,
                }
            )
        elif current_month < previous_month * 0.8:
            insights.append(
                {
                    "type": "positive",
                    "title": "Great Job Reducing Spending!",
                    "message": f"Your spending decreased by {((1 - current_month/previous_month) * 100):.0f}% this month. Keep it up!",
                    "actionable": False,
                }
            )

    # Savings rate insight
    savings_rate = savings_analysis["savings_rate"]
    if savings_rate < 5:
        insights.append(
            {
                "type": "alert",
                "title": "Low Savings Rate",
                "message": f"Your savings rate is {savings_rate:.1f}%. Consider reducing expenses or increasing income to reach at least 10%.",
                "actionable": True,
            }
        )
    elif savings_rate >= 20:
        insights.append(
            {
                "type": "excellent",
                "title": "Excellent Savings Habits!",
                "message": f"Your savings rate is {savings_rate:.1f}%. You're building great financial security!",
                "actionable": False,
            }
        )

    # Category-specific insights
    if spending_by_category:
        top_category = spending_by_category[0]
        if top_category["amount"] > 1000000:  # Over 1 million in top category
            insights.append(
                {
                    "type": "info",
                    "title": f"Top Expense: {top_category['category']}",
                    "message": f"You spent {top_category['amount']:,.0f} UZS on {top_category['category']} this month. Is this aligned with your priorities?",
                    "actionable": True,
                }
            )

    # Income stability insight
    if income_analysis["income_by_category"]:
        salary_income = next(
            (
                cat
                for cat in income_analysis["income_by_category"]
                if cat["category"] == "Salary & Income"
            ),
            None,
        )

        if salary_income and salary_income["count"] == 1:
            insights.append(
                {
                    "type": "info",
                    "title": "Income Source Diversity",
                    "message": "Consider diversifying your income sources for better financial stability.",
                    "actionable": True,
                }
            )

    return insights

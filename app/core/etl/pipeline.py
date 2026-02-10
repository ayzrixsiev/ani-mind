import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, date
from enum import Enum
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update, exists, and_, text

from app.core import models
from app.core.etl import ingest, transform, load, aggregate


# -----------------------------------------------------------------------------
# PIPELINE MODULE - Orchestration
# Purpose: Run the complete ETL pipeline in correct order, handle errors, monitor pipeline performance, show etl status and logs
# Why: Automate entire process
# -----------------------------------------------------------------------------


class PipelineStatus(Enum):
    """Pipeline execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PipelineStep(Enum):
    """Individual pipeline steps."""

    INGEST = "ingest"
    TRANSFORM = "transform"
    LOAD = "load"
    AGGREGATE = "aggregate"


# Configure logging for pipeline
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PipelineLogger:
    """Custom logger for ETL pipeline operations."""

    def __init__(self, user_id: int):
        """
        Initialize a pipeline logger scoped to a specific user.

        Args:
            user_id: User whose pipeline run is being logged.

        Example:
            logger = PipelineLogger(user_id)
        """
        self.user_id = user_id
        self.start_time = datetime.now()
        self.logs = []

    def log(self, step: str, message: str, level: str = "info"):
        """
        Log a pipeline message.
        Why: centralized logs make debugging and auditing easier.
        """
        timestamp = datetime.now().isoformat()
        log_entry = {
            "timestamp": timestamp,
            "step": step,
            "message": message,
            "level": level,
            "elapsed_seconds": (datetime.now() - self.start_time).total_seconds(),
        }
        self.logs.append(log_entry)

        # Also log to console
        if level == "error":
            logger.error(f"[User {self.user_id}] {step}: {message}")
        elif level == "warning":
            logger.warning(f"[User {self.user_id}] {step}: {message}")
        else:
            logger.info(f"[User {self.user_id}] {step}: {message}")

    def get_logs(self) -> List[Dict[str, Any]]:
        """
        Get all logs for this pipeline run.
        Why: consumers may need step-by-step execution details.
        """
        return self.logs

    def get_summary(self) -> Dict[str, Any]:
        """
        Get pipeline execution summary.
        Why: provides a compact overview for UI and monitoring.
        """
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()

        return {
            "user_id": self.user_id,
            "start_time": self.start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "total_logs": len(self.logs),
            "logs": self.logs,
        }


async def run_ingest_pipeline(
    user_id: int,
    db: AsyncSession,
    account_id: Optional[int] = None,
    file_content: Optional[bytes] = None,
    api_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Run the ingest step of the pipeline.

    This handles both CSV file uploads and API data fetching.
    Why: centralizes ingest behavior and error handling.

    Args:
        user_id: User to ingest data for
        account_id: Account to associate with transactions
        file_content: CSV file content (for file uploads)
        api_config: API configuration (for API data fetching)
        db: Database session

    Returns:
        Ingestion results and statistics
    """
    pipeline_logger = PipelineLogger(user_id)
    pipeline_logger.log("ingest", "Starting data ingestion...")

    try:
        if file_content:
            # CSV file ingestion
            pipeline_logger.log("ingest", "Processing CSV file...")
            result = await ingest.ingest_from_csv(file_content, user_id, account_id, db)
            pipeline_logger.log(
                "ingest",
                f"CSV processed: {result['saved']} saved, {result['duplicates']} duplicates",
            )

        elif api_config:
            # API data ingestion (generic by default; supports custom APIs)
            api_type = api_config.get("type", "generic")
            pipeline_logger.log("ingest", f"Fetching data from {api_type} API...")

            if account_id is None:
                raise ValueError("account_id is required for API ingestion")

            # For now, expect a URL/headers payload and route through ingest_from_api.
            # Provider-specific helpers can be layered later if needed.
            url = api_config.get("url")
            headers = api_config.get("headers", {})
            params = api_config.get("params")
            source = api_config.get("source", api_type)

            if not url:
                raise ValueError("api_config.url is required for API ingestion")

            result = await ingest.ingest_from_api(
                url=url,
                headers=headers,
                user_id=user_id,
                account_id=account_id,
                params=params,
                db=db,
                source=source,
            )

            pipeline_logger.log(
                "ingest", f"API data processed: {result['saved']} saved"
            )
        else:
            raise ValueError("Either file_content or api_config must be provided")

        pipeline_logger.log("ingest", "Data ingestion completed successfully")
        return {
            "status": PipelineStatus.COMPLETED,
            "result": result,
            "logs": pipeline_logger.get_logs(),
        }

    except Exception as e:
        pipeline_logger.log("ingest", f"Ingestion failed: {str(e)}", "error")
        return {
            "status": PipelineStatus.FAILED,
            "error": str(e),
            "logs": pipeline_logger.get_logs(),
        }


async def run_transform_pipeline(user_id: int, db: AsyncSession) -> Dict[str, Any]:
    """
    Run the transform step of the pipeline.

    This cleans and normalizes all unprocessed transactions.
    Why: ensures all raw transactions become analysis-ready.

    Args:
        user_id: User to transform data for
        db: Database session

    Returns:
        Transformation results and statistics
    """
    pipeline_logger = PipelineLogger(user_id)
    pipeline_logger.log("transform", "Starting data transformation...")

    try:
        # Transform all unprocessed transactions
        result = await transform.transform_all_unprocessed(user_id, db)

        pipeline_logger.log(
            "transform",
            f"Transformation completed: {result['processed']}/{result['total']} processed",
        )

        if result["failed"] > 0:
            pipeline_logger.log(
                "transform",
                f"{result['failed']} transactions failed to transform",
                "warning",
            )

        return {
            "status": PipelineStatus.COMPLETED,
            "result": result,
            "logs": pipeline_logger.get_logs(),
        }

    except Exception as e:
        pipeline_logger.log("transform", f"Transformation failed: {str(e)}", "error")
        return {
            "status": PipelineStatus.FAILED,
            "error": str(e),
            "logs": pipeline_logger.get_logs(),
        }


async def run_load_pipeline(user_id: int, db: AsyncSession) -> Dict[str, Any]:
    """
    Run the load step of the pipeline.

    This optimizes storage and updates balances.
    Why: keeps derived data (balances, stats) accurate and fast to query.

    Args:
        user_id: User to load data for
        db: Database session

    Returns:
        Loading results and statistics
    """
    pipeline_logger = PipelineLogger(user_id)
    pipeline_logger.log("load", "Starting data loading...")

    try:
        # Load processed data and update balances
        result = await load.load_processed_data(user_id, db)

        pipeline_logger.log(
            "load",
            f"Data loading completed: {result['accounts_updated']} accounts updated",
        )

        if not result["data_valid"]:
            pipeline_logger.log("load", "Data validation issues found", "warning")

        return {
            "status": PipelineStatus.COMPLETED,
            "result": result,
            "logs": pipeline_logger.get_logs(),
        }

    except Exception as e:
        pipeline_logger.log("load", f"Loading failed: {str(e)}", "error")
        return {
            "status": PipelineStatus.FAILED,
            "error": str(e),
            "logs": pipeline_logger.get_logs(),
        }


async def run_aggregate_pipeline(user_id: int, db: AsyncSession) -> Dict[str, Any]:
    """
    Run the aggregate step of the pipeline.

    This generates insights and analytics.
    Why: users need summarized insights rather than raw transactions.

    Args:
        user_id: User to aggregate data for
        db: Database session

    Returns:
        Aggregation results and insights
    """
    pipeline_logger = PipelineLogger(user_id)
    pipeline_logger.log("aggregate", "Starting data aggregation...")

    try:
        # Generate financial dashboard data
        dashboard_data = await aggregate.get_financial_dashboard(user_id, db)

        pipeline_logger.log(
            "aggregate",
            f"Aggregation completed: {len(dashboard_data['insights'])} insights generated",
        )

        return {
            "status": PipelineStatus.COMPLETED,
            "result": dashboard_data,
            "logs": pipeline_logger.get_logs(),
        }

    except Exception as e:
        pipeline_logger.log("aggregate", f"Aggregation failed: {str(e)}", "error")
        return {
            "status": PipelineStatus.FAILED,
            "error": str(e),
            "logs": pipeline_logger.get_logs(),
        }


async def run_complete_etl_pipeline(
    user_id: int,
    db: AsyncSession,
    account_id: Optional[int] = None,
    file_content: Optional[bytes] = None,
    api_config: Optional[Dict[str, Any]] = None,
    steps_to_run: Optional[List[PipelineStep]] = None,
) -> Dict[str, Any]:
    """
    Run the complete ETL pipeline.

    This is the main orchestrator that runs all steps in sequence.
    Why: provides a single entry point for end-to-end processing.

    Args:
        user_id: User to run pipeline for
        account_id: Account to associate with new data
        file_content: CSV file content (optional)
        api_config: API configuration (optional)
        steps_to_run: Which steps to run (None = all steps)
        db: Database session

    Returns:
        Complete pipeline results and statistics

    Example flow:
        1. Ingest: CSV/API → Raw transactions
        2. Transform: Raw → Clean transactions
        3. Load: Clean → Optimized storage
        4. Aggregate: Optimized → Insights
    """
    # Default to running all steps
    if steps_to_run is None:
        steps_to_run = [
            PipelineStep.INGEST,
            PipelineStep.TRANSFORM,
            PipelineStep.LOAD,
            PipelineStep.AGGREGATE,
        ]

    # Main pipeline logger
    pipeline_logger = PipelineLogger(user_id)
    pipeline_logger.log("pipeline", f"Starting ETL pipeline for user {user_id}")
    pipeline_logger.log(
        "pipeline", f"Steps to run: {[step.value for step in steps_to_run]}"
    )

    # Track results for each step
    step_results = {}
    pipeline_status = PipelineStatus.RUNNING

    try:
        # STEP 1: INGEST (if new data provided)
        if PipelineStep.INGEST in steps_to_run and (file_content or api_config):
            pipeline_logger.log("pipeline", "Step 1: Ingestion")
            step_results["ingest"] = await run_ingest_pipeline(
                user_id, db, account_id, file_content, api_config
            )

            if step_results["ingest"]["status"] == PipelineStatus.FAILED:
                pipeline_status = PipelineStatus.FAILED
                raise Exception(f"Ingestion failed: {step_results['ingest']['error']}")

        # STEP 2: TRANSFORM
        if PipelineStep.TRANSFORM in steps_to_run:
            pipeline_logger.log("pipeline", "Step 2: Transformation")
            step_results["transform"] = await run_transform_pipeline(user_id, db)

            if step_results["transform"]["status"] == PipelineStatus.FAILED:
                pipeline_status = PipelineStatus.FAILED
                raise Exception(
                    f"Transformation failed: {step_results['transform']['error']}"
                )

        # STEP 3: LOAD
        if PipelineStep.LOAD in steps_to_run:
            pipeline_logger.log("pipeline", "Step 3: Loading")
            step_results["load"] = await run_load_pipeline(user_id, db)

            if step_results["load"]["status"] == PipelineStatus.FAILED:
                pipeline_status = PipelineStatus.FAILED
                raise Exception(f"Loading failed: {step_results['load']['error']}")

        # STEP 4: AGGREGATE
        if PipelineStep.AGGREGATE in steps_to_run:
            pipeline_logger.log("pipeline", "Step 4: Aggregation")
            step_results["aggregate"] = await run_aggregate_pipeline(user_id, db)

            if step_results["aggregate"]["status"] == PipelineStatus.FAILED:
                pipeline_status = PipelineStatus.FAILED
                raise Exception(
                    f"Aggregation failed: {step_results['aggregate']['error']}"
                )

        # Pipeline completed successfully
        pipeline_status = PipelineStatus.COMPLETED
        pipeline_logger.log("pipeline", "ETL pipeline completed successfully")

    except Exception as e:
        pipeline_status = PipelineStatus.FAILED
        pipeline_logger.log("pipeline", f"ETL pipeline failed: {str(e)}", "error")

    # Compile final results
    final_result = {
        "status": pipeline_status,
        "user_id": user_id,
        "steps_run": [step.value for step in steps_to_run],
        "step_results": step_results,
        "pipeline_summary": pipeline_logger.get_summary(),
        "total_duration": pipeline_logger.get_summary()["duration_seconds"],
    }

    return final_result


async def get_pipeline_status(user_id: int, db: AsyncSession) -> Dict[str, Any]:
    """
    Get current pipeline status for a user.

    This shows:
    - How many transactions are unprocessed
    - Last pipeline run status
    - Data quality metrics
    Why: helps users and ops decide when to re-run ETL.

    Args:
        user_id: User to check status for
        db: Database session

    Returns:
        Current pipeline status and metrics
    """
    # Get transaction counts
    total_stmt = select(func.count(models.Transaction.id)).where(
        models.Transaction.owner_id == user_id
    )
    unprocessed_stmt = select(func.count(models.Transaction.id)).where(
        and_(
            models.Transaction.owner_id == user_id,
            models.Transaction.processed == False,
        )
    )

    total_result = await db.execute(total_stmt)
    unprocessed_result = await db.execute(unprocessed_stmt)

    total_transactions = total_result.scalar()
    unprocessed_transactions = unprocessed_result.scalar()

    # Calculate processing percentage
    if (
        total_transactions is not None
        and total_transactions > 0
        and unprocessed_transactions is not None
    ):
        processing_percentage = (
            (total_transactions - unprocessed_transactions) / total_transactions * 100
        )
    else:
        processing_percentage = 100

    processed_count = (
        (total_transactions - unprocessed_transactions)
        if total_transactions is not None and unprocessed_transactions is not None
        else 0
    )

    return {
        "user_id": user_id,
        "total_transactions": (
            total_transactions if total_transactions is not None else 0
        ),
        "unprocessed_transactions": (
            unprocessed_transactions if unprocessed_transactions is not None else 0
        ),
        "processed_transactions": processed_count,
        "processing_percentage": round(processing_percentage, 2),
        "needs_processing": (
            unprocessed_transactions if unprocessed_transactions is not None else 0
        )
        > 0,
        "status": (
            "ready"
            if (unprocessed_transactions if unprocessed_transactions is not None else 0)
            == 0
            else "needs_transform"
        ),
    }


async def get_pipeline_health_check(db: AsyncSession) -> Dict[str, Any]:
    """
    Perform a health check on the entire pipeline system.

    This is useful for monitoring and debugging:
    - Check database connectivity
    - Verify table structures
    - Check for performance issues
    - Validate data integrity

    Args:
        db: Database session

    Returns:
        System health status and recommendations
    Why: surfaces operational issues before they affect users.
    """
    health_status = {
        "overall_status": "healthy",
        "checks": [],
        "recommendations": [],
        "timestamp": datetime.now().isoformat(),
    }

    try:
        # Check 1: Database connectivity
        await db.execute(text("SELECT 1"))
        health_status["checks"].append(
            {
                "name": "database_connectivity",
                "status": "pass",
                "message": "Database connection successful",
            }
        )
    except Exception as e:
        health_status["checks"].append(
            {
                "name": "database_connectivity",
                "status": "fail",
                "message": f"Database connection failed: {str(e)}",
            }
        )
        health_status["overall_status"] = "unhealthy"

    try:
        # Check 2: Table structure
        await db.execute(text("SELECT COUNT(*) FROM transactions LIMIT 1"))
        await db.execute(text("SELECT COUNT(*) FROM accounts LIMIT 1"))
        await db.execute(text("SELECT COUNT(*) FROM users_table LIMIT 1"))

        health_status["checks"].append(
            {
                "name": "table_structure",
                "status": "pass",
                "message": "All required tables exist",
            }
        )
    except Exception as e:
        health_status["checks"].append(
            {
                "name": "table_structure",
                "status": "fail",
                "message": f"Table structure issue: {str(e)}",
            }
        )
        health_status["overall_status"] = "unhealthy"

    try:
        # Check 3: Data quality
        unprocessed_stmt = select(func.count(models.Transaction.id)).where(
            models.Transaction.processed == False
        )
        unprocessed_count = (await db.execute(unprocessed_stmt)).scalar()

        if unprocessed_count is not None and unprocessed_count > 1000:
            health_status["checks"].append(
                {
                    "name": "data_quality",
                    "status": "warning",
                    "message": f"High number of unprocessed transactions: {unprocessed_count}",
                }
            )
            health_status["recommendations"].append(
                "Consider running transform pipeline to process unprocessed transactions"
            )
        else:
            health_status["checks"].append(
                {
                    "name": "data_quality",
                    "status": "pass",
                    "message": f"Low unprocessed transaction count: {unprocessed_count}",
                }
            )
    except Exception as e:
        health_status["checks"].append(
            {
                "name": "data_quality",
                "status": "fail",
                "message": f"Data quality check failed: {str(e)}",
            }
        )

    return health_status


async def schedule_pipeline_run(
    user_id: int, db: AsyncSession, schedule_type: str = "daily"
) -> Dict[str, Any]:
    """
    Schedule regular pipeline runs.

    This would integrate with a task scheduler like Celery
    for automated pipeline execution.
    Why: keeps data fresh without manual triggers.

    Args:
        user_id: User to schedule pipeline for
        schedule_type: "daily", "weekly", "monthly"
        db: Database session

    Returns:
        Scheduling configuration
    """
    # This is a placeholder for scheduler integration
    # In a real implementation, you would:
    # 1. Store schedule in database
    # 2. Configure Celery beat or equivalent
    # 3. Set up monitoring and alerts

    schedule_config = {
        "user_id": user_id,
        "schedule_type": schedule_type,
        "enabled": True,
        "last_run": None,
        "next_run": datetime.now().isoformat(),
        "steps_to_run": [
            step.value
            for step in [
                PipelineStep.TRANSFORM,
                PipelineStep.LOAD,
                PipelineStep.AGGREGATE,
            ]
        ],
    }

    return schedule_config


async def rollback_pipeline(
    user_id: int, step: PipelineStep, db: AsyncSession
) -> Dict[str, Any]:
    """
    Rollback a specific pipeline step.

    Useful for:
    - Undoing failed transformations
    - Recovering from data corruption
    - Testing pipeline changes

    Args:
        user_id: User to rollback for
        step: Which step to rollback
        db: Database session

    Returns:
        Rollback results
    Why: provides a safety valve when transformations introduce errors.
    """
    logger.info(f"Starting rollback for user {user_id}, step: {step.value}")

    if step == PipelineStep.TRANSFORM:
        # Mark all transactions as unprocessed
        stmt = (
            update(models.Transaction)
            .where(
                models.Transaction.owner_id == user_id,
                models.Transaction.processed == True,
            )
            .values(processed=False)
        )

        await db.execute(stmt)
        await db.commit()

        return {
            "status": "success",
            "message": f"Rolled back transform step for user {user_id}",
            "transactions_affected": "all processed transactions",
        }

    elif step == PipelineStep.LOAD:
        # Reset account balances to 0 (they'll be recalculated on next load)
        stmt = (
            update(models.Account)
            .where(models.Account.owner_id == user_id)
            .values(balance=0, updated_at=datetime.now())
        )

        await db.execute(stmt)
        await db.commit()

        return {
            "status": "success",
            "message": f"Rolled back load step for user {user_id}",
            "accounts_affected": "all user accounts",
        }

    else:
        return {
            "status": "error",
            "message": f"Rollback not supported for step: {step.value}",
        }

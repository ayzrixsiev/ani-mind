# Fin-Pulse

**Fin-Pulse** is a modern, containerized backend API designed to help users ingest, process, and gain insights from their personal financial data.  

Through secure transaction logging, automated ETL pipelines, analytical modeling, and intelligent pattern detection, it turns raw financial logs into clear, actionable understanding of spending, trends, and opportunities.

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688?style=flat&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-1E88E5?style=flat&logo=sqlalchemy&logoColor=white)](https://www.sqlalchemy.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat&logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![Docker](https://img.shields.io/badge/Docker-✓-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com)
[![pytest](https://img.shields.io/badge/pytest-8.0+-0A9EDC?style=flat&logo=pytest&logoColor=white)](https://docs.pytest.org)
[![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-CI/CD-2088FF?style=flat&logo=github-actions&logoColor=white)](https://github.com/features/actions)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Current Capabilities

- Secure user authentication & role-based access (JWT, regular users + admin)
- Personal transaction logging (description, amount, date, category, type: income/expense)
- Ownership rules — users can only view/edit/delete their own entries
- Financial statistics endpoint (total spend/income, averages, category breakdowns, monthly trends)
- Admin user management endpoint
- Fully asynchronous PostgreSQL integration (SQLAlchemy 2.0)
- Database schema migrations (Alembic)
- Comprehensive automated testing suite
  - Isolated test database
  - Transaction rollbacks for test independence
  - Async HTTP client testing (httpx)
- Containerized development & deployment (Docker + Docker Compose)
- Continuous integration pipeline (GitHub Actions)

## Technology Roadmap

- Transaction data ingestion from CSV/JSON or external APIs (Plaid sandbox, bank exports)
- Automated ETL pipelines for categorization, cleaning, and aggregation (Prefect / Airflow)
- Analytical schema & materialized views for fast queries (monthly aggregates, trends)
- Anomaly detection & spending pattern insights (scikit-learn)
- Model experiment tracking & versioning (MLflow)
- Production model serving (BentoML / KServe)
- Performance monitoring & data drift detection (Evidently AI)
- Advanced orchestration & GitOps (Kubernetes, ArgoCD / Flux)

## Quick Start

```bash
# Clone repository
git clone https://github.com/ayzrixsiev/flowmind.git
cd flowmind

# Copy environment file
cp .env.example .env

# Start services (PostgreSQL + API)
docker compose up -d --build

# Apply migrations
docker compose exec api alembic upgrade head

# Open interactive API documentation
# → http://localhost:8000/docs

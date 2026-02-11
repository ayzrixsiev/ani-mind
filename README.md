# Fin‑Pulse

Fin‑Pulse is an AI‑ready finance backend that ingests raw transactions, standardizes them through a full ETL pipeline, and serves analytics and explanations through FastAPI. It is built to feel fast and trustworthy: clean data in, consistent insights out.

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688?style=flat&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-1E88E5?style=flat&logo=sqlalchemy&logoColor=white)](https://www.sqlalchemy.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat&logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![Docker](https://img.shields.io/badge/Docker-✓-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com)
[![pytest](https://img.shields.io/badge/pytest-8.0+-0A9EDC?style=flat&logo=pytest&logoColor=white)](https://docs.pytest.org)
[![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-CI/CD-2088FF?style=flat&logo=github-actions&logoColor=white)](https://github.com/features/actions)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## What It Does
- Ingests CSV and API data with deduplication
- Cleans and normalizes dates, amounts, merchants, and categories
- Updates account balances and cached user stats
- Generates dashboards: trends, spending by category, income stability, budgets
- Serves analytics and AI‑style explanations via FastAPI
- Includes health checks, status monitoring, and robust ETL logging

## ETL Pipeline
Ingest → Transform → Load → Aggregate  
Each stage is a dedicated module with validation, metrics, and clear failure handling.

## Run Locally (Docker)
```bash
docker compose up --build
```

Open API docs:  
`http://localhost:8000/docs`

## Run Tests
```bash
docker compose up --build -d
docker compose exec -T api pytest -v
```

## Tech Stack
- Python, FastAPI, SQLAlchemy 2.0
- PostgreSQL, Alembic
- Async DB access with `psycopg`
- Docker, Docker Compose
- Pytest + GitHub Actions

## License
Apache 2.0

# Ani-mind

**Ani-mind** is a modern, containerized backend API designed to help users build and manage personal anime watchlists while discovering new titles that match their tastes.  

Through thoughtful data collection (titles, ratings, genres, status) and intelligent recommendations, it aims to make finding enjoyable anime effortless and personalized.

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
- Personal anime list management (title, rating 0–10, watching status, genres)
- Ownership rules — users can only view/edit/delete their own entries
- User statistics endpoint (total watched, average rating, top genres)
- Admin user deletion endpoint
- Fully asynchronous PostgreSQL integration (SQLAlchemy 2.0)
- Database schema migrations (Alembic)
- Comprehensive automated testing suite
  - Isolated test database
  - Transaction rollbacks for test independence
  - Async HTTP client testing (httpx)
- Containerized development & deployment (Docker + Docker Compose)
- Continuous integration pipeline (GitHub Actions)

## Technology Roadmap

- External anime metadata integration (Jikan / MyAnimeList API)
- Content-based recommendation engine (genre similarity + embeddings)
- Model experiment tracking & versioning (MLflow)
- Production model serving (BentoML / KServe)
- Model performance & data drift monitoring (Evidently AI)
- Advanced orchestration & GitOps (Kubernetes, ArgoCD / Flux)

## Quick Start

```bash
# Clone repository
git clone https://github.com/ayzrixsiev/ani-mind.git
cd ani-mind

# Copy environment file
cp .env.example .env

# Start services (PostgreSQL + API)
docker compose up -d --build

# Apply migrations
docker compose exec api alembic upgrade head

# Open interactive API documentation
# http://localhost:8000/docs

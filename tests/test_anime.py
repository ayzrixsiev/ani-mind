import uuid
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_get_anime_list_empty(client: AsyncClient, auth_headers_user):
    """Empty list when user has no anime"""
    response = await client.get("/anime/list", headers=auth_headers_user)
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_add_anime(client: AsyncClient, auth_headers_user):
    """Can add new anime to list"""
    payload = {
        "title": f"Test Anime {uuid.uuid4().hex[:8]}",
        "rating": 9,
        "status": "Completed",
        "genres": ["Fantasy", "Action", "Romance"],
    }
    response = await client.post("/anime/list", json=payload, headers=auth_headers_user)

    if response.status_code != 201:
        print(f"ERROR Response: {response.status_code}")
        print(f"ERROR Detail: {response.json()}")

    assert response.status_code == 201
    data = response.json()
    assert data["title"].startswith("Test Anime ")
    assert data["rating"] == 9
    assert "id" in data
    assert isinstance(data["genres"], list)


@pytest.mark.asyncio
async def test_get_anime_list_after_add(
    client: AsyncClient, auth_headers_user, test_anime
):
    """List returns added anime"""
    response = await client.get("/anime/list", headers=auth_headers_user)
    assert response.status_code == 200
    data = response.json()
    assert len(data) >= 1
    assert any(anime["id"] == test_anime.id for anime in data)


@pytest.mark.asyncio
async def test_update_anime(client: AsyncClient, auth_headers_user, test_anime):
    """Can update existing anime"""
    update_payload = {
        "rating": 10,
        "status": "Completed",
        "genres": ["Action", "Adventure", "Fantasy"],
    }
    response = await client.patch(
        f"/anime/list/{test_anime.id}", json=update_payload, headers=auth_headers_user
    )
    assert response.status_code == 200
    updated = response.json()
    assert updated["rating"] == 10
    assert updated["status"] == "Completed"
    assert set(updated["genres"]) == {"Action", "Adventure", "Fantasy"}


@pytest.mark.asyncio
async def test_delete_anime(client: AsyncClient, auth_headers_user, test_anime):
    """Can delete anime from list"""
    response = await client.delete(
        f"/anime/list/{test_anime.id}", headers=auth_headers_user
    )
    assert response.status_code == 200

    # Verify it's gone
    list_response = await client.get("/anime/list", headers=auth_headers_user)
    data = list_response.json()
    assert not any(a["id"] == test_anime.id for a in data)


@pytest.mark.asyncio
async def test_unauthorized_access(client: AsyncClient):
    """401 when no auth header"""
    response = await client.get("/anime/list")
    assert response.status_code == 401

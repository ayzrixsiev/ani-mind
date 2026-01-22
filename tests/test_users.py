import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_signup_success(client: AsyncClient):
    """Successful signup returns 201 and correct user data (no password in response)"""
    payload = {
        "email": "newuser123@example.com",
        "role": "user",
        "password": "strongpass123",
    }
    response = await client.post("/profile/signup", json=payload)

    assert response.status_code == 201
    data = response.json()
    assert data["email"] == payload["email"]
    assert "id" in data
    assert "password" not in data
    assert data["role"] == "user"


@pytest.mark.asyncio
async def test_signup_duplicate_email(client: AsyncClient):
    """Duplicate email returns 409 Conflict"""
    payload = {
        "email": "duplicate@example.com",
        "role": "user",
        "password": "pass12345678",
    }
    # First signup
    await client.post("/profile/signup", json=payload)
    # Second attempt
    response = await client.post("/profile/signup", json=payload)

    assert response.status_code == 409
    assert response.json()["detail"] == "User already exists"


@pytest.mark.asyncio
async def test_signup_invalid_data(client: AsyncClient):
    """Invalid payload returns 422 Unprocessable Entity"""
    payload = {"email": "not-an-email", "password": "short"}
    response = await client.post("/profile/signup", json=payload)

    assert response.status_code == 422
    assert "detail" in response.json()


@pytest.mark.asyncio
async def test_login_success(client: AsyncClient):
    """Login returns 200 with access_token"""
    # First create user
    payload = {
        "email": "loginuser@example.com",
        "password": "validpass123",
        "role": "user",
    }
    await client.post("/profile/signup", json=payload)

    # Then login
    login_payload = {"email": payload["email"], "password": payload["password"]}
    response = await client.post("/profile/login", json=login_payload)

    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert isinstance(data["access_token"], str)
    assert len(data["access_token"]) > 20


@pytest.mark.asyncio
async def test_delete_user_as_admin(client: AsyncClient, auth_headers_admin):
    """Admin can delete another user"""
    # Create normal user to delete
    user_payload = {
        "email": "todelete@example.com",
        "password": "pass12345678",
        "role": "user",
    }
    signup_resp = await client.post("/profile/signup", json=user_payload)
    user_id = signup_resp.json()["id"]

    # Delete as admin (using auth_headers from fixture)
    delete_response = await client.delete(
        f"/profile/{user_id}", headers=auth_headers_admin
    )

    assert delete_response.status_code == 200

    # Verify user is gone (optional but strong test)
    get_response = await client.get(f"/profile/{user_id}", headers=auth_headers_admin)
    assert get_response.status_code == 404


@pytest.mark.asyncio
async def test_delete_user_as_non_admin_fails(client: AsyncClient, auth_headers_user):
    """Normal user cannot delete another user (403 or 401 expected)"""
    # Create user to delete
    payload = {
        "email": "protected@example.com",
        "role": "user",
        "password": "pass12345678",
    }
    signup_resp = await client.post("/profile/signup", json=payload)
    user_id = signup_resp.json()["id"]

    response = await client.delete(f"/profile/{user_id}", headers=auth_headers_user)
    assert response.status_code in (403, 401)

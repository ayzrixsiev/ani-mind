import pytest


@pytest.mark.asyncio
async def test_signup_success(client):

    # Arrange the data
    payload = {"email": "test@gmail.com", "password": "password123", "role": "admin"}
    # Make an action
    responnse = await client.post("/profile/signup", json=payload)
    # Assert response
    assert responnse.status_code == 201  # Check whether we got success response 201
    data = responnse.json()  # Convert the resonpse into json/dict
    assert data["email"] == "test@gmail.com"  # Validate the email that we send
    assert "id" in data  # Check if db genegrated an id
    assert "password" not in data  # Response should not return password


@pytest.mark.asyncio
async def test_signup_duplicate(client):

    # First sign up so that after we can sign up again
    payload = {"email": "test@gmail.com", "password": "password123"}
    await client.post("/profile/signup", json=payload)

    # Second sign up to test duplicate
    response = await client.post("/profile/signup", json=payload)
    assert response.status_code == 409
    assert response.json()["detail"] == "User already exists"


@pytest.mark.asyncio
async def test_login_success(client):

    payload = {"email": "test@gmail.com", "password": "password123"}
    await client.post("/profile/signup", json=payload)
    response = await client.post("/profile/login", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data


@pytest.mark.asyncio
async def test_delete_user(client):

    # Normal user to delete
    poor_user_to_delete = {"email": "pooruser@gmail.com", "password": "password"}
    response_signup = await client.post("/profile/signup", json=poor_user_to_delete)
    user_id = response_signup.json()["id"]

    # Admin user that will delete
    user_admin = {"email": "test@gmail.com", "password": "password", "role": "admin"}
    await client.post("/profile/signup", json=user_admin)

    user_login = {"email": "test@gmail.com", "password": "password"}
    response_login = await client.post("/profile/login", json=user_login)
    user_token = response_login.json()["access_token"]

    response_delete = await client.delete(
        f"/profile/{user_id}", headers={"Authorization": f"Bearer {user_token}"}
    )
    assert response_delete.status_code == 200

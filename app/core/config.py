from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DATABASE_URL: str
    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int

    # This tells Pydantic to read from the .env file
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


# Create a single instance of the settings to use everywhere
settings = Settings()

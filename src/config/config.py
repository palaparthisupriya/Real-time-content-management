import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    # Server Configuration
    HOST: str = "0.0.0.0"
    PORT: int = 8080

    # Redis Configuration
    REDIS_URL: str = "redis://localhost:6379/0"

    # L1 LRU Cache Configuration
    L1_CAPACITY: int = 100

    # L2 Redis Cache Configuration
    L2_TTL_SECONDS: int = 30

    # Distributed Lock Configuration
    LOCK_TIMEOUT_MS: int = 5000
    LOCK_RETRY_INTERVAL_MS: int = 100
    LOCK_MAX_RETRIES: int = 50

    # Oracle Simulation Configuration
    ORACLE_DELAY_SECONDS: float = 2.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


# Global settings singleton
settings = Settings()

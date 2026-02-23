from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE = Path(__file__).resolve().parents[2] / ".env"


class Settings(BaseSettings):
    app_name: str = "Finance Upload Unified"
    app_env: str = "local"
    api_prefix: str = "/api"
    sqlserver_connection_string: str = Field(
        default=(
            "mssql+pyodbc://sa:YourStrong!Passw0rd@localhost:1433/ExcelImportDB"
            "?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        )
    )
    max_upload_size_mb: int = 20
    allowed_extensions: list[str] = [".xlsx", ".xls"]

    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding="utf-8",
        extra="ignore",  # Ignore env vars not defined on Settings (e.g. stray or malformed .env lines)
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()

from typing import Any, Dict, Optional, Union

from pydantic.v1 import BaseSettings, PostgresDsn, validator, Field


class PostgresConfig(BaseSettings):
    DB_HOST: str = Field(..., env="DB_HOST")
    DB_PORT: str = Field(..., env="DB_PORT")
    DB_USER: str = Field(..., env="DB_USER")
    DB_DB: str = Field(..., env="DB_DB")
    DB_PASS: str = Field(..., env="DB_PASS")

    DB_DSN: Union[PostgresDsn, str] = ""

    @validator("DB_DSN", pre=True, always=True)
    def assemble_api_db_connection(
        cls, v: Optional[str], values: Dict[str, Any]
    ) -> str:
        if v:
            return v
        return PostgresDsn.build(
            scheme="postgresql",
            user=values.get("DB_USER"),
            password=values.get("DB_PASS"),
            host=values.get("DB_HOST"),
            port=values.get("DB_PORT"),
            path=f"/{values.get('DB_DB') or ''}",
        )

    class Config:
        env_file = ".env"


def get_postgres_config() -> PostgresConfig:
    return PostgresConfig()  # type: ignore[call-arg]

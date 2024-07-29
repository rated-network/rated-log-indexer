from typing import Optional

from pydantic.v1 import Field
from pydantic.v1 import BaseSettings


class IndexerConfig(BaseSettings):
    CLOUDWATCH_REGION: str = Field(..., env="CLOUDWATCH_REGION")
    CLOUDWATCH_AWS_ACCESS_KEY_ID: str = Field(..., env="CLOUDWATCH_AWS_ACCESS_KEY_ID")
    CLOUDWATCH_AWS_SECRET_ACCESS_KEY: str = Field(
        ..., env="CLOUDWATCH_AWS_SECRET_ACCESS_KEY"
    )
    CLOUDWATCH_LOG_GROUP: str = Field(..., env="CLOUDWATCH_LOG_GROUP")
    CLOUDWATCH_START_TIME: int = Field(..., env="CLOUDWATCH_START_TIME")
    CLOUDWATCH_FILTER_PATTERN: Optional[str] = Field(
        None, env="CLOUDWATCH_FILTER_PATTERN"
    )

    INTEGRATION_ID: str = Field(..., env="INTEGRATION_ID")
    INTEGRATION_SECRET: str = Field(..., env="INTEGRATION_SECRET")

    class Config:
        env_file = ".env"


def get_indexer_config() -> IndexerConfig:
    return IndexerConfig()  # type: ignore[call-arg]

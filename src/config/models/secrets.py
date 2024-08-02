from enum import Enum
from typing import Optional

from pydantic import BaseModel, StrictBool, model_validator

from src.config.secrets.aws_secrets_manager import AwsSecretsManagerConfig


class SecretProvider(str, Enum):
    AWS = "aws"


class SecretsYamlConfig(BaseModel):
    use_secrets_manager: StrictBool
    provider: Optional[SecretProvider] = None

    aws: Optional[AwsSecretsManagerConfig] = None

    @model_validator(mode="before")
    def validate_secrets_manager(cls, values):
        use_secrets_manager = values.get("use_secrets_manager", False)
        if use_secrets_manager:
            provider = values.get("provider")
            if not provider:
                raise ValueError(
                    "Provider is required when use_secrets_manager is True"
                )

            provider_config = values.get(provider.lower() if provider else None)
            if not provider_config:
                raise ValueError(
                    f'Missing secrets provider configuration for "{provider}"'
                )
        return values

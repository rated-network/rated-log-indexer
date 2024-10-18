from enum import Enum
from typing import Optional

from pydantic import BaseModel, StrictStr, model_validator


class AuthMethod(str, Enum):
    SERVICE_ACCOUNT = "service_account"
    DEFAULT = "application_default_credentials"


class LogFeatures(BaseModel):
    id: StrictStr
    timestamp: StrictStr


class StorageConfig(BaseModel):
    bucket_name: StrictStr
    log_features: Optional[LogFeatures] = None
    prefix: Optional[StrictStr] = None


class GoogleConfig(BaseModel):
    project_id: StrictStr
    auth_method: AuthMethod
    credentials_path: Optional[StrictStr] = None
    storage_config: Optional[StorageConfig] = None

    @model_validator(mode="before")
    def validate_credentials_path(cls, values):
        method = values.get("auth_method")
        if method == AuthMethod.SERVICE_ACCOUNT and not values.get("credentials_path"):
            raise ValueError(
                "Credentials path is required for service account authentication"
            )
        return values

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
    auth_method: StrictStr
    credentials_path: Optional[StrictStr] = None
    storage_config: Optional[StorageConfig] = None

    @model_validator(mode="before")
    def validate_auth_method(cls, values):
        method = values.get("auth_method")
        if method and method.upper() not in AuthMethod.__members__:
            raise ValueError(
                f'Invalid auth method found "{method}": please use one of {AuthMethod.__members__.keys()}'
                # noqa
            )
        values["auth_method"] = AuthMethod[method].value
        return values

    @model_validator(mode="before")
    def validate_credentials_path(cls, values):
        method = values.get("auth_method")
        if method == AuthMethod.SERVICE_ACCOUNT and not values.get("credentials_path"):
            raise ValueError(
                "Credentials path is required for service account authentication"
            )
        return values

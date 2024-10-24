from enum import Enum
from typing import Optional

from pydantic import BaseModel, StrictStr, model_validator


class StorageInputs(str, Enum):
    LOGS = "logs"
    METRICS = "metrics"


class AuthMethod(str, Enum):
    SERVICE_ACCOUNT = "service_account"
    DEFAULT = "application_default_credentials"


class LogFeatures(BaseModel):
    id: StrictStr
    timestamp: StrictStr


class GoogleLogsConfig(BaseModel):
    log_features: LogFeatures


class GoogleMetricsConfig(BaseModel):
    # Placeholder for actual implementation.
    metric_name: StrictStr
    metric_features: StrictStr


class GoogleInputs(str, Enum):
    OBJECTS = "objects"
    LOGS = "logs"
    METRICS = "metrics"


class StorageConfig(BaseModel):
    bucket_name: StrictStr
    input_type: StorageInputs
    logs_config: Optional[GoogleLogsConfig] = None
    metrics_config: Optional[GoogleMetricsConfig] = None
    prefix: Optional[StrictStr] = None


class GoogleConfig(BaseModel):
    project_id: StrictStr
    auth_method: AuthMethod
    config_type: GoogleInputs
    credentials_path: Optional[StrictStr] = None
    storage_config: Optional[StorageConfig] = None
    logs_config: Optional[GoogleLogsConfig] = None
    metrics_config: Optional[GoogleMetricsConfig] = None

    @model_validator(mode="before")
    def validate_credentials_path(cls, values):
        method = values.get("auth_method")
        if method == AuthMethod.SERVICE_ACCOUNT and not values.get("credentials_path"):
            raise ValueError(
                "Credentials path is required for service account authentication"
            )
        return values

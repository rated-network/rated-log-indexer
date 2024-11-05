from typing import Optional, List, ClassVar

import structlog
from pydantic import (
    BaseModel,
    StrictStr,
    PositiveInt,
    model_validator,
    HttpUrl,
    PositiveFloat,
)

from rated_exporter_sdk.providers.prometheus.types import Step, TimeUnit  # type: ignore

logger = structlog.getLogger(__name__)


class PrometheusQueryConfig(BaseModel):
    query: StrictStr
    step: Optional[Step] = None
    slaos_metric_name: StrictStr
    organization_identifier: Optional[StrictStr] = None
    fallback_org_id: Optional[StrictStr] = None

    _warning_logged: ClassVar[bool] = False

    @model_validator(mode="before")
    def convert_empty_to_none(cls, values):
        """Convert empty or whitespace-only strings to None for optional string fields"""
        for field in ["organization_identifier", "fallback_org_id"]:
            if field in values and isinstance(values[field], str):
                stripped_value = values[field].strip()
                if not stripped_value:
                    values[field] = None
                else:
                    values[field] = stripped_value
        return values

    @model_validator(mode="after")
    def validate_step(self) -> "PrometheusQueryConfig":
        """
        Ensure that the step:
        1. Is less than or equal to 60 seconds
        2. Divides 60 seconds evenly when converted to seconds
        """
        if self.step is not None:
            # Convert to milliseconds first for accurate integer conversion
            step_ms = (
                self.step.value
                if self.step.unit == TimeUnit.MILLISECONDS
                else (
                    self.step.value * 1000
                    if self.step.unit == TimeUnit.SECONDS
                    else self.step.value * 60000
                )
            )

            step_in_seconds = step_ms // 1000

            if step_in_seconds > 60 or step_in_seconds < 1:
                raise ValueError("Step must be between 1 and 60 seconds")

            if not (60 % step_in_seconds == 0):
                raise ValueError("Step must evenly divide 60 seconds")

        return self

    @model_validator(mode="after")
    def validate_org_id_fallback(self) -> "PrometheusQueryConfig":
        if self.organization_identifier is None:
            if self.fallback_org_id is None:
                raise ValueError(
                    "`fallback_org_id` must be provided when `organization_identifier` is not set"
                )

            if not self.__class__._warning_logged:
                logger.warning(
                    "Organization identifier not provided, fallback value will be used",
                    organization_identifier=self.fallback_org_id,
                    fallback_org_id=self.fallback_org_id,
                    query=self.query,
                )
                self.__class__._warning_logged = True

        return self


class PrometheusAuthConfig(BaseModel):
    username: Optional[StrictStr] = None
    password: Optional[StrictStr] = None
    token: Optional[StrictStr] = None
    cert_path: Optional[StrictStr] = None
    key_path: Optional[StrictStr] = None
    verify_ssl: bool = True

    @model_validator(mode="after")
    def validate_auth_config(self):
        # Check basic auth configuration
        if self.username is not None and self.password is None:
            raise ValueError("Password is required when username is provided")
        if self.password is not None and self.username is None:
            raise ValueError("Username is required when password is provided")

        # Check certificate auth configuration
        if self.cert_path is not None and self.key_path is None:
            raise ValueError("Key path is required when certificate path is provided")
        if self.key_path is not None and self.cert_path is None:
            raise ValueError("Certificate path is required when key path is provided")

        # Check mutually exclusive auth methods
        auth_methods = [
            bool(self.username and self.password),  # Basic auth
            bool(self.token),  # Token auth
            bool(self.cert_path and self.key_path),  # Certificate auth
        ]
        if sum(auth_methods) > 1:
            raise ValueError(
                "Only one authentication method can be used at a time: "
                "basic auth (username/password), token, or certificate"
            )

        return self


class PrometheusConfig(BaseModel):
    base_url: HttpUrl
    auth: Optional[PrometheusAuthConfig] = None
    queries: List[PrometheusQueryConfig]

    timeout: Optional[PositiveFloat] = 5.0
    pool_connections: Optional[PositiveInt] = 10
    pool_maxsize: Optional[PositiveInt] = 10
    max_parallel_queries: Optional[PositiveInt] = 5
    retry_backoff_factor: Optional[PositiveFloat] = 0.1
    max_retries: Optional[PositiveInt] = 3

    @model_validator(mode="after")
    def validate_connection_settings(self):
        if self.pool_connections > self.pool_maxsize:
            raise ValueError("pool_connections cannot be greater than pool_maxsize")
        if self.max_parallel_queries > self.pool_maxsize:
            raise ValueError("max_parallel_queries cannot be greater than pool_maxsize")

        return self

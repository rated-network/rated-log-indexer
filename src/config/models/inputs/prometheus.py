from typing import Optional, List
from pydantic import BaseModel, StrictStr, PositiveInt, model_validator, HttpUrl

from rated_exporter_sdk.providers.prometheus.types import Step  # type: ignore


class PrometheusQueryConfig(BaseModel):
    query: StrictStr
    step: Optional[Step] = None
    slaos_metric_name: StrictStr  # Name to use for the metric when sending to slaOS
    organization_identifier: StrictStr


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

    timeout: Optional[float] = 15.0
    pool_connections: Optional[PositiveInt] = 10
    pool_maxsize: Optional[PositiveInt] = 10
    max_parallel_queries: Optional[PositiveInt] = 5
    retry_backoff_factor: Optional[float] = 0.1
    max_retries: Optional[PositiveInt] = 3

    @model_validator(mode="after")
    def validate_connection_settings(self):
        if self.pool_connections > self.pool_maxsize:
            raise ValueError("pool_connections cannot be greater than pool_maxsize")
        if self.max_parallel_queries > self.pool_maxsize:
            raise ValueError("max_parallel_queries cannot be greater than pool_maxsize")
        return self

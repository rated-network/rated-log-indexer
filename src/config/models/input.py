import enum

from pydantic import BaseModel, StrictStr, model_validator
from typing import Optional, List


class CloudwatchLogsConfig(BaseModel):
    log_group_name: StrictStr
    log_stream_name: Optional[StrictStr] = None
    filter_pattern: Optional[StrictStr] = None


class DatadogLogsConfig(BaseModel):
    indexes: List[StrictStr] = ["*"]
    query: StrictStr = "*"


class CloudwatchConfig(BaseModel):
    region: StrictStr
    aws_access_key_id: StrictStr
    aws_secret_access_key: StrictStr
    logs_config: Optional[CloudwatchLogsConfig] = None


class DatadogConfig(BaseModel):
    site: StrictStr
    api_key: StrictStr
    app_key: StrictStr
    logs_config: Optional[DatadogLogsConfig] = None


class IntegrationTypes(str, enum.Enum):
    CLOUDWATCH = "cloudwatch"
    DATADOG = "datadog"


class InputTypes(str, enum.Enum):
    LOGS = "logs"
    METRICS = "metrics"


class InputYamlConfig(BaseModel):
    integration: IntegrationTypes
    type: InputTypes
    cloudwatch: Optional[CloudwatchConfig] = None
    datadog: Optional[DatadogConfig] = None

    @model_validator(mode="before")
    def validate_input_config(cls, values):
        integration_type = values.get("integration")
        if integration_type:
            config_attr = integration_type
            if not values.get(config_attr):
                raise ValueError(
                    f'Configuration for input source "{integration_type}" is not found. Please add input configuration for {integration_type}.'
                    # noqa
                )
            for key in IntegrationTypes:
                if key != config_attr:
                    values[key.value] = None
        return values

    @model_validator(mode="before")
    def validate_integration_source(cls, values):
        integration_type = values.get("integration")
        if (
            integration_type
            and integration_type.upper() not in IntegrationTypes.__members__
        ):
            raise ValueError(
                f'Invalid input source found "{integration_type}": please use one of {IntegrationTypes.__members__.keys()}'  # noqa
            )
        return values

    @model_validator(mode="before")
    def validate_input_type(cls, values):
        input_type = values.get("type")
        if input_type and input_type.upper() not in InputTypes.__members__:
            raise ValueError(
                f'Invalid input source found "{input_type}": please use one of {InputTypes.__members__.keys()}'
                # noqa
            )
        return values

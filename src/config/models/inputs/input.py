from enum import Enum

from pydantic import BaseModel, model_validator, Field, StrictStr
from typing import Optional

from src.config.models.offset import OffsetYamlConfig
from src.config.models.filters import FiltersYamlConfig
from src.config.models.inputs.cloudwatch import CloudwatchConfig
from src.config.models.inputs.datadog import DatadogConfig


class IntegrationTypes(str, Enum):
    CLOUDWATCH = "cloudwatch"
    DATADOG = "datadog"


class InputTypes(str, Enum):
    LOGS = "logs"
    METRICS = "metrics"


class InputYamlConfig(BaseModel):
    integration_prefix: StrictStr = Field(
        default="", description="Prefix for the integration."
    )
    integration: IntegrationTypes
    type: InputTypes
    filters: Optional[FiltersYamlConfig] = None
    offset: OffsetYamlConfig

    cloudwatch: Optional[CloudwatchConfig] = None
    datadog: Optional[DatadogConfig] = None

    @model_validator(mode="before")
    def validate_filters_requirement(cls, values):
        input_type = values.get("type")
        filters = values.get("filters")

        if input_type == InputTypes.LOGS and filters is None:
            raise ValueError("'filters' is mandatory when input type is LOGS")

        return values

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

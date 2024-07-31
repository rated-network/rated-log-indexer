import enum

from pydantic import BaseModel, StrictStr, model_validator
from typing import Optional


class CloudwatchConfig(BaseModel):
    region: StrictStr
    log_group_name: StrictStr
    log_stream_name: Optional[StrictStr] = None


class DatadogConfig(BaseModel):
    api_key: StrictStr
    app_key: StrictStr
    query: StrictStr


class InputTypes(str, enum.Enum):
    CLOUDWATCH = "cloudwatch"
    DATADOG = "datadog"


class InputYamlConfig(BaseModel):
    type: InputTypes
    cloudwatch: Optional[CloudwatchConfig] = None
    datadog: Optional[DatadogConfig] = None

    @model_validator(mode="before")
    def validate_input_config(cls, values):
        input_type = values.get("type")
        if input_type:
            config_attr = input_type
            if not values.get(config_attr):
                raise ValueError(
                    f'Configuration for input source "{input_type}" is not found. Please add input configuration for {input_type}.'
                    # noqa
                )
            for key in InputTypes:
                if key != config_attr:
                    values[key.value] = None
        return values

    @model_validator(mode="before")
    def validate_input_source(cls, values):
        input_type = values.get("type")
        if input_type and input_type.upper() not in InputTypes.__members__:
            raise ValueError(
                f'Invalid input source found "{input_type}": please use one of {InputTypes.__members__.keys()}'  # noqa
            )
        return values

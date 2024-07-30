from pydantic import BaseModel, root_validator, StrictStr
from typing import Optional
from typing_extensions import Literal


class CloudwatchConfig(BaseModel):
    region: StrictStr
    log_group_name: StrictStr
    log_stream_name: Optional[StrictStr] = None


class DatadogConfig(BaseModel):
    api_key: StrictStr
    app_key: StrictStr
    query: StrictStr


class InputYamlConfig(BaseModel):
    type: Literal["cloudwatch", "datadog"]
    cloudwatch: Optional[CloudwatchConfig] = None
    datadog: Optional[DatadogConfig] = None

    @root_validator(pre=True)
    def validate_input_config(cls, values):
        input_type = values.get("type")
        if input_type == "cloudwatch":
            if "cloudwatch" not in values or not values["cloudwatch"]:
                raise ValueError(
                    'cloudwatch configuration is required when type is "cloudwatch"'
                )
            values["datadog"] = None
        elif input_type == "datadog":
            if "datadog" not in values or not values["datadog"]:
                raise ValueError(
                    'datadog configuration is required when type is "datadog"'
                )
            values["cloudwatch"] = None
        return values

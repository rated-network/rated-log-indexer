import enum
from typing import Optional

from pydantic import BaseModel, StrictStr, model_validator


class RatedOutputConfig(BaseModel):
    slaos_api_key: StrictStr
    ingestion_id: StrictStr
    ingestion_key: StrictStr
    ingestion_url: StrictStr


class ConsoleOutputConfig(BaseModel):
    placeholder: StrictStr


class OutputTypes(str, enum.Enum):
    CONSOLE = "console"
    RATED = "rated"


class OutputYamlConfig(BaseModel):
    type: OutputTypes

    rated: Optional[RatedOutputConfig] = None
    console: Optional[ConsoleOutputConfig] = None

    @model_validator(mode="before")
    def validate_output_config(cls, values):
        output_type = values.get("type")
        if output_type:
            config_attr = output_type
            if not values.get(config_attr):
                raise ValueError(
                    f'Configuration for output source "{output_type}" is not found. Please add output configuration for {output_type}.'
                    # noqa
                )
            for key in OutputTypes:
                if key != config_attr:
                    values[key.value] = None
        return values

    @model_validator(mode="before")
    def validate_output_source(cls, values):
        output_type = values.get("type")
        if output_type and output_type.upper() not in OutputTypes.__members__:
            raise ValueError(
                f'Invalid output source found "{output_type}": please use one of {OutputTypes.__members__.keys()}'  # noqa
            )
        return values

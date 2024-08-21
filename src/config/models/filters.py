from rated_parser.payloads.inputs import (  # type: ignore
    JsonFieldDefinition,
    RawTextFieldDefinition,
    LogFormat as RatedParserLogFormat,
)
from pydantic import BaseModel, StrictStr, StrictInt, field_validator
from typing import List, Union, Dict


class FiltersYamlConfig(BaseModel):
    version: StrictInt
    log_format: RatedParserLogFormat
    log_example: Union[StrictStr, Dict]
    fields: Union[List[JsonFieldDefinition], List[RawTextFieldDefinition]]

    @field_validator("log_format", mode="before")
    def set_log_format(cls, v):
        if isinstance(v, str):
            return RatedParserLogFormat(v.lower())
        return v

    @field_validator("log_example")
    def validate_log_example(cls, v, info):
        log_format = info.data.get("log_format")

        if log_format == RatedParserLogFormat.RAW_TEXT and not isinstance(v, str):
            raise ValueError("log_example must be a string when log_format is RAW_TEXT")
        elif log_format == RatedParserLogFormat.JSON and not isinstance(v, dict):
            raise ValueError("log_example must be a dictionary when log_format is JSON")
        return v

    @field_validator("fields")
    def validate_field_types(cls, v, info):
        log_format = info.data.get("log_format")

        if not v:
            raise ValueError("Filter fields cannot be empty")

        if "customer_id" not in [field.key for field in v]:
            raise ValueError("customer_id field is required in filters")

        if log_format == RatedParserLogFormat.RAW_TEXT:
            for field in v:
                if not isinstance(field, RawTextFieldDefinition):
                    raise ValueError(
                        "fields must be a list of RawTextFieldDefinition when log_format is 'raw_text'"
                    )
        elif log_format == RatedParserLogFormat.JSON:
            for field in v:
                if not isinstance(field, JsonFieldDefinition):
                    raise ValueError(
                        "fields must be a list of JsonFieldDefinition when log_format is 'json_dict'"
                    )
        return v
